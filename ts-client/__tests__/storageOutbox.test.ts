// The outbox in the app's storage (React Native): AsyncStorage, one with
// its API but no multi-calls, and MMKV.

import { describe, expect, it, vi } from 'vitest';
import { noOutcomes, Outbox } from '../services/Outbox';
import { storageOutbox, type AsyncStorageLike, type MMKVLike } from '../react-native/storageOutbox';
import { setWebCrypto } from '../utils/webcrypto';

type Data = { data: Map<string, string> };

function asyncStorage(multi = true): AsyncStorageLike & Data {
  const data = new Map<string, string>();
  const storage: AsyncStorageLike & Data = {
    data,
    getItem: async (key) => data.get(key) ?? null,
    setItem: async (key, value) => {
      data.set(key, value);
    },
    removeItem: async (key) => {
      data.delete(key);
    },
    getAllKeys: async () => [...data.keys()],
  };
  if (multi) {
    storage.multiGet = async (keys) => keys.map((key) => [key, data.get(key) ?? null] as const);
    storage.multiSet = async (pairs) => {
      for (const [key, value] of pairs) data.set(key, value);
    };
    storage.multiRemove = async (keys) => {
      for (const key of keys) data.delete(key);
    };
  }
  return storage;
}

function mmkv(): MMKVLike & Data {
  const data = new Map<string, string>();
  return {
    data,
    getString: (key) => data.get(key),
    set: (key, value) => {
      data.set(key, value);
    },
    delete: (key) => {
      data.delete(key);
    },
    getAllKeys: () => [...data.keys()],
  };
}

const limits = { maxMessages: 100, maxBytes: 1 << 20, maxAge: 3_600_000 };

function add(outbox: Outbox, id: string): number {
  const message = { t: 'm', p: {}, id };
  return outbox.add(message, JSON.stringify(message), undefined, 0);
}

function answering(outbox: Outbox, acked = 0): void {
  outbox.connected('ann', 0);
  outbox.serverAnswers(true);
  outbox.answeredWith(acked, [], 'n1');
}

/** The numbers handed to the connection, once the storage wrote that they may have gone. */
async function due(outbox: Outbox): Promise<number[]> {
  const hand = () => {
    const out: number[] = [];
    outbox.due(0, noOutcomes(), (_message, s) => {
      out.push(s!);
      return true;
    });
    return out;
  };
  const now = hand();
  if (now.length > 0) return now;
  await new Promise<void>((resolve) => (outbox.writable = resolve));
  outbox.writable = null;
  return hand();
}

/** What the storage holds of the messages, by number. */
const messages = (storage: Data) =>
  [...storage.data.keys()].filter((key) => key.startsWith('wse-outbox:wse:')).map((key) => Number(key.slice(15))).sort();

const settled = () => new Promise((resolve) => setTimeout(resolve, 10));

describe('the outbox in the app storage (React Native)', () => {
  it.each([
    ['AsyncStorage', () => asyncStorage()],
    ['a storage with its API but no multi-calls', () => asyncStorage(false)],
    ['MMKV', () => mmkv()],
  ])('in %s keeps what waits for the next start, until the server took it', async (_kind, make) => {
    const storage = make();
    const first = new Outbox(limits, { store: storageOutbox(storage) });
    await first.opened;
    expect(first.durable).toBe(true);
    answering(first);
    for (const id of ['a', 'b', 'c']) add(first, id);
    expect(await due(first)).toEqual([1, 2, 3]);
    first.acked(1);
    first.close();
    await settled();
    expect(messages(storage)).toEqual([2, 3]);

    const again = new Outbox(limits, { store: storageOutbox(storage) });
    await again.opened;
    expect(again.producer).toBe(first.producer);
    expect(again.size).toBe(2);
    expect(again.hello()).toEqual({ producer: first.producer, node: 'n1', written: 3, from: 2 });
    answering(again, 1);
    expect(await due(again)).toEqual([2, 3]);
    expect(add(again, 'd')).toBe(4);
    again.acked(4);
    again.close();
    await settled();
    expect([...storage.data.keys()]).toEqual(['wse-outbox:wse']);
  });

  it('makes its producer with the WebCrypto the client was given, the environment having none', async () => {
    vi.stubGlobal('crypto', undefined);
    const given = {
      getRandomValues: <T extends ArrayBufferView>(array: T) => {
        new Uint8Array(array.buffer, array.byteOffset, array.byteLength).fill(0xab);
        return array;
      },
    } as unknown as Crypto;
    setWebCrypto(given);
    try {
      expect(new Outbox(limits).producer.startsWith('abababababab')).toBe(true);
      const kept = new Outbox(limits, { store: storageOutbox(asyncStorage()) });
      await kept.opened;
      expect(kept.durable).toBe(true);
      expect(kept.producer.startsWith('abababababab')).toBe(true);
      kept.close();
    } finally {
      setWebCrypto(null);
      vi.unstubAllGlobals();
    }
  });

  it('a removal that failed is done again when the storage is written anew', async () => {
    const storage = asyncStorage();
    const first = new Outbox(limits, { store: storageOutbox(storage) });
    await first.opened;
    add(first, 'a'); // before any connection: anyone's
    await settled();
    expect(messages(storage)).toEqual([1]);
    const multiRemove = storage.multiRemove!;
    let fails = true;
    storage.multiRemove = async (keys) => {
      if (fails) {
        fails = false;
        throw new Error('busy');
      }
      return multiRemove(keys);
    };
    // The user signs out; the removal fails, and the store is written anew
    // as the client ends.
    first.clear();
    await settled();
    first.close();
    await settled();
    expect(messages(storage)).toEqual([]);
    const again = new Outbox(limits, { store: storageOutbox(storage) });
    await again.opened;
    expect(again.size).toBe(0);
    again.close();
  });

  it('one that opens waits for the last client’s writes to end', async () => {
    const storage = asyncStorage();
    const first = new Outbox(limits, { store: storageOutbox(storage) });
    await first.opened;
    const multiSet = storage.multiSet!;
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    storage.multiSet = async (pairs) => {
      await held;
      return multiSet(pairs);
    };
    add(first, 'a');
    await settled();
    first.close();
    const second = new Outbox(limits, { store: storageOutbox(storage) });
    await settled();
    storage.multiSet = multiSet;
    release();
    await second.opened;
    expect(second.size).toBe(1);
    expect(add(second, 'b')).toBe(2);
    second.close();
    await settled();
    expect(messages(storage)).toEqual([1, 2]);
  });

  it('is one client’s while it runs', async () => {
    const storage = asyncStorage();
    const first = new Outbox(limits, { store: storageOutbox(storage) });
    await first.opened;
    const said: Array<[string, string]> = [];
    const second = new Outbox(limits, {
      store: storageOutbox(storage),
      onStorageError: (operation, error) => said.push([operation, String(error)]),
    });
    await second.opened;
    expect(second.durable).toBe(false);
    expect(said).toHaveLength(1);
    expect(said[0][1]).toContain("another client's");
    first.close();
    const third = new Outbox(limits, { store: storageOutbox(storage) });
    await third.opened;
    expect(third.durable).toBe(true);
    third.close();
  });

  it('keeps sends made before it opened, numbered after what it held', async () => {
    const storage = asyncStorage();
    const first = new Outbox(limits, { store: storageOutbox(storage) });
    await first.opened;
    add(first, 'a');
    first.close();
    await settled();
    const again = new Outbox(limits, { store: storageOutbox(storage) });
    // Sent at once, before the storage answered.
    add(again, 'b');
    await again.opened;
    expect(again.size).toBe(2);
    answering(again);
    expect(await due(again)).toEqual([1, 2]);
    again.close();
    await settled();
    expect(messages(storage)).toEqual([1, 2]);
  });

  it('lets what a sign-out cleared go before the user is forgotten', async () => {
    const storage = asyncStorage();
    const outbox = new Outbox(limits, { store: storageOutbox(storage) });
    await outbox.opened;
    outbox.connected('ann', 0);
    add(outbox, 'a');
    await settled();
    expect(messages(storage)).toEqual([1]);
    outbox.clear();
    outbox.close();
    await settled();
    expect(messages(storage)).toEqual([]);
    expect(JSON.parse(storage.data.get('wse-outbox:wse')!).user).toBeNull();
  });

  it('says a storage that fails, and writes it whole once it can', async () => {
    const storage = asyncStorage();
    const said: string[] = [];
    const outbox = new Outbox(limits, {
      store: storageOutbox(storage),
      onStorageError: (operation) => said.push(operation),
    });
    await outbox.opened;
    const multiSet = storage.multiSet!;
    storage.multiSet = async () => {
      throw new Error('full');
    };
    add(outbox, 'a');
    await vi.waitFor(() => expect(said).toEqual(['write']));
    storage.multiSet = multiSet;
    // A second later the outbox writes it anew, with both.
    await new Promise((resolve) => setTimeout(resolve, 1_100));
    add(outbox, 'b');
    await vi.waitFor(() => expect(messages(storage)).toEqual([1, 2]));
    outbox.close();
  });

  /** An outbox whose storage failed twice in a row, the second time on
   * the rewrite: a third message comes within the second after it. */
  async function failedTwice() {
    const storage = asyncStorage();
    const said: string[] = [];
    const outbox = new Outbox(limits, {
      store: storageOutbox(storage),
      onStorageError: (operation) => said.push(operation),
    });
    await outbox.opened;
    const multiSet = storage.multiSet!;
    storage.multiSet = async () => {
      throw new Error('full');
    };
    add(outbox, 'a');
    await vi.waitFor(() => expect(said).toEqual(['write']));
    // The rewrite, at once, fails too.
    add(outbox, 'b');
    await settled();
    storage.multiSet = multiSet;
    add(outbox, 'c');
    await settled();
    return { storage, outbox };
  }

  it('writes it whole once the second is over, with no more change to wait for', async () => {
    const { storage, outbox } = await failedTwice();
    await vi.waitFor(() => expect(messages(storage)).toEqual([1, 2, 3]), { timeout: 2_000 });
    outbox.close();
  });

  it('writes it whole when it closes within the second', async () => {
    const { storage, outbox } = await failedTwice();
    outbox.close();
    await settled();
    expect(messages(storage)).toEqual([1, 2, 3]);
  });
});
