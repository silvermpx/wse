// The outbox in a file (wse-client/node): the format every client shares,
// across restarts, and as the process ends.

import { spawnSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { transformWithOxc } from 'vite';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createWSEClient } from '../core/client';
import { addBody, encodeLine, encodeOutbox, fileOutbox, readOutbox, saltSeed } from '../node/fileOutbox';
import { noOutcomes, numbered, Outbox, unnumbered } from '../services/Outbox';

interface VectorLine {
  kind: string;
  json: string;
  line: string;
}
interface VectorRecord {
  s: number;
  id: string;
  at: number;
  until: number;
  owned: boolean;
  requires?: string[];
  text: string;
}
interface Vector {
  salt: string;
  lines: VectorLine[];
  state: { producer: string; node: string; written: number; next: number; user: string; records: VectorRecord[] };
  damaged: Array<{ why: string; file: string; records: string[] | null; user?: string | null }>;
}

const vector: Vector = JSON.parse(
  readFileSync(new URL('../../crates/wse-client/tests/vectors/outbox.json', import.meta.url), 'utf8'),
);

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

/** The numbers handed to the connection now. */
function due(outbox: Outbox): number[] {
  const out: number[] = [];
  outbox.due(0, noOutcomes(), (_message, s) => {
    out.push(s!);
    return true;
  });
  return out;
}

/** The numbers handed to the connection, once the file holds that they may have gone. */
async function dueWritten(outbox: Outbox): Promise<number[]> {
  const now = due(outbox);
  if (now.length > 0) return now;
  await new Promise<void>((resolve) => (outbox.writable = resolve));
  outbox.writable = null;
  return due(outbox);
}

describe('the file of every client (tests/vectors/outbox.json)', () => {
  it('writes each line byte for byte as the vector has it', () => {
    const seed = saltSeed(vector.salt);
    const { state } = vector;
    const head = encodeOutbox(
      { producer: state.producer, node: null, written: 0, next: state.next, user: null, records: [] },
      vector.salt,
    );
    expect(head.data).toBe(`${vector.lines[0].line}\n`);
    for (const line of vector.lines.slice(1)) {
      let written: string;
      if (line.kind === 'add') {
        const { add } = JSON.parse(line.json);
        const text = line.json.slice(line.json.indexOf(',"text":') + 8, -2);
        written = encodeLine(seed, addBody({ ...add, json: unnumbered(text) }));
      } else {
        written = encodeLine(seed, JSON.stringify(JSON.parse(line.json)));
      }
      expect(written).toBe(`${line.line}\n`);
    }
  });

  it('writes a file anew in the order the vector has', () => {
    const { state } = vector;
    const records = state.records.map((r) => ({ ...r, json: unnumbered(r.text) }));
    const { data, lines } = encodeOutbox({ ...state, records }, vector.salt);
    const byKind = (kind: string) => vector.lines.filter((l) => l.kind === kind);
    const expected = [
      vector.lines[0],
      ...byKind('node'),
      ...byKind('written'),
      byKind('user')[0],
      ...vector.lines.filter((l) => l.kind === 'add' && /"id":"[cd]"/.test(l.json)),
    ];
    expect(data).toBe(expected.map((l) => `${l.line}\n`).join(''));
    expect(lines).toBe(6);
  });

  it('reads the file to the state the vector has', () => {
    const read = readOutbox(vector.lines.map((l) => `${l.line}\n`).join(''))!;
    const { state } = vector;
    expect([read.producer, read.node, read.written, read.next, read.user]).toEqual([
      state.producer,
      state.node,
      state.written,
      state.next,
      state.user,
    ]);
    expect(
      read.records.map((r) => ({
        s: r.s,
        id: r.id,
        at: r.at,
        until: r.until,
        owned: r.owned,
        ...(r.requires ? { requires: r.requires } : {}),
        text: numbered(r.s, r.json),
      })),
    ).toEqual(state.records);
    expect(read.records[0].message).toEqual({ t: 'm', p: {}, id: 'c' });
  });

  it.each(vector.damaged)('$why', (damaged) => {
    const read = readOutbox(damaged.file);
    if (damaged.records === null) {
      expect(read).toBeNull();
      return;
    }
    expect(read!.records.map((r) => r.id)).toEqual(damaged.records);
    expect(read!.user).toBe(damaged.user ?? null);
  });
});

describe('the outbox in a file', () => {
  let dir: string;
  let path: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'wse-outbox-'));
    path = join(dir, 'queue');
  });

  afterEach(() => {
    chmodSync(dir, 0o700);
    rmSync(dir, { recursive: true, force: true });
  });

  it('says a mark is written once the file holds it, when it was handed behind a rewrite', async () => {
    const state = { producer: 'p', written: 0, next: 1, user: null, node: null, records: [] };
    const { log } = fileOutbox(path).open(
      () => state,
      (_, error) => {
        throw error;
      },
    );
    if (!log) throw new Error('no log');
    await new Promise<void>((resolve) => log.flush(resolve));
    // An append runs, a rewrite waits behind it, and the mark is handed after both.
    log.put({ node: 'one' });
    log.flush();
    log.rewrite(state);
    log.put({ written: 1 });
    const seen = await new Promise<number>((resolve) =>
      log.flush(() => resolve(readOutbox(readFileSync(path, 'utf8')).written)),
    );
    expect(seen).toBe(1);
    log.close();
  });

  it('keeps what waits for the next client, until the server took it', async () => {
    const first = new Outbox(limits, { store: fileOutbox(path) });
    expect(first.durable).toBe(true);
    answering(first);
    for (const id of ['a', 'b', 'c']) add(first, id);
    expect(await dueWritten(first)).toEqual([1, 2, 3]);
    first.acked(1);
    first.close();

    const again = new Outbox(limits, { store: fileOutbox(path) });
    expect(again.producer).toBe(first.producer);
    expect(again.size).toBe(2);
    expect(again.hello()).toEqual({ producer: first.producer, node: 'n1', written: 3, from: 2 });
    answering(again, 1);
    expect(due(again)).toEqual([2, 3]);
    expect(add(again, 'd')).toBe(4);
    again.close();
  });

  it('never uses a number twice, a cleared one included', () => {
    const first = new Outbox(limits, { store: fileOutbox(path) });
    add(first, 'a');
    add(first, 'b');
    first.clear();
    first.close();
    const again = new Outbox(limits, { store: fileOutbox(path) });
    expect(again.size).toBe(0);
    expect(add(again, 'c')).toBe(3);
    again.close();
  });

  it('reads no tail that does not check', () => {
    const first = new Outbox(limits, { store: fileOutbox(path) });
    add(first, 'a');
    add(first, 'b');
    first.close();
    const data = readFileSync(path);
    writeFileSync(path, Buffer.concat([data.subarray(0, data.length - 5), Buffer.from([0xff, 0xfe, 0x00, 0x0a])]));
    const again = new Outbox(limits, { store: fileOutbox(path) });
    expect(again.size).toBe(1);
    again.close();
  });

  it('says a file another client has, and keeps what waits in memory', () => {
    const first = new Outbox(limits, { store: fileOutbox(path) });
    const said: Array<[string, string]> = [];
    const second = new Outbox(limits, {
      store: fileOutbox(path),
      onStorageError: (operation, error) => said.push([operation, String(error)]),
    });
    expect(said).toHaveLength(1);
    expect(said[0][0]).toBe('open');
    expect(said[0][1]).toContain("another client's");
    expect(second.durable).toBe(false);
    add(second, 'a');
    expect(second.size).toBe(1);
    first.close();
    // Let go: the next one has it.
    const third = new Outbox(limits, { store: fileOutbox(path) });
    expect(third.durable).toBe(true);
    third.close();
  });

  it('writes the file anew once what went outweighs what is kept', async () => {
    const outbox = new Outbox(limits, { store: fileOutbox(path) });
    answering(outbox);
    for (let i = 0; i < 300; i++) {
      const s = add(outbox, `m${i}`);
      due(outbox);
      outbox.acked(s);
      if (i % 50 === 0) await new Promise((resolve) => setTimeout(resolve, 5));
    }
    outbox.close();
    expect(readFileSync(path, 'utf8').split('\n').length).toBeLessThan(80);
    const again = new Outbox(limits, { store: fileOutbox(path) });
    expect(again.size).toBe(0);
    expect(add(again, 'next')).toBe(301);
    again.close();
  });

  it.skipIf(process.platform === 'win32' || process.getuid?.() === 0)(
    'says a file that cannot be written, and writes it whole once it can',
    async () => {
      const said: string[] = [];
      const outbox = new Outbox(limits, {
        store: fileOutbox(path),
        onStorageError: (operation) => said.push(operation),
      });
      answering(outbox);
      // Written anew at open; then no file can be made beside it.
      await new Promise((resolve) => setTimeout(resolve, 50));
      chmodSync(dir, 0o500);
      for (let i = 0; i < 100; i++) {
        const s = add(outbox, `m${i}`);
        due(outbox);
        outbox.acked(s);
      }
      add(outbox, 'kept');
      due(outbox);
      await vi.waitFor(() => expect(said).toEqual(['write']), { timeout: 5_000 });
      chmodSync(dir, 0o700);
      outbox.disconnected();
      add(outbox, 'after');
      outbox.close();
      const again = new Outbox(limits, { store: fileOutbox(path) });
      expect(again.size).toBe(2);
      again.close();
    },
  );

  // The process ends with process.exit(): what it handed the file is in it.
  describe('a process that ends', () => {
    let built: string;

    beforeEach(async () => {
      built = join(dir, 'built');
      // The modules as JavaScript, for a Node of its own.
      const sources = new URL('../', import.meta.url);
      for (const file of ['services/Outbox.ts', 'node/fileOutbox.ts', 'utils/id.ts', 'utils/webcrypto.ts']) {
        const { code } = await transformWithOxc(readFileSync(new URL(file, sources), 'utf8'), file, { lang: 'ts' });
        const out = join(built, file.replace(/\.ts$/, '.js'));
        mkdirSync(join(out, '..'), { recursive: true });
        writeFileSync(out, code);
      }
      writeFileSync(join(built, 'package.json'), '{"type":"module"}');
      writeFileSync(
        join(built, 'child.js'),
        `import { Outbox } from './services/Outbox.js';
import { fileOutbox } from './node/fileOutbox.js';
const [path, mode] = process.argv.slice(2);
const outbox = new Outbox({ maxMessages: 1000, maxBytes: 1 << 24, maxAge: 3600000 }, { store: fileOutbox(path) });
const add = (id) => { const m = { t: 'm', p: { id }, id }; outbox.add(m, JSON.stringify(m), undefined, Date.now()); };
for (let i = 0; i < 10; i++) add('a' + i);
await new Promise((resolve) => setTimeout(resolve, 100));
for (let i = 0; i < 40; i++) add('b' + i);
// 'running': a write runs as it ends; 'handed': what it was handed waits.
if (mode === 'running') await Promise.resolve();
process.exit(0);
`,
      );
    });

    it.each(['handed', 'running'])('writes what it was handed (%s)', (mode) => {
      const child = spawnSync(process.execPath, [join(built, 'child.js'), path, mode], { encoding: 'utf8' });
      expect(child.stderr).toBe('');
      expect(child.status).toBe(0);
      const read = readOutbox(readFileSync(path, 'utf8'))!;
      expect(read.records).toHaveLength(50);
      expect(read.records.at(-1)!.id).toBe('b39');
      // Let go when it ended.
      const again = new Outbox(limits, { store: fileOutbox(path) });
      expect(again.durable).toBe(true);
      again.close();
    });
  });

  describe('a client', () => {
    it('keeps what it sent with no connection for the next one, in the file', async () => {
      const first = createWSEClient({ endpoints: ['ws://outbox.test/wse'], offline: { store: fileOutbox(path) } });
      expect(first.state.getSnapshot().storage).toBe('durable');
      expect(first.send('order', { n: 1 }, { id: 'kept' })).toBe('queued');
      expect(first.kept()).toBe(1);
      first.destroy();
      expect(readOutbox(readFileSync(path, 'utf8'))!.records.map((r) => r.id)).toEqual(['kept']);

      const second = createWSEClient({ endpoints: ['ws://outbox.test/wse'], offline: { store: fileOutbox(path) } });
      expect(second.kept()).toBe(1);
      const errors: unknown[] = [];
      const other = createWSEClient({ endpoints: ['ws://outbox.test/wse'], offline: { store: fileOutbox(path) } });
      other.on('wseStorageError', (detail) => errors.push(detail));
      await Promise.resolve();
      expect(other.state.getSnapshot().storage).toBe('unavailable');
      expect(errors).toHaveLength(1);
      other.destroy();
      second.signOut();
      expect(readOutbox(readFileSync(path, 'utf8'))!.records).toEqual([]);
    });
  });
});
