// Solid over a client of its own, the same tests on Solid 1.9 and on 2.0
// (two Vitest projects): the helpers take the client's connection, topics
// and listeners after the first render, never while the owner runs, and let
// them go with the owner; a changed topic list or a new client is taken
// before the old one is let go.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as solid from 'solid-js';
import { createRoot, createSignal } from 'solid-js';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, tick } from '../helpers.js';
import { createWSEClient, type WSEClient } from '../../core/index.js';
import type { WSMessage } from '../../types.js';
import { createWSEState, createWSETopics, onWSEEvent, onWSEMessage } from '../../solid/index.js';

let client: WSEClient;
const URL = 'ws://solid.test/wse';

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  client = createWSEClient({ endpoints: [URL], offline: { enabled: false } });
});

afterEach(() => {
  client.destroy();
  resetClientState();
  vi.unstubAllGlobals();
});

const updates = () =>
  FakeWebSocket.last()
    .messagesOfType('subscription_update')
    .map((m) => `${m.p.action}:${[...m.p.topics].sort().join(',')}`);

describe('solid', () => {
  it('runs on the Solid its project names', () => {
    // flush() is new in Solid 2.
    expect(typeof (solid as Record<string, unknown>).flush === 'function').toBe(process.env.SOLID_MAJOR === '2.0');
  });

  it('takes nothing while the owner runs; connects after the first render; lets go with the owner', async () => {
    let state!: () => { connection: string };
    const dispose = createRoot((dispose) => {
      state = createWSEState(client);
      // Nothing while the owner runs: a server render stops here. (Solid 1
      // runs the root's effects as it returns, Solid 2 after the flush.)
      expect(FakeWebSocket.instances).toHaveLength(0);
      expect(state().connection).toBe('pending');
      return dispose;
    });
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);
    await connectServer(FakeWebSocket.last());
    expect(state().connection).toBe('connected');
    dispose();
    await tick();
    expect(FakeWebSocket.last().closeCalls.length).toBeGreaterThan(0);
  });

  it('outside an owner it says so before taking anything', () => {
    expect(() => createWSEState(client)).toThrow(/owner/);
    expect(FakeWebSocket.instances).toHaveLength(0);
  });

  it('a changed topic list keeps the topics it still holds', async () => {
    const [topics, setTopics] = createSignal<string[]>(['a', 'b']);
    const dispose = createRoot((dispose) => {
      createWSETopics(topics, client);
      return dispose;
    });
    await tick();
    await connectServer(FakeWebSocket.last());
    setTopics(['a', 'b', 'c']);
    await tick();
    setTopics(['c', 'a']);
    await tick();
    expect(updates()).toEqual(['subscribe:a,b', 'subscribe:c', 'unsubscribe:b']);
    dispose();
  });

  it("gives a handler the client's own messages in order, and hears events holding nothing", async () => {
    const seen: WSMessage[] = [];
    const heard: unknown[] = [];
    const listening = createRoot((dispose) => {
      onWSEEvent('presenceJoin', (detail) => heard.push(detail), client);
      return dispose;
    });
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(0);
    const dispose = createRoot((dispose) => {
      onWSEMessage('price_update', (message) => seen.push(message), client);
      return dispose;
    });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    ws.serverSend({ c: 'U', t: 'price_update', p: { n: 1 }, v: 1 });
    ws.serverSend({ c: 'U', t: 'price_update', p: { n: 2 }, v: 1 });
    ws.serverSend({ c: 'WSE', t: 'presence_join', p: { user: 'bob' }, v: 1 });
    await tick();
    listening();
    ws.serverSend({ c: 'WSE', t: 'presence_join', p: { user: 'eve' }, v: 1 });
    await tick();
    expect(seen.map((m) => m.p)).toEqual([{ n: 1 }, { n: 2 }]);
    expect(heard).toEqual([{ user: 'bob' }]);
    dispose();
  });

  it('a new client from an accessor is taken before the old one is let go', async () => {
    const other = createWSEClient({ endpoints: ['ws://solid.other/wse'], offline: { enabled: false } });
    const [current, setCurrent] = createSignal<WSEClient>(client);
    let state!: () => { connection: string };
    const dispose = createRoot((dispose) => {
      state = createWSEState(current);
      createWSETopics(['news'], current);
      return dispose;
    });
    await tick();
    await connectServer(FakeWebSocket.last());
    expect(state().connection).toBe('connected');
    setCurrent(other);
    await tick();
    const sockets = FakeWebSocket.instances;
    expect(sockets).toHaveLength(2);
    expect(sockets[1].url.startsWith('ws://solid.other/wse')).toBe(true);
    // The first client was let go once the second was taken.
    expect(sockets[0].closeCalls.length).toBeGreaterThan(0);
    expect(state().connection).not.toBe('connected');
    dispose();
    other.destroy();
  });

  it('a hundred owners leave no lease and no listener behind', async () => {
    let held = 0;
    const counted = <T extends (...args: any[]) => any>(take: T) =>
      ((...args: Parameters<T>) => {
        const taken = take(...args);
        held++;
        let gone = false;
        const letGo = () => {
          if (!gone) held--;
          gone = true;
        };
        return typeof taken === 'function'
          ? () => (letGo(), taken())
          : { release: () => (letGo(), taken.release()) };
      }) as T;
    const own = {
      retain: client.retain.bind(client),
      subscribe: client.subscribe.bind(client),
      onMessage: client.onMessage.bind(client),
      listen: client.state.listen.bind(client.state),
    };
    vi.spyOn(client, 'retain').mockImplementation(counted(own.retain));
    vi.spyOn(client, 'subscribe').mockImplementation(counted(own.subscribe));
    vi.spyOn(client, 'onMessage').mockImplementation(counted(own.onMessage));
    vi.spyOn(client.state, 'listen').mockImplementation(counted(own.listen));
    let most = 0;
    for (let i = 0; i < 100; i++) {
      const dispose = createRoot((dispose) => {
        createWSEState(client);
        createWSETopics(['news'], client);
        onWSEMessage('news', () => {}, client);
        return dispose;
      });
      await tick(1);
      most = Math.max(most, held);
      dispose();
    }
    // A state listener, a topic lease, a message listener and three
    // connection leases per owner; nothing once it is disposed.
    expect(most).toBe(6);
    expect(held).toBe(0);
  });
});
