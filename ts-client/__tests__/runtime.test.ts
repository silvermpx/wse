// @vitest-environment node
//
// The client where there is no page: Node, Electron's main process, a
// worker. It says when the storage it was named cannot be had here, rather
// than writing into nothing; it connects again at once when the application
// says the network or the machine is back (wake); and it sends what it holds
// before it ends (close, with a deadline).

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, tick } from './helpers.js';
import { createWSEClient, type WSEClient } from '../core/index.js';

const URL = 'ws://runtime.test/wse';

let clients: WSEClient[] = [];

function client(options: Parameters<typeof createWSEClient>[0] = {}): WSEClient {
  const made = createWSEClient({ endpoints: [URL], offline: { enabled: false }, ...options });
  clients.push(made);
  return made;
}

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  vi.useFakeTimers();
});

afterEach(() => {
  for (const made of clients) made.destroy();
  clients = [];
  resetClientState();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('a client without a page', () => {
  it('says the storage it was named cannot be had here, once each, and holds its topics in memory', async () => {
    const errors: Array<{ operation: string; message: string }> = [];
    const c = client({
      storage: 'app:alice',
      consumer: 'board',
      topics: ['news'],
      offline: { enabled: true, persistToStorage: true },
    });
    c.on('wseStorageError', (detail) => errors.push(detail));
    expect(c.state.getSnapshot().storage).toBe('unavailable');
    c.retain();
    await tick();
    await connectServer(FakeWebSocket.last());
    // The page storage for the topics and the checkpoint, IndexedDB for the queue.
    expect(errors.map((e) => e.operation)).toEqual(['open', 'open']);
    expect(errors.map((e) => e.message).join(' | ')).toMatch(/page storage.*\|.*IndexedDB/);
    expect(c.state.getSnapshot().storage).toBe('unavailable');
    expect(c.state.getSnapshot().topics).toContain('news');
  });

  it('connects again at once on wake instead of waiting out the backoff', async () => {
    const c = client({ reconnection: { baseDelay: 20_000, maxDelay: 30_000 } });
    c.retain();
    await tick();
    await connectServer(FakeWebSocket.last());
    FakeWebSocket.last().serverClose(1006);
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);
    c.wake();
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(2);
  });

  it('leaves a connection that is alive alone on wake', async () => {
    const c = client();
    c.retain();
    await tick();
    await connectServer(FakeWebSocket.last());
    c.wake();
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);
    expect(FakeWebSocket.last().closeCalls).toHaveLength(0);
  });

  it('makes a connection silent past its idle limit again on wake (a sleep no timer saw)', async () => {
    const c = client();
    c.retain();
    await tick();
    await connectServer(FakeWebSocket.last());
    // The clock moved on while nothing ran: the machine slept.
    vi.setSystemTime(Date.now() + 10 * 60_000);
    c.wake();
    await tick();
    expect(FakeWebSocket.instances[0].closeCalls.length).toBeGreaterThan(0);
    expect(FakeWebSocket.instances).toHaveLength(2);
  });

  it('sends what it holds before it ends on close', async () => {
    const c = client();
    c.retain();
    await tick();
    expect(c.send('order', { id: 1 })).toBe('queued');
    const closing = c.close(2_000);
    await connectServer(FakeWebSocket.last());
    await vi.advanceTimersByTimeAsync(100);
    await closing;
    const ws = FakeWebSocket.last();
    expect(ws.messagesOfType('order').map((m) => m.p)).toEqual([{ id: 1 }]);
    expect(ws.closeCalls.length).toBeGreaterThan(0);
    expect(c.send('order', { id: 2 })).toBe('dropped');
  });

  it('ends at the deadline on close when what it holds cannot go', async () => {
    const c = client();
    c.retain();
    await tick();
    c.send('order', { id: 1 });
    let ended = false;
    void c.close(1_000).then(() => {
      ended = true;
    });
    await vi.advanceTimersByTimeAsync(990);
    expect(ended).toBe(false);
    await vi.advanceTimersByTimeAsync(50);
    expect(ended).toBe(true);
    expect(c.send('order', { id: 2 })).toBe('dropped');
  });

  it('ends at once on close when it runs no connection', async () => {
    const c = client();
    c.send('order', { id: 1 });
    await c.close(5_000);
    expect(c.send('order', { id: 2 })).toBe('dropped');
  });
});
