// A message sent with `live: true` goes on the connection that is ready
// when it is sent, or not at all: never held for a later connection, never
// persisted, never replayed after a reconnect. For commands that must not
// run late.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ENDPOINT, FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, tick } from './helpers.js';
import { acquireSession, makeSessionKey, wseActions } from '../services/WSESession.js';
import type { UseWSEConfig } from '../services/WSESession.js';

describe('live-only messages', () => {
  beforeEach(() => {
    installFakeWebSocket();
    resetClientState();
  });

  afterEach(() => {
    resetClientState();
  });

  function start(config: UseWSEConfig = {}) {
    return acquireSession({
      key: makeSessionKey('cookie', [ENDPOINT]),
      auth: 'cookie',
      initialTopics: [],
      config: { endpoints: [ENDPOINT], ...config },
    });
  }

  it('goes out on the ready connection', async () => {
    start();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    expect(wseActions.sendMessage('valve', { open: true }, { live: true })).toBe('sent');
    expect(ws.appTextMessages().filter((m) => m.t === 'valve')).toHaveLength(1);
  });

  it('is dropped without a ready connection, even with offline: true', async () => {
    start({ offline: { enabled: true } });
    await tick();
    const ws = FakeWebSocket.last();
    expect(wseActions.sendMessage('valve', { open: true }, { live: true, offline: true })).toBe('dropped');
    await connectServer(ws);
    await new Promise((resolve) => setTimeout(resolve, 200));
    await tick(6);
    expect(ws.appTextMessages().filter((m) => m.t === 'valve')).toHaveLength(0);
  });

  it('held behind the rate limit, it is not sent on the next connection', async () => {
    start({ rateLimit: { capacity: 1, refillRate: 1 } });
    await tick();
    const first = FakeWebSocket.last();
    await connectServer(first);
    // The one token goes to an ordinary message; the command waits.
    wseActions.sendMessage('note', {});
    await tick();
    const status = wseActions.sendMessage('valve', { open: true }, { live: true });
    await tick();
    expect(first.appTextMessages().filter((m) => m.t === 'valve')).toHaveLength(0);

    first.serverClose(1006);
    await tick();
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    const second = FakeWebSocket.last();
    expect(second).not.toBe(first);
    await connectServer(second);
    await new Promise((resolve) => setTimeout(resolve, 200));
    await tick(6);
    expect(second.appTextMessages().filter((m) => m.t === 'valve')).toHaveLength(0);
    expect(status).not.toBe('sent');
  });

  it('is dropped with no session at all, and never sent once one starts', async () => {
    // Before the first consumer mounts (and after the last one left).
    expect(wseActions.sendMessage('valve', { open: true }, { live: true })).toBe('dropped');
    start();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    await new Promise((resolve) => setTimeout(resolve, 200));
    await tick(6);
    expect(ws.appTextMessages().filter((m) => m.t === 'valve')).toHaveLength(0);
  });

  it('leaves ordinary messages as they were: held and sent on the next connection', async () => {
    start({ rateLimit: { capacity: 1, refillRate: 1 } });
    await tick();
    const first = FakeWebSocket.last();
    await connectServer(first);
    wseActions.sendMessage('note', {});
    await tick();
    expect(wseActions.sendMessage('later', {})).toBe('queued');
    first.serverClose(1006);
    await tick();
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    const second = FakeWebSocket.last();
    await connectServer(second);
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    await tick(6);
    expect(second.appTextMessages().filter((m) => m.t === 'later')).toHaveLength(1);
  });
});
