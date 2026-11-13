// End-to-end against the real Rust server (opt-in).
//
// Runs only when WSE_E2E_PYTHON points to a Python that can import
// wse_server (an installed wheel, or this repository's prebuilt module):
//
//   WSE_E2E_PYTHON=.venv/bin/python npx vitest run ts-client/__tests__/realServer.test.ts
//
// Covers what fakes cannot prove: JWT cookie auth, an E2E session with the
// server's own implementation (its identity key pinned), stamped fan-out, a
// server kick (close 1005) followed by a recovery replay, a server stop (1001)
// without ever entering ERROR, a restart (new epoch) and sign-out; and the
// first E2E design, which the server still speaks to older clients.

import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import {
  acquireSession,
  destroyCurrentSession,
  getCurrentSession,
  makeSessionKey,
  wseActions,
} from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { logger, LogLevel } from '../utils/logger.js';
import type { WSEGapEvent } from '../types.js';

const PYTHON = process.env.WSE_E2E_PYTHON;

describe.skipIf(!PYTHON || typeof WebSocket === 'undefined')('real RustWSEServer', () => {
  const port = 5600 + Math.floor(Math.random() * 300);
  const endpoint = `ws://127.0.0.1:${port}/wse`;
  const events: any[] = [];
  let server: ChildProcessWithoutNullStreams;
  let token = '';
  let identity = '';

  const send = (command: string) => server.stdin.write(`${command}\n`);
  const transportReady = () => !!getCurrentSession()?.connection?.isTransportReady();

  beforeAll(async () => {
    logger.setLevel(LogLevel.ERROR);
    server = spawn(PYTHON!, ['-I', fileURLToPath(new URL('./e2e/server.py', import.meta.url)), String(port)]);
    createInterface({ input: server.stdout }).on('line', (line) => {
      try {
        const ev = JSON.parse(line);
        if (ev.event === 'token') token = ev.token;
        else if (ev.event === 'identity') identity = ev.key;
        else events.push(ev);
      } catch {
        // not an event line
      }
    });
    await vi.waitFor(() => {
      expect(token).not.toBe('');
      expect(identity).not.toBe('');
      expect(events.some((e) => e.event === 'started')).toBe(true);
    }, { timeout: 15_000 });

    // A browser sends the auth cookie by itself; Node's WebSocket takes it as a header.
    const Native = globalThis.WebSocket;
    vi.stubGlobal('WebSocket', class extends Native {
      constructor(url: string | URL) {
        super(url, { headers: { cookie: `access_token=${token}` } } as any);
      }
    });
  });

  afterAll(() => {
    destroyCurrentSession({ clearUserData: true });
    vi.unstubAllGlobals();
    send('quit');
    server.kill();
  });

  it('runs a full session lifecycle with encryption and recovery', async () => {
    const offsets: number[] = [];
    const epochs = new Set<string>();
    const echoes: any[] = [];
    const gaps: WSEGapEvent[] = [];
    const states: string[] = [];
    const unsubscribe = useWSEStore.subscribe((s, p) => {
      if (s.connectionState !== p.connectionState) states.push(s.connectionState);
    });

    const session = acquireSession({
      key: makeSessionKey('cookie', [endpoint]),
      auth: 'cookie',
      initialTopics: ['prices'],
      config: {
        endpoints: [endpoint],
        offline: { enabled: false },
        security: { encryptionEnabled: true, serverKeys: [identity] },
        reconnection: { baseDelay: 200, maxDelay: 500 },
        registerHandlers: (p) => {
          p.registerHandler('price_update', (m) => {
            offsets.push(m.o);
            epochs.add(m.e);
          });
          p.registerHandler('echo_reply', (m) => echoes.push(m.p));
        },
      },
    });
    session.processor.setGapListener((g) => gaps.push(g));

    // JWT cookie auth, server_ready, the E2E handshake signed by the pinned key.
    await vi.waitFor(() => expect(transportReady()).toBe(true), { timeout: 10_000 });
    await vi.waitFor(() => expect(events.some((e) => e.event === 'subscribe')).toBe(true), { timeout: 5_000 });
    expect(useWSEStore.getState().security.encryptionEnabled).toBe(true);

    // Stamped, encrypted fan-out.
    send('publish 5');
    await vi.waitFor(() => expect(offsets).toHaveLength(5), { timeout: 5_000 });
    expect(offsets).toEqual([offsets[0], offsets[0] + 1, offsets[0] + 2, offsets[0] + 3, offsets[0] + 4]);

    // Encrypted client -> server message, decrypted and answered by the server.
    expect(wseActions.sendMessage('echo', { n: 42 })).toBe('sent');
    await vi.waitFor(() => expect(echoes.some((p) => p.n === 42)).toBe(true), { timeout: 5_000 });
    expect(events.some((e) => e.event === 'echo' && e.payload?.n === 42)).toBe(true);

    // Server kick (close 1005) while messages keep flowing: reconnect + replay, no hole.
    const before = offsets.length;
    send('kick');
    await vi.waitFor(() => expect(events.some((e) => e.event === 'kicked')).toBe(true), { timeout: 5_000 });
    send('publish 4');
    await vi.waitFor(() => {
      expect(offsets.length).toBeGreaterThanOrEqual(before + 4);
      expect(transportReady()).toBe(true);
    }, { timeout: 10_000 });
    const resubscribe = events.filter((e) => e.event === 'subscribe').pop();
    expect(resubscribe.recover).toBe(true);
    expect(resubscribe.recovered).toBe(true);
    offsets.forEach((o, i) => {
      if (i > 0) expect(o).toBe(offsets[i - 1] + 1);
    });
    expect(gaps).toEqual([]);

    // Server stop (1001): keeps retrying, never ERROR; restart: reconnect on a new epoch.
    send('stop');
    await vi.waitFor(() => expect(events.some((e) => e.event === 'stopped')).toBe(true), { timeout: 5_000 });
    await new Promise((r) => setTimeout(r, 2_000));
    expect(useWSEStore.getState().connectionState).toBe('reconnecting');
    send('start');
    await vi.waitFor(() => expect(transportReady()).toBe(true), { timeout: 15_000 });
    send('publish 3');
    await vi.waitFor(() => expect(epochs.size).toBe(2), { timeout: 10_000 });
    expect(gaps.map((g) => g.reason)).toContain('epoch_changed');
    expect(states).not.toContain('error');

    unsubscribe();
  }, 60_000);

  it('speaks the first E2E design to a server that still offers it', async () => {
    destroyCurrentSession({ clearUserData: true });
    const echoes: any[] = [];
    acquireSession({
      key: makeSessionKey('cookie', [endpoint]),
      auth: 'cookie',
      initialTopics: [],
      config: {
        endpoints: [endpoint],
        offline: { enabled: false },
        security: { encryptionEnabled: true, legacyE2e: true },
        registerHandlers: (p) => {
          p.registerHandler('echo_reply', (m) => echoes.push(m.p));
        },
      },
    });

    await vi.waitFor(() => expect(transportReady()).toBe(true), { timeout: 10_000 });
    expect(wseActions.sendMessage('echo', { n: 7 })).toBe('sent');
    await vi.waitFor(() => expect(echoes.some((p) => p.n === 7)).toBe(true), { timeout: 5_000 });
  }, 30_000);

  it('speaks msgpack with the server both ways', async () => {
    destroyCurrentSession({ clearUserData: true });
    const echoes: any[] = [];
    const prices: number[] = [];
    const subscribed = events.filter((e) => e.event === 'subscribe').length;
    acquireSession({
      key: makeSessionKey('cookie', [endpoint]),
      auth: 'cookie',
      initialTopics: ['prices'],
      config: {
        endpoints: [endpoint],
        offline: { enabled: false },
        wireFormat: 'msgpack',
        registerHandlers: (p) => {
          p.registerHandler('echo_reply', (m) => echoes.push(m.p));
          p.registerHandler('price_update', (m) => prices.push(m.p.seq));
        },
      },
    });

    // The subscription went in msgpack, and the server read it.
    await vi.waitFor(() => expect(transportReady()).toBe(true), { timeout: 10_000 });
    await vi.waitFor(
      () => expect(events.filter((e) => e.event === 'subscribe').length).toBeGreaterThan(subscribed),
      { timeout: 5_000 },
    );
    send('publish 2');
    await vi.waitFor(() => expect(prices.length).toBeGreaterThanOrEqual(2), { timeout: 5_000 });
    expect(wseActions.sendMessage('echo', { n: 9 })).toBe('sent');
    await vi.waitFor(() => expect(echoes.some((p) => p.n === 9)).toBe(true), { timeout: 5_000 });
  }, 30_000);
});
