// @vitest-environment happy-dom
//
// Adaptive quality (TanStack Query defaults follow the measured connection),
// the compression preference, the heartbeat interval, the outgoing queue's
// memory bound, message ttl and sampled handler timing.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ENDPOINT,
  FakeWebSocket,
  connectServer,
  installFakeWebSocket,
  resetClientState,
  tick,
} from './helpers.js';
import { AdaptiveQualityManager, type QueryClientLike } from '../services/AdaptiveQualityManager.js';
import { acquireSession, makeSessionKey, wseActions } from '../services/WSESession.js';
import type { UseWSEConfig } from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { useMessageQueueStore } from '../stores/useMessageQueueStore.js';
import { ConnectionQuality, ConnectionState, MessagePriority, type QueuedMessage } from '../types.js';

class FakeQueryClient implements QueryClientLike {
  defaults: { queries?: Record<string, unknown>; [key: string]: unknown };
  setCalls = 0;
  invalidations = 0;

  constructor(defaults: { queries?: Record<string, unknown>; [key: string]: unknown } = {}) {
    this.defaults = defaults;
  }

  getDefaultOptions() {
    return this.defaults;
  }

  setDefaultOptions(options: { queries?: Record<string, unknown>; [key: string]: unknown }) {
    this.setCalls++;
    this.defaults = options;
  }

  invalidateQueries() {
    this.invalidations++;
    return Promise.resolve();
  }
}

function recordEvents(name: string): any[] {
  const events: any[] = [];
  const listener = (e: Event) => events.push((e as CustomEvent).detail);
  window.addEventListener(name, listener);
  cleanups.push(() => window.removeEventListener(name, listener));
  return events;
}

let cleanups: Array<() => void> = [];

function connected(quality: AdaptiveQualityManager) {
  quality.setConnectionState(ConnectionState.CONNECTED);
}

describe('AdaptiveQualityManager', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    cleanups.forEach((fn) => fn());
    cleanups = [];
    vi.useRealTimers();
  });

  it('leaves the app defaults alone until a round trip is measured', () => {
    const qc = new FakeQueryClient({ queries: { retry: 7 } });
    const events = recordEvents('connectionQualityChanged');
    const quality = new AdaptiveQualityManager({ queryClient: qc });
    connected(quality);
    expect(qc.setCalls).toBe(0);
    expect(events).toEqual([]);
    expect(quality.getCurrentQuality()).toBeNull();
    quality.destroy();
  });

  it('relaxes polling on a fast link and keeps the app\'s other defaults', () => {
    const qc = new FakeQueryClient({ queries: { retry: 7, staleTime: 1234 }, mutations: { retry: 1 } });
    const quality = new AdaptiveQualityManager({ queryClient: qc });
    connected(quality);
    quality.recordLatency(20);

    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.EXCELLENT);
    expect(qc.defaults.queries).toMatchObject({ retry: 7, staleTime: Infinity, refetchInterval: false });
    expect(qc.defaults.mutations).toEqual({ retry: 1 });

    // Ending the session gives the app its own values back.
    quality.destroy();
    expect(qc.defaults.queries).toEqual({ retry: 7, staleTime: 1234 });
    expect(qc.defaults.mutations).toEqual({ retry: 1 });
  });

  it('degrades at once and refetches, but relaxes only after two confirmations', () => {
    const qc = new FakeQueryClient();
    const quality = new AdaptiveQualityManager({ queryClient: qc });
    connected(quality);
    quality.recordLatency(20);
    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.EXCELLENT);

    for (let i = 0; i < 5; i++) quality.recordLatency(900);
    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.POOR);
    expect(qc.defaults.queries).toMatchObject({ refetchInterval: 10_000 });
    expect(qc.invalidations).toBe(1);

    // Five fast round trips fill the window; that first better verdict only
    // arms the upgrade, the next one confirms it.
    for (let i = 0; i < 5; i++) quality.recordLatency(20);
    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.POOR);
    quality.recordLatency(20);
    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.EXCELLENT);
    expect(qc.invalidations).toBe(1);
    quality.destroy();
  });

  it('ignores a short outage and polls through a long one', () => {
    const qc = new FakeQueryClient();
    const quality = new AdaptiveQualityManager({ queryClient: qc, outageGraceMs: 5000 });
    connected(quality);
    quality.recordLatency(20);

    quality.setConnectionState(ConnectionState.RECONNECTING);
    vi.advanceTimersByTime(3000);
    connected(quality);
    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.EXCELLENT);

    quality.setConnectionState(ConnectionState.RECONNECTING);
    vi.advanceTimersByTime(5000);
    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.POOR);
    expect(qc.defaults.queries).toMatchObject({ refetchInterval: 10_000, refetchOnWindowFocus: true });

    // Back up: polling continues until the new socket proves itself.
    connected(quality);
    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.POOR);
    quality.recordLatency(20);
    quality.recordLatency(20);
    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.EXCELLENT);
    quality.destroy();
  });

  it('treats a first connection slower than the grace period as an outage', () => {
    const qc = new FakeQueryClient();
    const quality = new AdaptiveQualityManager({ queryClient: qc, outageGraceMs: 2000 });
    vi.advanceTimersByTime(2000);
    expect(quality.getCurrentQuality()).toBe(ConnectionQuality.POOR);
    quality.destroy();
  });

  it('keeps a polling safety net on a flapping link, but not for planned reconnects', () => {
    const planned = new AdaptiveQualityManager();
    connected(planned);
    for (let i = 0; i < 3; i++) {
      planned.expectReconnect();
      planned.setConnectionState(ConnectionState.RECONNECTING);
      connected(planned);
    }
    planned.recordLatency(20);
    expect(planned.getCurrentQuality()).toBe(ConnectionQuality.EXCELLENT);
    planned.destroy();

    const flapping = new AdaptiveQualityManager();
    connected(flapping);
    for (let i = 0; i < 3; i++) {
      flapping.setConnectionState(ConnectionState.RECONNECTING);
      connected(flapping);
    }
    flapping.recordLatency(20);
    expect(flapping.getCurrentQuality()).toBe(ConnectionQuality.FAIR);
    flapping.destroy();
  });

  it('reports every change as a connectionQualityChanged event', () => {
    const events = recordEvents('connectionQualityChanged');
    const quality = new AdaptiveQualityManager();
    connected(quality);
    quality.recordLatency(200);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      quality: ConnectionQuality.FAIR,
      previous: null,
      strategy: 'Hybrid Mode',
      connected: true,
    });
    expect(events[0].metrics.latency).toBe(200);
    quality.destroy();
  });
});

// ---------------------------------------------------------------------------
// Through the session
// ---------------------------------------------------------------------------

function start(config: UseWSEConfig = {}) {
  return acquireSession({
    key: makeSessionKey('cookie', [ENDPOINT]),
    auth: 'cookie',
    initialTopics: [],
    config: { endpoints: [ENDPOINT], offline: { enabled: false }, ...config },
  });
}

describe('session options', () => {
  beforeEach(() => {
    installFakeWebSocket();
    resetClientState();
    vi.useFakeTimers();
  });

  afterEach(() => {
    cleanups.forEach((fn) => fn());
    cleanups = [];
    wseActions.setCompression(true); // the preference outlives sessions
    resetClientState();
    useMessageQueueStore.getState().setMemoryLimit(0);
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('asks for compression in client_hello and switches it at runtime', async () => {
    start({ performance: { compressionThreshold: 4096 } });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    const hello = ws.textMessages().find((m) => m.t === 'client_hello');
    expect(hello.p.features.compression).toBe(true);
    expect(hello.p.compression_threshold).toBe(4096);

    wseActions.setCompression(false);
    await tick();
    const config = ws.textMessages().filter((m) => m.t === 'client_config');
    expect(config).toHaveLength(1);
    expect(config[0].p).toEqual({ compression: false, compression_threshold: 4096 });
    expect(useWSEStore.getState().config.compressionEnabled).toBe(false);

    // The next connection asks for it from the start.
    ws.serverClose(1006);
    await vi.advanceTimersByTimeAsync(2_000);
    const ws2 = FakeWebSocket.last();
    expect(ws2).not.toBe(ws);
    await connectServer(ws2);
    const hello2 = ws2.textMessages().find((m) => m.t === 'client_hello');
    expect(hello2.p.features.compression).toBe(false);
  });

  it('pings at diagnostics.healthCheckInterval', async () => {
    start({ diagnostics: { healthCheckInterval: 5000 } });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    const before = ws.textMessages().filter((m) => m.t === 'ping').length;
    await vi.advanceTimersByTimeAsync(15_000);
    expect(ws.textMessages().filter((m) => m.t === 'ping').length - before).toBe(3);
  });

  it('drops a queued message whose ttl ran out before the connection was ready', async () => {
    start();
    await tick();
    expect(wseActions.sendMessage('quote', { n: 1 }, { ttl: 1000 })).toBe('queued');
    expect(wseActions.sendMessage('order', { n: 2 })).toBe('queued');
    await vi.advanceTimersByTimeAsync(2000);

    const dropped = useWSEStore.getState().metrics.messagesDropped;
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    const sent = ws.appTextMessages().map((m) => m.t);
    expect(sent).toContain('order');
    expect(sent).not.toContain('quote');
    expect(useWSEStore.getState().metrics.messagesDropped).toBe(dropped + 1);
  });

  it('measures sampled handler run times when diagnostics are on', async () => {
    start({
      diagnostics: { enabled: true, sampleRate: 1 },
      registerHandlers: (p) => p.registerHandler('tick', () => {}),
    });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    ws.serverSend({ c: 'U', t: 'tick', p: {}, v: 1 });
    await tick();
    expect(useWSEStore.getState().metrics.handlerTimeAvg).not.toBeNull();
  });

  it('steers a QueryClient from the measured round trips', async () => {
    const qc = new FakeQueryClient({ queries: { retry: 2 } });
    start({ queryClient: qc });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    // The heartbeat ping is answered by the fake server with a PONG.
    await vi.advanceTimersByTimeAsync(16_000);
    expect(qc.setCalls).toBeGreaterThan(0);
    expect(qc.defaults.queries).toMatchObject({ retry: 2 });
    expect(qc.defaults.queries).toHaveProperty('refetchInterval');
  });
});

describe('outgoing queue memory bound', () => {
  afterEach(() => {
    const store = useMessageQueueStore.getState();
    store.clearQueue();
    store.setMemoryLimit(0);
    store.setCapacity(10000);
  });

  function msg(id: string, priority: number, bytes: number): QueuedMessage {
    return { id, type: 't', payload: {}, priority, timestamp: Date.now(), retries: 0, bytes };
  }

  it('makes room only by evicting lower priorities, lowest first', () => {
    const store = useMessageQueueStore.getState();
    store.setMemoryLimit(1000);
    expect(store.enqueue(msg('bg', MessagePriority.BACKGROUND, 400))).toBe(true);
    expect(store.enqueue(msg('n1', MessagePriority.NORMAL, 400))).toBe(true);
    // Needs 300 more bytes: the BACKGROUND message goes, the NORMAL one stays.
    expect(useMessageQueueStore.getState().enqueue(msg('hi', MessagePriority.HIGH, 500))).toBe(true);
    const ids = useMessageQueueStore.getState().messages.map((m) => m.id);
    expect(ids).toEqual(['n1', 'hi']);
    expect(useMessageQueueStore.getState().bytes).toBe(900);

    // Nothing lower than NORMAL is left to evict: a NORMAL newcomer is refused.
    expect(useMessageQueueStore.getState().enqueue(msg('n2', MessagePriority.NORMAL, 200))).toBe(false);
    // A message larger than the whole bound never fits.
    expect(useMessageQueueStore.getState().enqueue(msg('big', MessagePriority.CRITICAL, 2000))).toBe(false);
  });

  it('reports the fuller of the two bounds as utilization', () => {
    const store = useMessageQueueStore.getState();
    store.setMemoryLimit(1000);
    store.enqueue(msg('a', MessagePriority.NORMAL, 900));
    const stats = useMessageQueueStore.getState().stats;
    expect(stats.utilizationPercent).toBeCloseTo(90);
    expect(stats.backpressure).toBe(true);
    expect(stats.bytes).toBe(900);
  });
});
