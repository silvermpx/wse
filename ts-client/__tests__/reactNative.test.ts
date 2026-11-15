// @vitest-environment happy-dom
//
// The React Native entry: the token as an Authorization header, and the
// connection paused in the background and made again in the foreground.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeWebSocket, connectServer, resetClientState, tick } from './helpers.js';
import { BACKGROUND, createReactNativeClient, type AppStateLike } from '../react-native/index.js';

class RnWebSocket extends FakeWebSocket {
  readonly options: unknown;
  constructor(url: string, _protocols?: unknown, options?: unknown) {
    super(url);
    this.options = options;
  }
}

function appState(): AppStateLike & { set(state: string): void } {
  const listeners = new Set<(state: string) => void>();
  const state = {
    currentState: 'active' as string | null,
    addEventListener: (_type: 'change', listener: (s: string) => void) => {
      listeners.add(listener);
      return { remove: () => listeners.delete(listener) };
    },
    set(next: string) {
      state.currentState = next;
      for (const listener of listeners) listener(next);
    },
  };
  return state;
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', RnWebSocket);
  resetClientState();
  vi.useFakeTimers();
});

afterEach(() => {
  resetClientState();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('react-native', () => {
  it('sends the token as a header, and pauses in the background after the grace', async () => {
    const app = appState();
    let token = 'first';
    const client = createReactNativeClient({
      endpoints: ['ws://phone.test/wse'],
      offline: { enabled: false },
      appState: app,
      backgroundGraceMs: 1_000,
      token: () => token,
    });
    client.retain();
    await tick();
    const first = FakeWebSocket.last() as RnWebSocket;
    expect(first.options).toEqual({ headers: { Authorization: 'Bearer first' } });
    await connectServer(first);

    // A dialog over the app: back before the grace, nothing happens.
    app.set('background');
    await vi.advanceTimersByTimeAsync(500);
    app.set('active');
    await vi.advanceTimersByTimeAsync(2_000);
    expect(first.closeCalls).toHaveLength(0);

    app.set('background');
    await vi.advanceTimersByTimeAsync(1_000);
    expect(client.state.getSnapshot().paused).toEqual([BACKGROUND]);
    expect(first.closeCalls.length).toBeGreaterThan(0);

    token = 'second';
    app.set('active');
    await tick();
    const second = FakeWebSocket.last() as RnWebSocket;
    expect(second).not.toBe(first);
    expect(second.options).toEqual({ headers: { Authorization: 'Bearer second' } });
    expect(client.state.getSnapshot().paused).toEqual([]);
    client.destroy();
  });
});
