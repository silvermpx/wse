// @vitest-environment happy-dom
//
// The React hook: stable actions, one shared connection per page with a
// lifetime bound to its consumers, StrictMode safety, cancellation of an
// in-flight initialization, logout clearing per-user data, and no re-render
// per inbound message.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { StrictMode, createElement, type ReactNode } from 'react';
import {
  ENDPOINT,
  FakeWebSocket,
  connectServer,
  installFakeWebSocket,
  resetClientState,
  stamped,
} from './helpers.js';
import { useWSE } from '../hooks/useWSE.js';
import type { UseWSEConfig } from '../hooks/useWSE.js';
import { getCurrentSession } from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { SUBSCRIPTION_STORAGE_KEY, saveSubscriptions } from '../constants.js';
import { ConnectionState } from '../types.js';

const config = (): UseWSEConfig => ({
  endpoints: [ENDPOINT],
  offline: { enabled: false },
  reconnection: { baseDelay: 10, maxDelay: 20 },
});

const liveSockets = () => FakeWebSocket.instances.filter((ws) => ws.closeCalls.length === 0 && ws.readyState !== FakeWebSocket.CLOSED);

const strict = ({ children }: { children: ReactNode }) => createElement(StrictMode, null, children);

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  localStorage.clear();
});

afterEach(() => {
  resetClientState();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('useWSE actions', () => {
  it('returns the same function identities on every render and store update', async () => {
    const { result, rerender } = renderHook(
      // Inline topics and config objects: new identities on every render.
      ({ token }) => useWSE(token, ['a'], config()),
      { initialProps: { token: 'cookie' } },
    );
    const first = result.current;

    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await act(async () => {
      await connectServer(FakeWebSocket.last());
    });
    rerender({ token: 'cookie' });
    act(() => {
      useWSEStore.getState().incrementMetric('messagesReceived');
      useWSEStore.getState().addTopics(['b']);
    });

    for (const name of ['subscribe', 'unsubscribe', 'sendMessage', 'sendBatch', 'forceReconnect', 'changeEndpoint', 'requestSnapshot'] as const) {
      expect(result.current[name]).toBe(first[name]);
    }
    // Inline config/topics never caused a reconnect.
    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it('an effect that depends on subscribe/unsubscribe does not loop', async () => {
    let effectRuns = 0;
    const { result } = renderHook(() => {
      const wse = useWSE('cookie', [], config());
      // The ChatRoom pattern from the docs.
      const { subscribe, unsubscribe } = wse;
      useStableEffect(() => {
        effectRuns++;
        subscribe(['chat:1']);
        return () => unsubscribe(['chat:1']);
      }, [subscribe, unsubscribe]);
      return wse;
    });

    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await act(async () => {
      await connectServer(ws);
      for (let i = 1; i <= 20; i++) ws.serverSend(stamped('chat:1', i));
      await new Promise((r) => setTimeout(r, 20));
    });

    expect(effectRuns).toBe(1);
    expect(result.current.activeTopics).toEqual(['chat:1']);
    expect(ws.messagesOfType('subscription_update').filter((m) => m.p.action === 'unsubscribe')).toHaveLength(0);
  });

  it('two subscribes in the same tick both survive and are resubscribed after a reconnect', async () => {
    const { result } = renderHook(() => useWSE('cookie', ['a'], config()));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await act(async () => {
      await connectServer(FakeWebSocket.last());
    });

    act(() => {
      result.current.subscribe(['x']);
      result.current.subscribe(['y']);
    });
    expect(useWSEStore.getState().activeTopics).toEqual(['a', 'x', 'y']);

    await act(async () => {
      FakeWebSocket.last().serverClose(1006);
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const ws2 = FakeWebSocket.last();
    await act(async () => {
      await connectServer(ws2);
    });
    expect(ws2.messagesOfType('subscription_update')[0].p.topics).toEqual(['a', 'x', 'y']);
  });
});

// A tiny useEffect wrapper so the test reads like application code.
import { useEffect } from 'react';
function useStableEffect(effect: () => void | (() => void), deps: unknown[]) {
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(effect, deps);
}

describe('connection lifetime', () => {
  it('StrictMode opens exactly one socket', async () => {
    renderHook(() => useWSE('cookie', ['a'], config()), { wrapper: strict });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await new Promise((r) => setTimeout(r, 30));
    expect(FakeWebSocket.instances).toHaveLength(1);
    expect(FakeWebSocket.last().closeCalls).toHaveLength(0);
  });

  it('closes the connection when the last consumer unmounts', async () => {
    const a = renderHook(() => useWSE('cookie', ['a'], config()));
    const b = renderHook(() => useWSE('cookie', ['b'], config()));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await act(async () => {
      await connectServer(ws);
    });
    // The second consumer's topics joined the shared connection.
    expect(useWSEStore.getState().activeTopics).toEqual(['a', 'b']);

    a.unmount();
    await new Promise((r) => setTimeout(r, 10));
    expect(ws.closeCalls).toHaveLength(0);

    b.unmount();
    await vi.waitFor(() => expect(ws.closeCalls).toHaveLength(1));
    expect(getCurrentSession()).toBeNull();
    expect(useWSEStore.getState().connectionState).toBe(ConnectionState.DISCONNECTED);
  });

  it('a consumer with other endpoints gets its own connection settings', async () => {
    const { rerender } = renderHook(({ endpoints }) => useWSE('cookie', ['a'], { ...config(), endpoints }), {
      initialProps: { endpoints: [ENDPOINT] },
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    rerender({ endpoints: ['ws://other.test/wse'] });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    expect(FakeWebSocket.last().url).toContain('other.test');
    expect(liveSockets()).toHaveLength(1);
  });

  it('a token change during initialization leaves exactly one live socket', async () => {
    const { rerender } = renderHook(({ token }) => useWSE(token, ['a'], config()), {
      initialProps: { token: 'user-a' as string | undefined },
    });
    rerender({ token: 'user-b' });
    rerender({ token: 'user-c' });
    await vi.waitFor(() => expect(liveSockets()).toHaveLength(1));
    await new Promise((r) => setTimeout(r, 30));
    expect(liveSockets()).toHaveLength(1);
    expect(getCurrentSession()?.auth).toBe('user-c');
  });

  it('logout during initialization never connects', async () => {
    const { rerender } = renderHook(({ token }) => useWSE(token, ['a'], config()), {
      initialProps: { token: 'cookie' as string | undefined },
    });
    rerender({ token: undefined });
    await new Promise((r) => setTimeout(r, 30));
    expect(liveSockets()).toHaveLength(0);
    expect(getCurrentSession()).toBeNull();
  });
});

describe('logout and shared browsers', () => {
  it('logout clears saved subscriptions, recovery positions and queues', async () => {
    const { rerender } = renderHook(({ token }) => useWSE(token, ['private:alice'], config()), {
      initialProps: { token: 'cookie' as string | undefined },
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await act(async () => {
      await connectServer(ws, { user_id: 'alice' });
      ws.serverSend(stamped('private:alice', 1));
      await new Promise((r) => setTimeout(r, 10));
    });
    expect(localStorage.getItem(SUBSCRIPTION_STORAGE_KEY)).toContain('private:alice');
    expect(useWSEStore.getState().subscriptions.recoveryState['private:alice']).toBeDefined();

    rerender({ token: undefined });
    await vi.waitFor(() => expect(ws.closeCalls).toHaveLength(1));
    expect(localStorage.getItem(SUBSCRIPTION_STORAGE_KEY)).toBeNull();
    expect(useWSEStore.getState().activeTopics).toEqual([]);
    expect(useWSEStore.getState().subscriptions.recoveryState).toEqual({});
  });

  it("does not subscribe user B to user A's saved topics", async () => {
    saveSubscriptions(['private:alice'], 'alice');
    renderHook(() => useWSE('cookie', undefined, config()));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await act(async () => {
      await connectServer(ws, { user_id: 'bob' });
    });
    const topics = ws.messagesOfType('subscription_update')[0].p.topics;
    expect(topics).not.toContain('private:alice');
    expect(topics).toEqual(['system_events']);
  });

  it('restores the same user’s saved topics, and survives malformed storage', async () => {
    localStorage.setItem(SUBSCRIPTION_STORAGE_KEY, '{"userId":"alice","topics":"not-an-array"}');
    const first = renderHook(() => useWSE('cookie', undefined, config()));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await act(async () => {
      await connectServer(FakeWebSocket.last(), { user_id: 'alice' });
    });
    expect(FakeWebSocket.last().messagesOfType('subscription_update')[0].p.topics).toEqual(['system_events']);
    first.unmount();
    await vi.waitFor(() => expect(getCurrentSession()).toBeNull());

    resetClientState();
    saveSubscriptions(['orders', 'private:alice'], 'alice');
    renderHook(() => useWSE('cookie', undefined, config()));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await act(async () => {
      await connectServer(FakeWebSocket.last(), { user_id: 'alice' });
    });
    expect(FakeWebSocket.last().messagesOfType('subscription_update')[0].p.topics).toEqual(['orders', 'private:alice']);
  });
});

describe('render cost', () => {
  it('does not re-render per inbound message', async () => {
    let renders = 0;
    renderHook(() => {
      renders++;
      return useWSE('cookie', ['prices'], config());
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await act(async () => {
      await connectServer(ws);
    });
    const before = renders;

    await act(async () => {
      for (let i = 1; i <= 200; i++) ws.serverSend(stamped('prices', i));
      await new Promise((r) => setTimeout(r, 50));
    });

    expect(useWSEStore.getState().metrics.messagesReceived).toBeGreaterThanOrEqual(200);
    expect(renders - before).toBeLessThanOrEqual(3);
  });
});

describe('the token argument', () => {
  it('warns that a JWT passed as token is never sent, and does not put it in the URL', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const jwt = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1In0.c2lnbmF0dXJl';
    renderHook(() => useWSE(jwt, ['a'], config()));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));

    expect(FakeWebSocket.last().url).not.toContain(jwt);
    expect(warn.mock.calls.some((args) => String(args[0]).includes('never sent'))).toBe(true);
  });
});
