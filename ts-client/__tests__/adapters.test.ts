// @vitest-environment happy-dom
//
// The framework adapters hold a client's connection while a component (or a
// store subscriber) is there, render its state, and let it go after.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { StrictMode, createElement } from 'react';
import { act, cleanup, render } from '@testing-library/react';
import { createApp, defineComponent, h } from 'vue';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, tick } from './helpers.js';
import { createWSEClient, type WSEClient } from '../core/index.js';
import { WSEProvider, useWSEMessage, useWSEState, useWSETopics } from '../react/index.js';
import { useWSEState as useVueState, wsePlugin } from '../vue/index.js';
import { wseState } from '../svelte/index.js';

const URL = 'ws://adapters.test/wse';
let client: WSEClient;

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  vi.useFakeTimers();
  client = createWSEClient({ endpoints: [URL], offline: { enabled: false } });
});

afterEach(() => {
  cleanup();
  client.destroy();
  resetClientState();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('react', () => {
  function Status() {
    const state = useWSEState();
    useWSETopics(['prices']);
    useWSEMessage('price_update', () => {});
    return createElement('span', null, state.connection);
  }

  it('connects while mounted, renders the state, and lets go after', async () => {
    const view = render(createElement(StrictMode, null, createElement(WSEProvider, { client }, createElement(Status))));
    await act(async () => {
      await tick();
    });
    // Strict Mode mounts twice: still one connection.
    expect(FakeWebSocket.instances).toHaveLength(1);
    await act(async () => {
      await connectServer(FakeWebSocket.last());
    });
    expect(view.container.textContent).toBe('connected');
    expect(FakeWebSocket.last().messagesOfType('subscription_update')[0].p.topics).toEqual(['prices']);
    view.unmount();
    await vi.advanceTimersByTimeAsync(10);
    expect(FakeWebSocket.last().closeCalls.length).toBeGreaterThan(0);
  });
});

describe('vue', () => {
  it('connects from mount to unmount and renders the state', async () => {
    const App = defineComponent({
      setup() {
        const state = useVueState();
        return () => h('span', state.value.connection);
      },
    });
    const root = document.createElement('div');
    const app = createApp(App).use(wsePlugin(client));
    app.mount(root);
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);
    await connectServer(FakeWebSocket.last());
    await tick();
    expect(root.textContent).toBe('connected');
    app.unmount();
    await vi.advanceTimersByTimeAsync(10);
    expect(FakeWebSocket.last().closeCalls.length).toBeGreaterThan(0);
  });
});

describe('svelte', () => {
  it('gives the state at once and holds the connection while subscribed', async () => {
    const seen: string[] = [];
    const stop = wseState(client).subscribe((state) => seen.push(state.connection));
    expect(seen[0]).toBe('pending');
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);
    await connectServer(FakeWebSocket.last());
    expect(seen.at(-1)).toBe('connected');
    stop();
    await vi.advanceTimersByTimeAsync(10);
    expect(FakeWebSocket.last().closeCalls.length).toBeGreaterThan(0);
  });

  it('opens nothing while rendering on a server', async () => {
    vi.stubGlobal('window', undefined);
    const seen: string[] = [];
    const stop = wseState(client).subscribe((state) => seen.push(state.connection));
    stop();
    await tick();
    expect(seen).toEqual(['pending']);
    expect(FakeWebSocket.instances).toHaveLength(0);
  });
});
