// @vitest-environment happy-dom
//
// The framework adapters take a window's bridge client as they take a client
// of its own: two windows' components on one connection.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { StrictMode, createElement } from 'react';
import { act, cleanup, render } from '@testing-library/react';
import { FakeWebSocket, installFakeWebSocket, resetClientState, serverReadyMessage, stamped } from './helpers.js';
import {
  createBridgeClient,
  createBridgeOwner,
  messagePortChannel,
  type BridgeClient,
  type BridgeOwner,
  type OwnerFrame,
  type WindowFrame,
} from '../bridge/index.js';
import { WSEProvider, useWSEMessage, useWSEState, useWSETopics } from '../react/index.js';

const URL = 'ws://adapters.test/wse';
let owner: BridgeOwner;
let windows: BridgeClient[];

function window(): BridgeClient {
  const { port1, port2 } = new MessageChannel();
  owner.attach(messagePortChannel<OwnerFrame, WindowFrame>(port1 as never));
  const made = createBridgeClient(messagePortChannel<WindowFrame, OwnerFrame>(port2 as never));
  windows.push(made);
  return made;
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 30));

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  owner = createBridgeOwner({ endpoints: [URL], offline: { enabled: false } });
  windows = [];
});

afterEach(() => {
  cleanup();
  for (const w of windows) w.destroy();
  owner.destroy();
  resetClientState();
  vi.unstubAllGlobals();
});

describe('react over the bridge', () => {
  function Prices() {
    const state = useWSEState();
    useWSETopics(['prices']);
    const last = { n: 0 };
    useWSEMessage('price_update', (message) => {
      last.n = message.p.n;
      document.title = String(message.p.n);
    });
    return createElement('span', null, `${state.connection}:${state.topicStates.prices?.subscription ?? '-'}`);
  }

  it('renders two windows’ state on one connection, under Strict Mode', async () => {
    const [a, b] = [window(), window()];
    const views = [a, b].map((client) =>
      render(createElement(StrictMode, null, createElement(WSEProvider, { client }, createElement(Prices)))),
    );
    await act(async () => {
      await settle();
    });
    expect(FakeWebSocket.instances).toHaveLength(1);
    const ws = FakeWebSocket.last();
    await act(async () => {
      ws.serverOpen();
      ws.serverSend(serverReadyMessage({ user_id: 'ann' }));
      ws.serverSend({
        c: 'WSE',
        t: 'subscription_update',
        p: { action: 'subscribe', success: true, success_topics: ['prices'], failed_topics: [] },
        v: 1,
      });
      await settle();
    });
    for (const view of views) expect(view.container.textContent).toBe('connected:active');
    await act(async () => {
      ws.serverSend(stamped('prices', 1, '0000000a', { n: 7 }));
      await settle();
    });
    expect(document.title).toBe('7');
    // One subscription for both windows.
    expect(ws.messagesOfType('subscription_update')).toHaveLength(1);
    views[0].unmount();
    await act(async () => {
      await settle();
    });
    expect(views[1].container.textContent).toBe('connected:active');
  });
});
