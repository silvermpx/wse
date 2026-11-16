// @vitest-environment happy-dom
//
// Hydration: the page the server rendered takes over in the browser with no
// mismatch, and only then the client connects.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from 'react';
import { hydrateRoot } from 'react-dom/client';
import { renderToString as reactToString } from 'react-dom/server';
import { renderToString as vueToString } from 'vue/server-renderer';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, tick } from './helpers.js';
// Svelte's browser entry: Node's resolution picks its server one for 'svelte'.
import { flushSync, hydrate, unmount } from '../../node_modules/svelte/src/index-client.js';
import { render as svelteRender } from 'svelte/server';
import { newClient, reactPage, vuePage } from './ssr/render.js';
import { compiledStatus } from './ssr/svelte.js';

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  (globalThis as any).IS_REACT_ACT_ENVIRONMENT = true;
});

afterEach(() => {
  resetClientState();
  vi.unstubAllGlobals();
});

/** What React and Vue say of a mismatch. */
function mismatches() {
  const said: string[] = [];
  const heard = (...args: unknown[]) => {
    const text = args.map(String).join(' ');
    if (/hydrat|mismatch|did not match/i.test(text)) said.push(text);
  };
  vi.spyOn(console, 'error').mockImplementation(heard);
  vi.spyOn(console, 'warn').mockImplementation(heard);
  return said;
}

describe('hydration', () => {
  it('of React matches the server render, then connects', async () => {
    const server = newClient();
    const html = reactToString(reactPage(server));
    server.destroy();
    expect(FakeWebSocket.instances).toHaveLength(0);

    const said = mismatches();
    const container = document.createElement('div');
    container.innerHTML = html;
    document.body.appendChild(container);
    const client = newClient();
    let root: ReturnType<typeof hydrateRoot> | undefined;
    await act(async () => {
      root = hydrateRoot(container, reactPage(client));
      await tick();
    });
    expect(said).toEqual([]);
    expect(FakeWebSocket.instances).toHaveLength(1);
    await act(async () => {
      await connectServer(FakeWebSocket.last());
    });
    expect(container.textContent).toBe('connection: connected');
    expect([...FakeWebSocket.last().messagesOfType('subscription_update')[0].p.topics].sort()).toEqual(['news', 'prices']);
    await act(async () => root!.unmount());
    client.destroy();
    container.remove();
  });

  it('of Vue matches the server render, then connects', async () => {
    const server = newClient();
    const html = await vueToString(vuePage(server));
    server.destroy();
    expect(FakeWebSocket.instances).toHaveLength(0);

    const said = mismatches();
    const container = document.createElement('div');
    container.innerHTML = html;
    document.body.appendChild(container);
    const client = newClient();
    const app = vuePage(client);
    app.mount(container);
    await tick();
    expect(said).toEqual([]);
    expect(FakeWebSocket.instances).toHaveLength(1);
    await connectServer(FakeWebSocket.last());
    await tick();
    expect(container.textContent).toBe('connection: connected');
    app.unmount();
    client.destroy();
    container.remove();
  });

  it('of Svelte matches the server render, then connects', async () => {
    const { default: ServerStatus } = await import(compiledStatus('server'));
    const { default: Status } = await import(compiledStatus('client'));
    // Rendered as on a server: there is no window there.
    vi.stubGlobal('window', undefined);
    const server = newClient();
    const { body } = svelteRender(ServerStatus, { props: { client: server } });
    server.destroy();
    vi.unstubAllGlobals();
    installFakeWebSocket();
    expect(FakeWebSocket.instances).toHaveLength(0);

    const said = mismatches();
    const container = document.createElement('div');
    container.innerHTML = body;
    document.body.appendChild(container);
    const client = newClient();
    const page = hydrate(Status, { target: container, props: { client } });
    await tick();
    expect(said).toEqual([]);
    expect(FakeWebSocket.instances).toHaveLength(1);
    await connectServer(FakeWebSocket.last());
    flushSync();
    expect(container.textContent).toBe('connection: connected');
    unmount(page);
    client.destroy();
    container.remove();
  });
});
