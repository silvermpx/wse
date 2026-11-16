// The server render (Next, Nuxt, SvelteKit render the same way): no window,
// no socket, no timer left behind; the page shows the client's first state.

import { afterEach, describe, expect, it, vi } from 'vitest';
import { renderToString as reactToString } from 'react-dom/server';
import { renderToString as vueToString } from 'vue/server-renderer';
import { render as svelteRender } from 'svelte/server';
import { newClient, reactPage, vuePage } from './ssr/render.js';
import { createWSEClient } from '../core/index.js';
import { compiledStatus } from './ssr/svelte.js';

afterEach(() => {
  vi.unstubAllGlobals();
});

function noSocket() {
  const opened = vi.fn();
  vi.stubGlobal('WebSocket', opened);
  return opened;
}

describe('the server render', () => {
  it('of React shows the first state and opens nothing', () => {
    expect(typeof window).toBe('undefined');
    const opened = noSocket();
    vi.useFakeTimers();
    try {
      const client = newClient();
      const html = reactToString(reactPage(client));
      expect(html).toContain('connection: pending');
      expect(opened).not.toHaveBeenCalled();
      // Nothing left running on the server.
      expect(vi.getTimerCount()).toBe(0);
      client.destroy();
    } finally {
      vi.useRealTimers();
    }
  });

  it('of Vue shows the first state and opens nothing', async () => {
    const opened = noSocket();
    const client = newClient();
    const html = await vueToString(vuePage(client));
    expect(html).toContain('connection: pending');
    expect(opened).not.toHaveBeenCalled();
    client.destroy();
  });

  it('of Svelte shows the first state and opens nothing', async () => {
    const opened = noSocket();
    const { default: Status } = await import(compiledStatus('server'));
    const client = newClient();
    const { body } = svelteRender(Status, { props: { client } });
    expect(body).toContain('connection: pending');
    expect(opened).not.toHaveBeenCalled();
    client.destroy();
  });

  it("leaves a server runtime's own localStorage alone", () => {
    // Node 25 has a localStorage of its own: one for the whole server, not a
    // page's, so whatever one request kept there the next would read.
    const touched: string[] = [];
    vi.stubGlobal('localStorage', {
      get length() {
        return 0;
      },
      key: () => null,
      getItem: (key: string) => (touched.push(`get ${key}`), null),
      setItem: (key: string) => void touched.push(`set ${key}`),
      removeItem: (key: string) => void touched.push(`remove ${key}`),
      clear: () => void touched.push('clear'),
    });
    noSocket();
    const client = createWSEClient({
      endpoints: ['ws://ssr.test/wse'],
      storage: 'app:alice',
      consumer: 'board',
      topics: ['prices'],
    });
    reactToString(reactPage(client));
    client.subscribe(['news']).release();
    client.signOut();
    expect(touched).toEqual([]);
  });
});
