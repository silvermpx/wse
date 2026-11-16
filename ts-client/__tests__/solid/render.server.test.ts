// The server render on Solid 1.9 (its build for a server): the helpers take
// nothing of the client, and the page shows its first state.

import { afterEach, describe, expect, it, vi } from 'vitest';
import { renderToString } from 'solid-js/web';
import { createWSEClient } from '../../core/index.js';
import { createWSEState, createWSETopics, onWSEEvent, onWSEMessage } from '../../solid/index.js';

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('the Solid server render', () => {
  it('shows the first state and takes nothing', () => {
    expect(typeof window).toBe('undefined');
    const opened = vi.fn();
    vi.stubGlobal('WebSocket', opened);
    const client = createWSEClient({ endpoints: ['ws://ssr.test/wse'], offline: { enabled: false } });
    const taken = [
      vi.spyOn(client, 'retain'),
      vi.spyOn(client, 'subscribe'),
      vi.spyOn(client, 'onMessage'),
      vi.spyOn(client, 'on'),
      vi.spyOn(client.state, 'listen'),
    ];
    const html = renderToString(() => {
      const state = createWSEState(client);
      createWSETopics(['news'], client);
      onWSEMessage('news', () => {}, client);
      onWSEEvent('wseGap', () => {}, client);
      return `connection: ${state().connection}`;
    });
    expect(html).toContain('connection: pending');
    for (const spy of taken) expect(spy).not.toHaveBeenCalled();
    expect(opened).not.toHaveBeenCalled();
    client.destroy();
  });
});
