// =============================================================================
// WebSocket Engine - Svelte (wse-client/svelte)
// =============================================================================
//
// Svelte stores over a client of its own (createWSEClient), by Svelte's
// store contract, so `$store` works in Svelte 4 and 5 without importing
// Svelte. A store holds the client's connection while anything subscribes
// to it in the browser; server rendering subscribes too, but opens nothing.

import type { WSEClient, WSEClientSnapshot } from '../core/client.js';
import type { WSMessage } from '../types.js';

export * from '../core/index.js';

/** Svelte's readable store contract. */
export interface Readable<T> {
  subscribe(run: (value: T) => void): () => void;
}

const inBrowser = (): boolean => typeof window !== 'undefined' && typeof document !== 'undefined';

/** A share of `client` while subscribed in the browser. */
function hold(client: WSEClient): () => void {
  if (!inBrowser()) return () => {};
  const lease = client.retain();
  return () => lease.release();
}

/** The client's state. */
export function wseState(client: WSEClient): Readable<WSEClientSnapshot> {
  return {
    subscribe(run) {
      const stop = client.state.subscribe(run);
      const release = hold(client);
      return () => {
        stop();
        release();
      };
    },
  };
}

/** The last message of `type` (null until one comes); `topics` are held while subscribed. */
export function wseMessage(client: WSEClient, type: string, topics: string[] = []): Readable<WSMessage | null> {
  let last: WSMessage | null = null;
  return {
    subscribe(run) {
      run(last);
      const release = hold(client);
      const lease = inBrowser() && topics.length > 0 ? client.subscribe(topics) : null;
      const stop = client.onMessage(type, (message) => {
        last = message;
        run(message);
      });
      return () => {
        stop();
        lease?.release();
        release();
      };
    },
  };
}
