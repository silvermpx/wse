// =============================================================================
// WebSocket Engine - Vue (wse-client/vue)
// =============================================================================
//
// Vue 3 over a client of its own (createWSEClient): provide it to the app or
// to a component's subtree, and use composables that hold its connection,
// topics and listeners from a component's mount to its unmount. Mount hooks
// never run while rendering on a server, so server rendering opens no socket.

import { inject, onBeforeUnmount, onMounted, provide, shallowRef } from 'vue';
import type { App, InjectionKey, ShallowRef } from 'vue';
import type { Lease, WSEClient, WSEClientSnapshot } from '../core/client.js';
import type { WSMessage } from '../types.js';

export * from '../core/index.js';

export const WSE_CLIENT: InjectionKey<WSEClient> = Symbol('wse-client');

/** `app.use(wsePlugin(client))`: every component of the app gets `client`. */
export function wsePlugin(client: WSEClient) {
  return {
    install(app: App) {
      app.provide(WSE_CLIENT, client);
    },
  };
}

/** Give the components below this one `client` (call in setup). */
export function provideWSEClient(client: WSEClient): void {
  provide(WSE_CLIENT, client);
}

/** The client provided above (call in setup). */
export function useWSEClient(): WSEClient {
  const client = inject(WSE_CLIENT, null);
  if (!client) throw new Error('useWSEClient: no client provided (wsePlugin or provideWSEClient)');
  return client;
}

/** Run `start` from the component's mount; what it returns runs at its unmount. */
function whileMounted(start: () => () => void): void {
  let stop: (() => void) | null = null;
  onMounted(() => {
    stop = start();
  });
  onBeforeUnmount(() => {
    stop?.();
    stop = null;
  });
}

/** The client's state, held (and connected) while the component is mounted. */
export function useWSEState(client: WSEClient = useWSEClient()): Readonly<ShallowRef<WSEClientSnapshot>> {
  const state = shallowRef(client.state.getSnapshot());
  whileMounted(() => {
    const lease: Lease = client.retain();
    state.value = client.state.getSnapshot();
    const stop = client.state.listen((snapshot) => {
      state.value = snapshot;
    });
    return () => {
      stop();
      lease.release();
    };
  });
  return state;
}

/** Hold `topics` while the component is mounted. */
export function useWSETopics(topics: string[], client: WSEClient = useWSEClient()): void {
  whileMounted(() => {
    const connection = client.retain();
    const lease = client.subscribe(topics);
    return () => {
      lease.release();
      connection.release();
    };
  });
}

/** Hear the messages of `type` while the component is mounted. */
export function useWSEMessage(
  type: string,
  handler: (message: WSMessage) => void,
  client: WSEClient = useWSEClient(),
): void {
  whileMounted(() => client.onMessage(type, handler));
}

/** Hear the client's events of `event` (`wseGap`, ...) while the component is mounted. */
export function useWSEEvent(event: string, handler: (detail: any) => void, client: WSEClient = useWSEClient()): void {
  whileMounted(() => client.on(event, handler));
}
