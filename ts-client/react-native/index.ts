// =============================================================================
// WebSocket Engine - React Native (wse-client/react-native)
// =============================================================================
//
// The React hooks and the core, with what a phone needs: the token sent as
// an Authorization header (React Native's WebSocket takes headers), the
// connection paused while the app is in the background (after a grace
// period: Android reports background for a permission dialog too) and made
// again when it comes back, and the WebSocket's crypto from the app
// (react-native-quick-crypto). Nothing of React Native is imported here: the
// app passes AppState and its crypto in, so a web bundle never pulls them.

// First: the MessagePack codec needs TextDecoder and TextEncoder as it loads.
import './textCodec.js';
import { createWSEClient, type WSEClient, type WSEClientOptions } from '../core/client.js';
import { setWebCrypto } from '../utils/webcrypto.js';

export * from '../react/index.js';
export { storageOutbox } from './storageOutbox.js';
export type { AsyncStorageLike, MMKVLike } from './storageOutbox.js';

/** React Native's AppState, as far as the client uses it. */
export interface AppStateLike {
  currentState: string | null;
  addEventListener(type: 'change', listener: (state: string) => void): { remove(): void };
}

export interface ReactNativeClientOptions extends WSEClientOptions {
  /** React Native's AppState: the connection pauses in the background. */
  appState?: AppStateLike;
  /** How long the app may be in the background before the connection pauses (default 5000 ms). */
  backgroundGraceMs?: number;
  /**
   * The JWT, sent as `Authorization: Bearer` at each connection attempt; a
   * function is asked each time (a refreshed token). The server reads its
   * cookie first: do not keep a stale one in the app's cookie store.
   */
  token?: string | (() => string | null | undefined);
  /** react-native-quick-crypto (its `subtle` and `getRandomValues`); needed for E2E and signing. */
  crypto?: Crypto;
}

/** The background the client is paused for, by the app's state. */
export const BACKGROUND = 'background';

export interface ReactNativeClient extends WSEClient {
  /** Stops following the app's state (destroy and signOut do too). */
  stopFollowingAppState(): void;
}

export function createReactNativeClient(options: ReactNativeClientOptions = {}): ReactNativeClient {
  const { appState, backgroundGraceMs = 5_000, token, crypto, ...rest } = options;
  if (crypto) setWebCrypto(crypto);
  const tokenNow = () => (typeof token === 'function' ? token() : token) ?? null;
  const webSocket =
    rest.webSocket ??
    (token !== undefined
      ? (url: string) => {
          const bearer = tokenNow();
          // React Native's WebSocket takes headers as a third argument.
          const RnWebSocket = WebSocket as unknown as new (
            url: string,
            protocols?: string | string[],
            options?: { headers: Record<string, string> },
          ) => WebSocket;
          return bearer
            ? new RnWebSocket(url, undefined, { headers: { Authorization: `Bearer ${bearer}` } })
            : new RnWebSocket(url);
        }
      : undefined);
  const client = createWSEClient({ auth: token !== undefined ? 'token' : undefined, ...rest, webSocket });

  let timer: ReturnType<typeof setTimeout> | null = null;
  const onState = (state: string) => {
    if (state === 'active') {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      client.resume(BACKGROUND);
    } else if (state === 'background' && !timer) {
      timer = setTimeout(() => {
        timer = null;
        client.pause(BACKGROUND);
      }, backgroundGraceMs);
    }
  };
  const subscription = appState?.addEventListener('change', onState) ?? null;
  if (appState?.currentState === 'background') client.pause(BACKGROUND);

  let following = subscription !== null;
  const stopFollowingAppState = () => {
    if (following) subscription?.remove();
    following = false;
    if (timer) clearTimeout(timer);
    timer = null;
  };
  return {
    ...client,
    stopFollowingAppState,
    // However the client ends, it no longer follows the app.
    close: async (deadlineMs: number) => {
      try {
        return await client.close(deadlineMs);
      } finally {
        stopFollowingAppState();
      }
    },
    signOut: () => {
      stopFollowingAppState();
      client.signOut();
    },
    destroy: () => {
      stopFollowingAppState();
      client.destroy();
    },
  };
}
