// =============================================================================
// WebSocket Engine - The WebCrypto the client uses
// =============================================================================
//
// The environment's own (browsers, Node 19+, Deno, Bun), unless one is set:
// React Native has none, and passes react-native-quick-crypto's. One for the
// whole process, as the environment's is.

let provided: Crypto | null = null;

/** Use `crypto` (an object with `subtle` and `getRandomValues`) instead of the environment's. */
export function setWebCrypto(crypto: Crypto | null): void {
  provided = crypto;
}

/** The WebCrypto set, or the environment's; null when there is none. */
export function webCryptoOrNull(): Crypto | null {
  if (provided) return provided;
  const own = typeof globalThis !== 'undefined' ? (globalThis as { crypto?: Crypto }).crypto : undefined;
  return own ?? null;
}

/** The WebCrypto set, or the environment's; throws when there is none. */
export function webCrypto(): Crypto {
  const c = webCryptoOrNull();
  if (!c?.subtle) throw new Error('WebCrypto is not available (HTTPS or localhost only, or set one with setWebCrypto)');
  return c;
}
