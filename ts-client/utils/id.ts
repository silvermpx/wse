// =============================================================================
// WebSocket Engine - Message ID generation
// =============================================================================

/**
 * Return a random RFC 4122 version-4 UUID.
 *
 * `crypto.randomUUID()` exists only in secure contexts (https://, localhost).
 * A dashboard served over plain http:// on a LAN host has no randomUUID, so
 * every send used to throw. `crypto.getRandomValues()` is available in every
 * context, so it is the fallback; Math.random is the last resort for runtimes
 * with no Web Crypto at all (IDs are message identifiers, not secrets).
 */
export function generateId(): string {
  const c: Crypto | undefined =
    typeof globalThis !== 'undefined' ? (globalThis as { crypto?: Crypto }).crypto : undefined;

  if (c && typeof c.randomUUID === 'function') {
    try {
      return c.randomUUID();
    } catch {
      // Some embedded browsers expose randomUUID but throw outside secure contexts.
    }
  }

  const bytes = new Uint8Array(16);
  if (c && typeof c.getRandomValues === 'function') {
    c.getRandomValues(bytes);
  } else {
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = Math.floor(Math.random() * 256);
    }
  }

  // Version 4, variant 10xx.
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  let hex = '';
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, '0');
  }
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
