// =============================================================================
// WebSocket Engine - window CustomEvent helper
// =============================================================================

/**
 * Dispatch a CustomEvent on `window` when one exists.
 *
 * The client also runs outside browsers (SSR, plain Node scripts, tests);
 * there `window` is undefined and an unguarded `window.dispatchEvent` threw a
 * ReferenceError from deep inside the connection code.
 */
export function dispatchWSEEvent<T>(name: string, detail: T): void {
  if (typeof window === 'undefined' || typeof window.dispatchEvent !== 'function') return;
  if (typeof CustomEvent === 'undefined') return;
  try {
    window.dispatchEvent(new CustomEvent<T>(name, { detail }));
  } catch {
    // Listener errors are reported by the browser; never let them break the client.
  }
}
