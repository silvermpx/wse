// =============================================================================
// WebSocket Engine - The page's localStorage
// =============================================================================
//
// A server runtime may have a localStorage of its own (Node 25 does): one for
// the whole process, where what one request kept the next would read. Outside
// a page there is none.

/** The page's localStorage, or null outside a page or where it may not be used. */
export function pageLocalStorage(): Storage | null {
  try {
    if (typeof window === 'undefined' || typeof document === 'undefined') return null;
    return typeof localStorage !== 'undefined' ? localStorage : null;
  } catch {
    return null;
  }
}
