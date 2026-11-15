// =============================================================================
// WebSocket Engine - The default client's context
// =============================================================================
//
// What useWSE() and the root package's classes run on: the page's zustand
// stores, the security manager, the owner number of ownership.ts, events on
// the window and the `wse_` keys of localStorage (as before 3.1).

import { useWSEStore } from '../stores/useWSEStore.js';
import { useMessageQueueStore } from '../stores/useMessageQueueStore.js';
import { securityManager } from '../utils/security.js';
import { dispatchWSEEvent } from '../utils/events.js';
import { ownerEpoch, nextOwnerEpoch } from '../services/ownership.js';
import { STORAGE_KEY_PREFIX, clearWSEStorage } from '../constants.js';
import { deleteDatabase, prefixedStorage, type ClientContext } from '../core/context.js';

/** The default client's offline queue. */
export const LEGACY_OFFLINE_DATABASE = 'wse_offline_queue';

let context: ClientContext | null = null;

export function legacyContext(): ClientContext {
  if (!context) {
    const storage = prefixedStorage(STORAGE_KEY_PREFIX, LEGACY_OFFLINE_DATABASE);
    context = {
      state: useWSEStore,
      queue: useMessageQueueStore,
      security: securityManager,
      owner: { current: ownerEpoch, next: nextOwnerEpoch },
      emit: dispatchWSEEvent,
      storage: {
        ...storage,
        // As before: every `wse_` key of localStorage and sessionStorage.
        clear: () => {
          clearWSEStorage();
          deleteDatabase(LEGACY_OFFLINE_DATABASE);
        },
      },
    };
  }
  return context;
}
