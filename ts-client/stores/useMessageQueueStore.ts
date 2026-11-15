// =============================================================================
// WebSocket Engine - Message Queue Store (Zustand)
// =============================================================================
//
// The page's outgoing queue as a zustand hook: the default client's (useWSE),
// made from the same creator a core client's queue is.

import { create } from 'zustand';
import { createQueueState, type MessageQueueStore } from '../core/state/queueState.js';

export type { MessageQueueStore } from '../core/state/queueState.js';

export const useMessageQueueStore = create<MessageQueueStore>()(createQueueState);

// Selectors
export const selectQueueSize = (state: MessageQueueStore) => state.size;
export const selectQueueStats = (state: MessageQueueStore) => state.stats;
export const selectIsProcessing = (state: MessageQueueStore) => state.processing;
export const selectHasBackpressure = (state: MessageQueueStore) => state.hasBackpressure();
export const selectQueuedMessages = (state: MessageQueueStore) => state.messages;
export const selectQueueUtilization = (state: MessageQueueStore) => state.stats.utilizationPercent;
