// =============================================================================
// WebSocket Engine - WSE Store (Zustand)
// =============================================================================
//
// The page's connection state as a zustand hook: the state of the default
// client (useWSE), made from the same creator a core client's store is.

import { create } from 'zustand';
import { subscribeWithSelector } from 'zustand/middleware';
import { createWSEState, type WSEStore } from '../core/state/wseState.js';

export type { WSEStore, InboundRecoveryPosition } from '../core/state/wseState.js';

export const useWSEStore = create<WSEStore>()(subscribeWithSelector(createWSEState));

// Selectors for common use cases
export const selectConnectionState = (state: WSEStore) => state.connectionState;
export const selectIsConnected = (state: WSEStore) => state.isConnected();
export const selectMetrics = (state: WSEStore) => state.metrics;
export const selectActiveTopics = (state: WSEStore) => state.activeTopics;
export const selectLastError = (state: WSEStore) => state.lastError;
export const selectCircuitBreaker = (state: WSEStore) => state.circuitBreaker;
export const selectQueueStats = (state: WSEStore) => state.messageQueue;
export const selectDiagnostics = (state: WSEStore) => state.diagnostics;
export const selectCanReconnect = (state: WSEStore) => state.canReconnect();
export const selectConnectionQuality = (state: WSEStore) => state.getConnectionQuality();

// Cleanup function for unmounting
export const cleanupWSEStore = () => {
  useWSEStore.getState().clearTimers();
};

// Expose store globally for dynamic staleTime checks (development only)
if (typeof window !== 'undefined') {
  try {
    const isDev = typeof process !== 'undefined' && process.env?.NODE_ENV === 'development';
    if (isDev) {
      (window as any).__WSE_STORE__ = useWSEStore;
    }
  } catch {
    // Ignore errors in environment detection
  }
}
