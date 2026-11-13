// =============================================================================
// WebSocket Engine - Message Queue Store (Zustand)
// =============================================================================
//
// The in-memory outgoing queue. Two bounds apply: a message count (capacity)
// and an estimated size in bytes (memoryLimit). When either is reached, a new
// message may push out queued messages of strictly lower priority, lowest and
// oldest first; otherwise it is rejected. A burst of low-priority messages can
// therefore never displace more important ones.

import { create } from 'zustand';
import {
  MessageQueueState,
  QueuedMessage,
  MessagePriority,
  QueueStats
} from '../types.js';

interface MessageQueueStore extends MessageQueueState {
  // Actions
  enqueue: (message: QueuedMessage) => boolean;
  dequeue: (count?: number) => QueuedMessage[];
  /**
   * Put messages that were dequeued but could not be sent back into the
   * queue. They keep their priority and timestamp, so they come out first
   * again and the original order is preserved.
   */
  requeue: (messages: QueuedMessage[]) => void;
  removeMessage: (id: string) => void;
  clearQueue: () => void;
  updateStats: () => void;
  setProcessing: (processing: boolean) => void;
  setCapacity: (capacity: number) => void;
  /** Bound on the queued messages' estimated size in bytes; 0 means none. */
  setMemoryLimit: (bytes: number) => void;

  // Priority management
  getByPriority: (priority: MessagePriority) => QueuedMessage[];
  removeOldest: (count: number) => void;

  // Computed
  hasBackpressure: () => boolean;
  getOldestMessageAge: () => number | null;
}

const DEFAULT_CAPACITY = 10000;

const initialStats: QueueStats = {
  size: 0,
  capacity: DEFAULT_CAPACITY,
  utilizationPercent: 0,
  oldestMessageAge: null,
  priorityDistribution: {},
  processingRate: 0,
  backpressure: false,
  bytes: 0,
  memoryLimit: 0,
};

const initialState: MessageQueueState = {
  messages: [],
  size: 0,
  bytes: 0,
  capacity: DEFAULT_CAPACITY,
  memoryLimit: 0,
  processing: false,
  stats: initialStats,
};

// Track message processing rate
let processedCount = 0;
let lastProcessedTime = Date.now();

function bytesOf(messages: QueuedMessage[]): number {
  let total = 0;
  for (const m of messages) total += m.bytes ?? 0;
  return total;
}

/**
 * The fields that follow from the message list. Utilization is the fuller of
 * the two bounds, so backpressure shows whichever limit is closer.
 */
function derive(
  s: Pick<MessageQueueState, 'capacity' | 'memoryLimit' | 'stats'>,
  messages: QueuedMessage[],
): Pick<MessageQueueState, 'messages' | 'size' | 'bytes' | 'stats'> {
  const size = messages.length;
  const bytes = bytesOf(messages);
  const byCount = s.capacity > 0 ? size / s.capacity : 0;
  const byBytes = s.memoryLimit > 0 ? bytes / s.memoryLimit : 0;
  const utilization = Math.max(byCount, byBytes);
  return {
    messages,
    size,
    bytes,
    stats: {
      ...s.stats,
      size,
      bytes,
      capacity: s.capacity,
      memoryLimit: s.memoryLimit,
      utilizationPercent: utilization * 100,
      backpressure: utilization > 0.8,
    },
  };
}

/**
 * Messages to evict so `incoming` fits, or null when it cannot fit: only
 * messages of strictly lower priority are candidates, lowest and oldest first.
 */
function evictionsFor(state: MessageQueueState, incoming: QueuedMessage): Set<string> | null {
  const incomingBytes = incoming.bytes ?? 0;
  if (state.memoryLimit > 0 && incomingBytes > state.memoryLimit) return null;

  let size = state.size;
  let bytes = state.bytes;
  const fits = () =>
    size < state.capacity && (state.memoryLimit <= 0 || bytes + incomingBytes <= state.memoryLimit);
  if (fits()) return new Set();

  const candidates = state.messages
    .filter(m => m.priority < incoming.priority)
    .sort((a, b) => (a.priority - b.priority) || (a.timestamp - b.timestamp));
  const evicted = new Set<string>();
  for (const m of candidates) {
    evicted.add(m.id);
    size -= 1;
    bytes -= m.bytes ?? 0;
    if (fits()) return evicted;
  }
  return null;
}

export const useMessageQueueStore = create<MessageQueueStore>((set, get) => ({
  ...initialState,

  enqueue: (message) => {
    const evicted = evictionsFor(get(), message);
    if (evicted === null) return false;

    set((s) => {
      const kept = evicted.size > 0 ? s.messages.filter(m => !evicted.has(m.id)) : s.messages;
      return derive(s, [...kept, message]);
    });
    return true;
  },

  dequeue: (count = 1) => {
    const state = get();

    const sorted = [...state.messages].sort((a, b) => {
      if (b.priority !== a.priority) {
        return b.priority - a.priority;
      }
      return a.timestamp - b.timestamp;
    });

    const dequeued = sorted.slice(0, count);
    const remaining = sorted.slice(count);

    if (dequeued.length > 0) {
      const now = Date.now();
      const timeDiff = (now - lastProcessedTime) / 1000;
      processedCount += dequeued.length;

      const processingRate = timeDiff > 0 ? processedCount / timeDiff : 0;

      if (timeDiff > 60) {
        processedCount = 0;
        lastProcessedTime = now;
      }

      set((s) => {
        const next = derive(s, remaining);
        next.stats.processingRate = Math.round(processingRate * 100) / 100;
        return next;
      });
    }

    return dequeued;
  },

  requeue: (messages) => {
    if (messages.length === 0) return;
    set((s) => {
      const known = new Set(s.messages.map(m => m.id));
      const back = messages.filter(m => !known.has(m.id));
      return derive(s, [...back, ...s.messages]);
    });
  },

  removeMessage: (id) => {
    set((s) => derive(s, s.messages.filter(m => m.id !== id)));
  },

  clearQueue: () => {
    processedCount = 0;
    lastProcessedTime = Date.now();

    if (get().size === 0 && get().messages.length === 0) return;

    set((s) => ({
      messages: [],
      size: 0,
      bytes: 0,
      stats: {
        ...initialStats,
        capacity: s.capacity,
        memoryLimit: s.memoryLimit,
      },
    }));
  },

  updateStats: () => {
    const state = get();
    const now = Date.now();

    const oldestMessageAge = state.messages.length > 0
      ? now - Math.min(...state.messages.map(m => m.timestamp))
      : null;

    const priorityDistribution = state.messages.reduce((acc, msg) => {
      acc[msg.priority] = (acc[msg.priority] || 0) + 1;
      return acc;
    }, {} as Record<number, number>);

    set((s) => ({
      stats: {
        ...s.stats,
        oldestMessageAge,
        priorityDistribution,
        processingRate: s.processing ? s.stats.processingRate : 0,
      },
    }));
  },

  // Skip no-op writes: an idle flush loop used to toggle this twice per tick
  // and re-render every subscriber twice a second.
  setProcessing: (processing) => {
    if (get().processing === processing) return;
    set({ processing });
  },

  setCapacity: (capacity) => set((s) => derive({ ...s, capacity }, s.messages)),

  setMemoryLimit: (bytes) => set((s) => {
    const memoryLimit = Number.isFinite(bytes) && bytes > 0 ? bytes : 0;
    return { memoryLimit, ...derive({ ...s, memoryLimit }, s.messages) };
  }),

  getByPriority: (priority) => {
    return get().messages.filter(m => m.priority === priority);
  },

  removeOldest: (count) => {
    set((s) => {
      const sorted = [...s.messages].sort((a, b) => a.timestamp - b.timestamp);
      return derive(s, sorted.slice(count));
    });
  },

  hasBackpressure: () => get().stats.backpressure,

  getOldestMessageAge: () => {
    const messages = get().messages;
    if (messages.length === 0) return null;

    const oldest = Math.min(...messages.map(m => m.timestamp));
    return Date.now() - oldest;
  },
}));

// Selectors
export const selectQueueSize = (state: MessageQueueStore) => state.size;
export const selectQueueStats = (state: MessageQueueStore) => state.stats;
export const selectIsProcessing = (state: MessageQueueStore) => state.processing;
export const selectHasBackpressure = (state: MessageQueueStore) => state.hasBackpressure();
export const selectQueuedMessages = (state: MessageQueueStore) => state.messages;
export const selectQueueUtilization = (state: MessageQueueStore) => state.stats.utilizationPercent;
