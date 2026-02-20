// =============================================================================
// WebSocket Engine - Message Queue Store (Zustand)
// =============================================================================

import { create } from 'zustand';
import {
  MessageQueueState,
  QueuedMessage,
  MessagePriority,
  QueueStats
} from '../types';

interface MessageQueueStore extends MessageQueueState {
  // Actions
  enqueue: (message: QueuedMessage) => boolean;
  dequeue: (count?: number) => QueuedMessage[];
  removeMessage: (id: string) => void;
  clearQueue: () => void;
  updateStats: () => void;
  setProcessing: (processing: boolean) => void;
  setCapacity: (capacity: number) => void;

  // Priority management
  getByPriority: (priority: MessagePriority) => QueuedMessage[];
  removeOldest: (count: number) => void;

  // Computed
  hasBackpressure: () => boolean;
  getOldestMessageAge: () => number | null;
}

const initialState: MessageQueueState = {
  messages: [],
  size: 0,
  capacity: 10000,
  processing: false,
  stats: {
    size: 0,
    capacity: 10000,
    utilizationPercent: 0,
    oldestMessageAge: null,
    priorityDistribution: {},
    processingRate: 0,
    backpressure: false,
  },
};

// Track message processing rate
let processedCount = 0;
let lastProcessedTime = Date.now();

export const useMessageQueueStore = create<MessageQueueStore>((set, get) => ({
  ...initialState,

  enqueue: (message) => {
    const state = get();

    // Check capacity
    if (state.size >= state.capacity) {
      const sorted = [...state.messages].sort((a, b) => {
        if (a.priority !== b.priority) return a.priority - b.priority;
        return a.timestamp - b.timestamp;
      });

      if (sorted.length > 0 && sorted[0].priority < message.priority) {
        set((s) => ({
          messages: s.messages.filter(m => m.id !== sorted[0].id),
          size: s.size - 1,
        }));
      } else {
        return false;
      }
    }

    set((s) => {
      const messages = [...s.messages, message];
      const size = messages.length;

      return {
        messages,
        size,
        stats: {
          ...s.stats,
          size,
          utilizationPercent: (size / s.capacity) * 100,
          backpressure: size > s.capacity * 0.8,
        },
      };
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
        const size = remaining.length;

        return {
          messages: remaining,
          size,
          stats: {
            ...s.stats,
            size,
            utilizationPercent: (size / s.capacity) * 100,
            backpressure: size > s.capacity * 0.8,
            processingRate: Math.round(processingRate * 100) / 100,
          },
        };
      });
    }

    return dequeued;
  },

  removeMessage: (id) => {
    set((s) => {
      const messages = s.messages.filter(m => m.id !== id);
      const size = messages.length;

      return {
        messages,
        size,
        stats: {
          ...s.stats,
          size,
          utilizationPercent: (size / s.capacity) * 100,
          backpressure: size > s.capacity * 0.8,
        },
      };
    });
  },

  clearQueue: () => {
    processedCount = 0;
    lastProcessedTime = Date.now();

    set({
      messages: [],
      size: 0,
      stats: {
        ...initialState.stats,
      },
    });
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

  setProcessing: (processing) => set({ processing }),

  setCapacity: (capacity) => set((s) => ({
    capacity,
    stats: {
      ...s.stats,
      capacity,
      utilizationPercent: (s.size / capacity) * 100,
      backpressure: s.size > capacity * 0.8,
    },
  })),

  getByPriority: (priority) => {
    return get().messages.filter(m => m.priority === priority);
  },

  removeOldest: (count) => {
    set((s) => {
      const sorted = [...s.messages].sort((a, b) => a.timestamp - b.timestamp);
      const remaining = sorted.slice(count);
      const size = remaining.length;

      return {
        messages: remaining,
        size,
        stats: {
          ...s.stats,
          size,
          utilizationPercent: (size / s.capacity) * 100,
          backpressure: size > s.capacity * 0.8,
        },
      };
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
