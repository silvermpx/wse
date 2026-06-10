// =============================================================================
// WebSocket Engine - Event Sequencer
// =============================================================================

import { logger } from '../utils/logger';

export class EventSequencer {
  private sequence = 0;
  private seenIds: Map<string, number>;
  // Per-topic last-seen recovery position. AUTHORITATIVE dedup/ordering for
  // stamped messages; survives reconnects within an epoch. Replaces the old
  // seq-based reorder buffer, which the server's global `seq` made unsound.
  private topicPositions = new Map<string, { epoch: string; offset: number }>();
  private readonly windowSize: number;
  private cleanupInterval: NodeJS.Timeout | null = null;

  private cleanupCounter = 0;
  private readonly CLEANUP_INTERVAL = 100;
  private readonly MAX_AGE_MS = 300000; // 5 minutes

  constructor(windowSize = 10000) {
    this.windowSize = windowSize;
    this.seenIds = new Map();

    if (typeof window !== 'undefined') {
      this.cleanupInterval = setInterval(() => {
        this.cleanup();
      }, 60000);
    }
  }

  getNextSequence(): number {
    return ++this.sequence;
  }

  getCurrentSequence(): number {
    return this.sequence;
  }

  isDuplicate(eventId: string): boolean {
    const now = Date.now();

    if (this.seenIds.has(eventId)) {
      return true;
    }

    this.seenIds.set(eventId, now);
    this.cleanupCounter++;

    if (this.cleanupCounter >= this.CLEANUP_INTERVAL) {
      this.cleanupCounter = 0;
      this.cleanupOldEntries();
    }

    return false;
  }

  private cleanupOldEntries(): void {
    const cutoff = Date.now() - this.MAX_AGE_MS;
    let removed = 0;

    for (const [id, timestamp] of this.seenIds) {
      if (timestamp < cutoff) {
        this.seenIds.delete(id);
        removed++;
      }
    }

    if (this.seenIds.size > this.windowSize * 1.5) {
      const excess = this.seenIds.size - this.windowSize;
      const entries = Array.from(this.seenIds.entries())
        .sort((a, b) => a[1] - b[1])
        .slice(0, excess);

      entries.forEach(([id]) => {
        this.seenIds.delete(id);
        removed++;
      });
    }

    if (removed > 0) {
      logger.debug(`Cleaned up ${removed} old event IDs`);
    }
  }

  /**
   * Idempotent dedup + gap detection by per-topic (epoch, offset). Mirrors the
   * Python client's check_topic_stamp.
   *   'duplicate' -- already delivered (offset <= last on same epoch); drop.
   *   'gap'       -- offset jumped ahead; deliver but recover the missed range
   *                  (this does NOT advance the position, so recovery from
   *                  `last` replays last+1..head in order without re-dedup).
   *   'deliver'   -- next in order, or first message on a new/changed epoch.
   */
  checkTopicStamp(topic: string, epoch: string, offset: number): 'duplicate' | 'gap' | 'deliver' {
    const prev = this.topicPositions.get(topic);
    if (!prev || prev.epoch !== epoch) {
      this.topicPositions.set(topic, { epoch, offset });
      return 'deliver';
    }
    const last = prev.offset;
    if (offset <= last) {
      return 'duplicate';
    }
    if (offset === last + 1) {
      this.topicPositions.set(topic, { epoch, offset });
      return 'deliver';
    }
    return 'gap';
  }

  getTopicPosition(topic: string): { epoch: string; offset: number } | undefined {
    return this.topicPositions.get(topic);
  }

  setTopicPosition(topic: string, epoch: string, offset: number): void {
    this.topicPositions.set(topic, { epoch, offset });
  }

  getStats(): {
    currentSequence: number;
    duplicateWindowSize: number;
    topicPositions: number;
  } {
    return {
      currentSequence: this.sequence,
      duplicateWindowSize: this.seenIds.size,
      topicPositions: this.topicPositions.size,
    };
  }

  private cleanup(): void {
    try {
      if (this.cleanupCounter > 0) {
        this.cleanupOldEntries();
      }
    } catch (error) {
      logger.error('Error in EventSequencer cleanup:', error);
    }
  }

  destroy(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }

    this.seenIds.clear();
    this.topicPositions.clear();
    this.sequence = 0;
    this.cleanupCounter = 0;

    logger.debug('EventSequencer destroyed');
  }

  reset(): void {
    // Full reset for a NEW session (not a transient reconnect, which must keep
    // topic positions so (epoch, offset) dedup spans the reconnect).
    this.sequence = 0;
    this.seenIds.clear();
    this.topicPositions.clear();
    this.cleanupCounter = 0;

    logger.debug('EventSequencer reset');
  }
}
