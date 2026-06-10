// =============================================================================
// WebSocket Engine - Event Sequencer
// =============================================================================

import { logger } from '../utils/logger';

export class EventSequencer {
  private sequence = 0;
  private seenIds: Map<string, number>;
  private expectedSequence = 0;
  private outOfOrderBuffer: Map<number, any>;
  // Per-topic last-seen recovery position. AUTHORITATIVE dedup/ordering for
  // stamped messages; survives reconnects within an epoch (the server's global
  // `seq` is not a per-topic order, so seq-based reordering is unsound).
  private topicPositions = new Map<string, { epoch: string; offset: number }>();
  private readonly windowSize: number;
  private readonly maxOutOfOrder: number;
  private cleanupInterval: NodeJS.Timeout | null = null;

  private cleanupCounter = 0;
  private readonly CLEANUP_INTERVAL = 100;
  private readonly MAX_AGE_MS = 300000; // 5 minutes

  constructor(windowSize = 10000, maxOutOfOrder = 100) {
    this.windowSize = windowSize;
    this.maxOutOfOrder = maxOutOfOrder;
    this.seenIds = new Map();
    this.outOfOrderBuffer = new Map();

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

  recordSequence(sequence: number): void {
    this.expectedSequence = Math.max(this.expectedSequence, sequence + 1);
  }

  processSequencedEvent(sequence: number, event: any): any[] | null {
    if (sequence === this.expectedSequence) {
      const eventsToDeliver = [event];
      this.expectedSequence = sequence + 1;

      while (this.outOfOrderBuffer.has(this.expectedSequence)) {
        const bufferedEvent = this.outOfOrderBuffer.get(this.expectedSequence);
        this.outOfOrderBuffer.delete(this.expectedSequence);
        eventsToDeliver.push(bufferedEvent);
        this.expectedSequence++;
      }

      return eventsToDeliver;
    }

    if (sequence > this.expectedSequence) {
      if (sequence - this.expectedSequence > this.maxOutOfOrder) {
        logger.warn(`Event sequence ${sequence} too far ahead of expected ${this.expectedSequence}`);
        this.expectedSequence = sequence + 1;
        this.outOfOrderBuffer.clear();
        return [event];
      }

      this.outOfOrderBuffer.set(sequence, {
        ...event,
        _bufferedAt: Date.now()
      });
      return null;
    }

    logger.debug(`Skipping old event with sequence ${sequence}, expected ${this.expectedSequence}`);
    return null;
  }

  getStats(): {
    currentSequence: number;
    expectedSequence: number;
    duplicateWindowSize: number;
    outOfOrderBufferSize: number;
    largestGap: number;
  } {
    let largestGap = 0;

    if (this.outOfOrderBuffer.size > 0) {
      const sequences = Array.from(this.outOfOrderBuffer.keys()).sort((a, b) => a - b);
      largestGap = sequences[0] - this.expectedSequence;
    }

    return {
      currentSequence: this.sequence,
      expectedSequence: this.expectedSequence,
      duplicateWindowSize: this.seenIds.size,
      outOfOrderBufferSize: this.outOfOrderBuffer.size,
      largestGap,
    };
  }

  private cleanup(): void {
    try {
      const now = Date.now();
      const maxAge = 5 * 60 * 1000;

      let removedBuffered = 0;
      for (const [seq, event] of this.outOfOrderBuffer) {
        const bufferedAt = event._bufferedAt || event.timestamp;
        if (bufferedAt && now - bufferedAt > maxAge) {
          this.outOfOrderBuffer.delete(seq);
          removedBuffered++;
        }
      }

      if (removedBuffered > 0) {
        logger.debug(`Removed ${removedBuffered} old buffered events`);
      }

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
    this.outOfOrderBuffer.clear();
    this.topicPositions.clear();
    this.sequence = 0;
    this.expectedSequence = 0;
    this.cleanupCounter = 0;

    logger.debug('EventSequencer destroyed');
  }

  reset(): void {
    // Full reset for a NEW session (not a transient reconnect, which must keep
    // topic positions so (epoch, offset) dedup spans the reconnect).
    this.sequence = 0;
    this.expectedSequence = 0;
    this.seenIds.clear();
    this.outOfOrderBuffer.clear();
    this.topicPositions.clear();
    this.cleanupCounter = 0;

    logger.debug('EventSequencer reset');
  }
}
