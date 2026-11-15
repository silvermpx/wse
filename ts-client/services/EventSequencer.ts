// =============================================================================
// WebSocket Engine - Event Sequencer
// =============================================================================

import { logger } from '../utils/logger.js';

/** Streams (origin epochs) remembered per topic: a cluster's nodes, plus restarts. */
const MAX_STREAMS_PER_TOPIC = 32;

/** Where a client stands in a topic: its last message, and its offset in each stream. */
export interface TopicPosition {
  epoch: string;
  offset: number;
  /** Epoch -> last offset delivered of that stream. */
  streams: Record<string, number>;
}

export class EventSequencer {
  private sequence = 0;
  private seenIds: Map<string, number>;
  // Per-topic last-seen recovery position. AUTHORITATIVE dedup/ordering for
  // stamped messages; survives reconnects within an epoch. Replaces the old
  // seq-based reorder buffer, which the server's global `seq` made unsound.
  private topicPositions = new Map<string, { epoch: string; offset: number }>();
  /**
   * Per topic, the last offset of each stream (origin epoch): a topic
   * published on several cluster nodes interleaves their streams, each in
   * order on its own. Most recently delivered last.
   */
  private streams = new Map<string, Map<string, number>>();
  /** When each stream last moved or became known (ms since the epoch). */
  private seen = new Map<string, Map<string, number>>();
  private readonly windowSize: number;

  private cleanupCounter = 0;
  private readonly CLEANUP_INTERVAL = 100;
  private readonly MAX_AGE_MS = 300000; // 5 minutes

  // No background timer: old ids are evicted every CLEANUP_INTERVAL inserts,
  // which bounds memory without keeping a timer alive for the page's life.
  constructor(windowSize = 10000) {
    this.windowSize = windowSize;
    this.seenIds = new Map();
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
   * Idempotent dedup + gap detection by (epoch, offset) within the topic's
   * stream of that epoch. Mirrors the Python client's check_topic_stamp.
   *   'duplicate' -- already delivered (offset <= last in its stream); drop.
   *   'gap'       -- offset jumped ahead in its stream. This does NOT advance
   *                  the position: the caller decides (MessageProcessor holds
   *                  the message while a requested replay may still fill the
   *                  hole, then delivers it, advances with setTopicPosition()
   *                  and emits a gap event -- a gap never blacks the topic out).
   *   'deliver'   -- next in order, or the first message of a stream not seen
   *                  yet (another node publishing on the topic, or a restart).
   */
  checkTopicStamp(topic: string, epoch: string, offset: number): 'duplicate' | 'gap' | 'deliver' {
    const last = this.streams.get(topic)?.get(epoch);
    if (last === undefined) {
      this.advance(topic, epoch, offset);
      return 'deliver';
    }
    if (offset <= last) {
      return 'duplicate';
    }
    if (offset === last + 1) {
      this.advance(topic, epoch, offset);
      return 'deliver';
    }
    return 'gap';
  }

  private advance(topic: string, epoch: string, offset: number): void {
    let streams = this.streams.get(topic);
    if (!streams) {
      streams = new Map();
      this.streams.set(topic, streams);
    }
    streams.delete(epoch); // most recent last
    streams.set(epoch, offset);
    if (streams.size > MAX_STREAMS_PER_TOPIC) {
      const oldest = streams.keys().next().value!;
      streams.delete(oldest);
      this.seen.get(topic)?.delete(oldest);
    }
    this.touch(topic, epoch);
    this.topicPositions.set(topic, { epoch, offset });
  }

  private touch(topic: string, epoch: string): void {
    let seen = this.seen.get(topic);
    if (!seen) {
      seen = new Map();
      this.seen.set(topic, seen);
    }
    seen.set(epoch, Date.now());
  }

  /**
   * Takes `offset` as where the stream of `epoch` stands, without a message
   * delivered (a server's reply lists its streams): a stream not known yet
   * starts there, as the oldest; a known one moves up to it. The last
   * message delivered stays the topic's anchor.
   */
  raiseStream(topic: string, epoch: string, offset: number): void {
    const streams = this.streams.get(topic);
    const own = streams?.get(epoch);
    if (own !== undefined) {
      if (offset > own) {
        streams!.set(epoch, offset);
        this.touch(topic, epoch);
      }
      return;
    }
    if (streams && streams.size >= MAX_STREAMS_PER_TOPIC) return;
    this.streams.set(topic, new Map([[epoch, offset], ...(streams ?? [])]));
    this.touch(topic, epoch);
    if (!this.topicPositions.has(topic)) this.topicPositions.set(topic, { epoch, offset });
  }

  /** Forgets the stream of `epoch`; the anchor moves to the last one left. */
  forgetStream(topic: string, epoch: string): void {
    const streams = this.streams.get(topic);
    if (!streams?.delete(epoch)) return;
    this.seen.get(topic)?.delete(epoch);
    if (this.topicPositions.get(topic)?.epoch === epoch) {
      const last = [...streams.entries()].pop();
      if (last) this.topicPositions.set(topic, { epoch: last[0], offset: last[1] });
      else this.topicPositions.delete(topic);
    }
  }

  /** Forgets the streams of `topic` that did not move since `before` (ms), but those `keep` keeps. */
  forgetQuietStreams(topic: string, before: number, keep: (epoch: string) => boolean): void {
    const seen = this.seen.get(topic);
    for (const epoch of [...(this.streams.get(topic)?.keys() ?? [])]) {
      if ((seen?.get(epoch) ?? 0) < before && !keep(epoch)) this.forgetStream(topic, epoch);
    }
  }

  /** Last delivered (epoch, offset) of a topic: the recovery anchor. */
  getTopicPosition(topic: string): { epoch: string; offset: number } | undefined {
    return this.topicPositions.get(topic);
  }

  /** Last delivered (epoch, offset) of the topic's stream of `epoch`. */
  getStreamPosition(topic: string, epoch: string): { epoch: string; offset: number } | undefined {
    const offset = this.streams.get(topic)?.get(epoch);
    return offset === undefined ? undefined : { epoch, offset };
  }

  /**
   * Where the client stands in each topic -- sent with a resubscribe: the
   * last delivered (epoch, offset), the anchor a server before 3.1 replays
   * from, and the last offset of each stream. Restricted to `topics` when
   * given.
   */
  getTopicPositions(topics?: string[]): Record<string, TopicPosition> {
    const out: Record<string, TopicPosition> = {};
    for (const topic of topics ?? [...this.topicPositions.keys()]) {
      const pos = this.topicPositions.get(topic);
      if (pos) out[topic] = { ...pos, streams: Object.fromEntries(this.streams.get(topic) ?? []) };
    }
    return out;
  }

  /** Forget positions of topics the client unsubscribed from. */
  clearTopics(topics: string[]): void {
    for (const topic of topics) {
      this.topicPositions.delete(topic);
      this.streams.delete(topic);
      this.seen.delete(topic);
    }
  }

  /**
   * Authoritative resync from a subscription ACK. Without it a
   * NotRecovered response left `topicPositions` at the stale offset:
   * every later stamped message read 'gap', was dropped, and the
   * re-subscribe loop asked for the same unrecoverable range forever —
   * the topic stayed black until the processor was destroyed. The
   * server's reported head is the
   * new truth; the snapshot request that follows the subscribe covers
   * the skipped window's state.
   */
  setTopicPosition(topic: string, epoch: string, offset: number): void {
    this.advance(topic, epoch, offset);
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
    this.seenIds.clear();
    this.topicPositions.clear();
    this.streams.clear();
    this.seen.clear();
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
    this.streams.clear();
    this.seen.clear();
    this.cleanupCounter = 0;

    logger.debug('EventSequencer reset');
  }
}
