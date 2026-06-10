import { describe, expect, it } from 'vitest';

import { EventSequencer } from './EventSequencer';

// Per-topic (epoch, offset) idempotent dedup + gap detection. Mirrors the
// Python client's TestTopicStamp suite for cross-client parity.
describe('EventSequencer.checkTopicStamp', () => {
  it('delivers and baselines a new topic', () => {
    const s = new EventSequencer();
    expect(s.checkTopicStamp('room', '0000000a', 5)).toBe('deliver');
    expect(s.getTopicPosition('room')).toEqual({ epoch: '0000000a', offset: 5 });
  });

  it('advances on in-order', () => {
    const s = new EventSequencer();
    s.checkTopicStamp('room', '0000000a', 5);
    expect(s.checkTopicStamp('room', '0000000a', 6)).toBe('deliver');
    expect(s.getTopicPosition('room')).toEqual({ epoch: '0000000a', offset: 6 });
  });

  it('drops duplicates (offset <= last on same epoch)', () => {
    const s = new EventSequencer();
    s.checkTopicStamp('room', '0000000a', 5);
    s.checkTopicStamp('room', '0000000a', 6);
    expect(s.checkTopicStamp('room', '0000000a', 6)).toBe('duplicate');
    expect(s.checkTopicStamp('room', '0000000a', 3)).toBe('duplicate');
    expect(s.getTopicPosition('room')).toEqual({ epoch: '0000000a', offset: 6 });
  });

  it('reports a gap without advancing (so recovery replays in order)', () => {
    const s = new EventSequencer();
    s.checkTopicStamp('room', '0000000a', 5);
    expect(s.checkTopicStamp('room', '0000000a', 9)).toBe('gap');
    expect(s.getTopicPosition('room')).toEqual({ epoch: '0000000a', offset: 5 });
    expect(s.checkTopicStamp('room', '0000000a', 6)).toBe('deliver');
  });

  it('rebaselines on an epoch change (server restart)', () => {
    const s = new EventSequencer();
    s.checkTopicStamp('room', '0000000a', 100);
    expect(s.checkTopicStamp('room', '0000000b', 0)).toBe('deliver');
    expect(s.getTopicPosition('room')).toEqual({ epoch: '0000000b', offset: 0 });
  });

  it('clears positions only on a full reset', () => {
    const s = new EventSequencer();
    s.checkTopicStamp('room', '0000000a', 5);
    s.reset();
    expect(s.getTopicPosition('room')).toBeUndefined();
  });
});
