//! Where each topic stands: the last offset delivered of each of its
//! streams, for dropping duplicates and finding gaps.
//!
//! A topic published on several cluster nodes interleaves their streams,
//! one epoch each, each in order on its own. The topic's position is the
//! last message delivered, the point a recovery asks from.

use web_time::Instant;

use crate::message::Position;

/// Streams remembered per topic: a cluster's nodes, plus the epochs a
/// restart leaves behind.
pub(crate) const MAX_STREAMS: usize = 32;

/// What to do with a topic message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The next in its stream, or the first of a stream not seen yet.
    Deliver,
    /// Delivered already.
    Duplicate,
    /// Past the next one: messages of its stream are missing.
    Gap,
}

/// A stream of a topic: the last message delivered of it, and when it moved
/// (or became known) last.
#[derive(Clone, Copy, Debug)]
struct Stream {
    at: Position,
    seen: Instant,
}

/// A topic's streams, most recent last.
#[derive(Debug, Default)]
pub(crate) struct Streams {
    streams: Vec<Stream>,
    position: Option<Position>,
}

impl Streams {
    /// The verdict on a message at `at`; delivered messages advance the
    /// stream.
    #[inline]
    pub(crate) fn check(&mut self, at: Position, now: Instant) -> Verdict {
        match self.streams.iter().rposition(|s| s.at.epoch == at.epoch) {
            None => {
                self.advance(at, now);
                Verdict::Deliver
            }
            Some(i) => {
                let last = self.streams[i].at.offset;
                if at.offset <= last {
                    Verdict::Duplicate
                } else if at.offset == last + 1 {
                    self.advance_at(i, at, now);
                    Verdict::Deliver
                } else {
                    // Not advanced: a recovery from the last one replays the
                    // missing range, which must not read as duplicates.
                    Verdict::Gap
                }
            }
        }
    }

    /// Moves the stream of `at` to it (most recent last).
    pub(crate) fn advance(&mut self, at: Position, now: Instant) {
        match self.streams.iter().rposition(|s| s.at.epoch == at.epoch) {
            Some(i) => self.advance_at(i, at, now),
            None => {
                if self.streams.len() == MAX_STREAMS {
                    self.streams.remove(0);
                }
                self.streams.push(Stream { at, seen: now });
                self.position = Some(at);
            }
        }
    }

    #[inline]
    fn advance_at(&mut self, i: usize, at: Position, now: Instant) {
        let last = self.streams.len() - 1;
        if i != last {
            self.streams[i..].rotate_left(1);
        }
        self.streams[last] = Stream { at, seen: now };
        self.position = Some(at);
    }

    /// Takes `at` as where its stream stands, without a message delivered:
    /// a stream not known yet starts there (as the oldest), a known one
    /// moves up to it. The last message delivered stays the topic's
    /// position.
    pub(crate) fn raise(&mut self, at: Position, now: Instant) {
        match self.streams.iter_mut().find(|s| s.at.epoch == at.epoch) {
            Some(stream) => {
                if at.offset > stream.at.offset {
                    *stream = Stream { at, seen: now };
                }
            }
            None => {
                if self.streams.len() == MAX_STREAMS {
                    return;
                }
                self.streams.insert(0, Stream { at, seen: now });
                self.position.get_or_insert(at);
            }
        }
    }

    /// Forgets the stream of `epoch`.
    pub(crate) fn forget(&mut self, epoch: u32) {
        self.streams.retain(|s| s.at.epoch != epoch);
        if self.position.is_some_and(|p| p.epoch == epoch) {
            self.position = self.streams.last().map(|s| s.at);
        }
    }

    /// Forgets the streams that did not move since `before`, but those
    /// `keep` keeps.
    pub(crate) fn forget_quiet(&mut self, before: Instant, keep: impl Fn(u32) -> bool) {
        let quiet: Vec<u32> = self
            .streams
            .iter()
            .filter(|s| s.seen < before && !keep(s.at.epoch))
            .map(|s| s.at.epoch)
            .collect();
        for epoch in quiet {
            self.forget(epoch);
        }
    }

    /// The last message delivered: where a recovery asks from.
    pub(crate) fn position(&self) -> Option<Position> {
        self.position
    }

    /// The last message delivered of the stream of `epoch`.
    pub(crate) fn stream(&self, epoch: u32) -> Option<Position> {
        self.streams
            .iter()
            .rev()
            .find(|s| s.at.epoch == epoch)
            .map(|s| s.at)
    }

    /// Where each stream stands, oldest first.
    pub(crate) fn positions(&self) -> impl Iterator<Item = Position> + '_ {
        self.streams.iter().map(|s| s.at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(epoch: u32, offset: u64) -> Position {
        Position { epoch, offset }
    }

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn in_order_duplicates_and_gaps() {
        let mut s = Streams::default();
        assert_eq!(s.check(at(1, 10), now()), Verdict::Deliver);
        assert_eq!(s.check(at(1, 11), now()), Verdict::Deliver);
        assert_eq!(s.check(at(1, 11), now()), Verdict::Duplicate);
        assert_eq!(s.check(at(1, 5), now()), Verdict::Duplicate);
        assert_eq!(s.check(at(1, 13), now()), Verdict::Gap);
        // A gap does not move the position.
        assert_eq!(s.position(), Some(at(1, 11)));
        assert_eq!(s.check(at(1, 12), now()), Verdict::Deliver);
        assert_eq!(s.check(at(1, 13), now()), Verdict::Deliver);
    }

    #[test]
    fn streams_of_a_topic_are_in_order_each_on_its_own() {
        let mut s = Streams::default();
        assert_eq!(s.check(at(1, 10), now()), Verdict::Deliver);
        assert_eq!(s.check(at(2, 500), now()), Verdict::Deliver);
        assert_eq!(s.check(at(1, 11), now()), Verdict::Deliver);
        assert_eq!(s.position(), Some(at(1, 11)));
        assert_eq!(s.check(at(2, 501), now()), Verdict::Deliver);
        assert_eq!(s.check(at(2, 500), now()), Verdict::Duplicate);
        assert_eq!(s.stream(1), Some(at(1, 11)));
        assert_eq!(s.stream(3), None);
    }

    #[test]
    fn the_oldest_stream_goes_first() {
        let mut s = Streams::default();
        for epoch in 0..MAX_STREAMS as u32 {
            s.check(at(epoch, 1), now());
        }
        // Stream 0 is used again: stream 1 is now the oldest.
        assert_eq!(s.check(at(0, 2), now()), Verdict::Deliver);
        assert_eq!(s.check(at(1000, 1), now()), Verdict::Deliver);
        assert_eq!(s.stream(1), None);
        assert_eq!(s.stream(0), Some(at(0, 2)));
        // A forgotten stream starts over.
        assert_eq!(s.check(at(1, 1), now()), Verdict::Deliver);
    }

    #[test]
    fn a_stream_raised_starts_or_moves_without_taking_the_position() {
        let mut s = Streams::default();
        s.raise(at(7, 4), now());
        assert_eq!(s.stream(7), Some(at(7, 4)));
        assert_eq!(s.check(at(7, 4), now()), Verdict::Duplicate);
        assert_eq!(s.check(at(1, 10), now()), Verdict::Deliver);
        s.raise(at(1, 12), now());
        s.raise(at(7, 2), now());
        assert_eq!(s.stream(1), Some(at(1, 12)));
        assert_eq!(s.stream(7), Some(at(7, 4)));
        assert_eq!(s.position(), Some(at(1, 10)));
        let mut positions: Vec<_> = s.positions().collect();
        positions.sort_unstable_by_key(|p| p.epoch);
        assert_eq!(positions, [at(1, 12), at(7, 4)]);
    }

    #[test]
    fn a_stream_forgotten_takes_the_position_with_it() {
        let mut s = Streams::default();
        let start = now();
        s.check(at(1, 1), start);
        s.check(at(2, 1), start + std::time::Duration::from_secs(10));
        s.forget(2);
        assert_eq!(s.position(), Some(at(1, 1)));
        assert_eq!(s.stream(2), None);
        s.check(at(2, 5), start + std::time::Duration::from_secs(10));
        // Quiet since before the cut: forgotten, but what is kept.
        s.forget_quiet(start + std::time::Duration::from_secs(5), |epoch| {
            epoch == 3
        });
        assert_eq!(s.stream(1), None);
        assert_eq!(s.stream(2), Some(at(2, 5)));
    }
}
