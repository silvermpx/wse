//! Where each topic stands: the last offset delivered of each of its
//! streams, for dropping duplicates and finding gaps.
//!
//! A topic published on several cluster nodes interleaves their streams,
//! one epoch each, each in order on its own. The topic's position is the
//! last message delivered, the point a recovery asks from.

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

/// A topic's streams, most recent last.
#[derive(Debug, Default)]
pub(crate) struct Streams {
    streams: Vec<Position>,
    position: Option<Position>,
}

impl Streams {
    /// The verdict on a message at `at`; delivered messages advance the
    /// stream.
    #[inline]
    pub(crate) fn check(&mut self, at: Position) -> Verdict {
        match self.streams.iter().rposition(|s| s.epoch == at.epoch) {
            None => {
                self.advance(at);
                Verdict::Deliver
            }
            Some(i) => {
                let last = self.streams[i].offset;
                if at.offset <= last {
                    Verdict::Duplicate
                } else if at.offset == last + 1 {
                    self.advance_at(i, at);
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
    pub(crate) fn advance(&mut self, at: Position) {
        match self.streams.iter().rposition(|s| s.epoch == at.epoch) {
            Some(i) => self.advance_at(i, at),
            None => {
                if self.streams.len() == MAX_STREAMS {
                    self.streams.remove(0);
                }
                self.streams.push(at);
                self.position = Some(at);
            }
        }
    }

    #[inline]
    fn advance_at(&mut self, i: usize, at: Position) {
        let last = self.streams.len() - 1;
        if i != last {
            self.streams[i..].rotate_left(1);
        }
        self.streams[last] = at;
        self.position = Some(at);
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
            .find(|s| s.epoch == epoch)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(epoch: u32, offset: u64) -> Position {
        Position { epoch, offset }
    }

    #[test]
    fn in_order_duplicates_and_gaps() {
        let mut s = Streams::default();
        assert_eq!(s.check(at(1, 10)), Verdict::Deliver);
        assert_eq!(s.check(at(1, 11)), Verdict::Deliver);
        assert_eq!(s.check(at(1, 11)), Verdict::Duplicate);
        assert_eq!(s.check(at(1, 5)), Verdict::Duplicate);
        assert_eq!(s.check(at(1, 13)), Verdict::Gap);
        // A gap does not move the position.
        assert_eq!(s.position(), Some(at(1, 11)));
        assert_eq!(s.check(at(1, 12)), Verdict::Deliver);
        assert_eq!(s.check(at(1, 13)), Verdict::Deliver);
    }

    #[test]
    fn streams_of_a_topic_are_in_order_each_on_its_own() {
        let mut s = Streams::default();
        assert_eq!(s.check(at(1, 10)), Verdict::Deliver);
        assert_eq!(s.check(at(2, 500)), Verdict::Deliver);
        assert_eq!(s.check(at(1, 11)), Verdict::Deliver);
        assert_eq!(s.position(), Some(at(1, 11)));
        assert_eq!(s.check(at(2, 501)), Verdict::Deliver);
        assert_eq!(s.check(at(2, 500)), Verdict::Duplicate);
        assert_eq!(s.stream(1), Some(at(1, 11)));
        assert_eq!(s.stream(3), None);
    }

    #[test]
    fn the_oldest_stream_goes_first() {
        let mut s = Streams::default();
        for epoch in 0..MAX_STREAMS as u32 {
            s.check(at(epoch, 1));
        }
        // Stream 0 is used again: stream 1 is now the oldest.
        assert_eq!(s.check(at(0, 2)), Verdict::Deliver);
        assert_eq!(s.check(at(1000, 1)), Verdict::Deliver);
        assert_eq!(s.stream(1), None);
        assert_eq!(s.stream(0), Some(at(0, 2)));
        // A forgotten stream starts over.
        assert_eq!(s.check(at(1, 1)), Verdict::Deliver);
    }
}
