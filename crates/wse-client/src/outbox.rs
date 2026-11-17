//! The client's outbox: every message it sends, except a live or signed
//! one, waits in it until the server answers for it, and goes again after a
//! reconnect.
//!
//! A server that answers (`acks`, from 3.2) confirms what its application
//! took (`ack`, up to and with a number) and says what it refused (`nack`).
//! The messages go out in order, numbered (`s`, first in the object) per
//! producer, one producer per outbox, and the server drops a repeat, so the
//! application takes each message once. A server that does not answer takes
//! a message once it is written, as before.
//!
//! The connection's task owns the outbox: the send path takes no lock, and
//! a message's text is shared with the writer, not copied. Only its room is
//! shared with the client's handles, so that a send is refused at once when
//! it is full.
//!
//! In a file, the outbox outlives the application. The file is a log of
//! lines, each `<crc32 of the salt and the JSON, 8 hex digits> <JSON>`: the
//! first line names the file's salt, producer and next number, then come a
//! message kept (its text as it is), the highest number written, an `ack`,
//! messages dropped, the last user and node. It is read up to the first line
//! that does not check (a tail cut short as the application ended), and
//! written anew under a new salt when it is opened and once what went
//! outweighs what is kept. A thread of its own writes it, the lines of a
//! batch at once and to the disk right after, so the connection's task never
//! waits for the disk.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytestring::ByteString;
use serde::Serialize;
use web_time::Instant;

/// How many messages wait at most, by default: the bytes bound memory, and
/// a count too small would hold a busy connection back, the server's answers
/// taking a while to come back.
const MAX_MESSAGES: usize = 100_000;

/// How many bytes of messages wait at most, by default.
const MAX_BYTES: usize = 16 * 1024 * 1024;

/// How long a message waits to be sent, by default.
const MAX_AGE: Duration = Duration::from_secs(3600);

/// The client's outbox ([`Config::offline`](crate::Config::offline)): where
/// the messages wait until the server answers for them, and how many.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Offline {
    /// The file they wait in, so that they outlive the application (outside
    /// the browser). It is this client's alone while it runs; a lock file
    /// beside it (`<path>.lock`) says so. None keeps them in memory, for the
    /// client's life.
    #[cfg(not(target_arch = "wasm32"))]
    pub path: Option<std::path::PathBuf>,
    /// The IndexedDB database they wait in, so that they outlive the page
    /// (in the browser): each tab keeps a producer of its own there, held
    /// by a Web Lock, and a page that opens takes over what a closed tab
    /// left. None keeps them in memory, for the client's life.
    #[cfg(target_arch = "wasm32")]
    pub database: Option<String>,
    /// How many wait at most (100,000), those written and not answered yet
    /// included: past it [`Client::send`](crate::Client::send) waits for
    /// room, and [`Client::try_send`](crate::Client::try_send) fails with
    /// [`Error::Full`](crate::Error::Full).
    pub max_messages: usize,
    /// How many bytes of messages wait at most (16 MiB), as
    /// `max_messages`. A message larger than this fails at once with
    /// [`Error::Full`](crate::Error::Full).
    pub max_bytes: usize,
    /// How long a message waits to be sent (an hour), unless it says
    /// otherwise: one not sent by then is dropped and the application told.
    /// Once written it waits for the server's answer whatever its age.
    pub max_age: Duration,
}

impl Offline {
    /// Kept in memory, for the client's life.
    pub fn in_memory() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            path: None,
            #[cfg(target_arch = "wasm32")]
            database: None,
            max_messages: MAX_MESSAGES,
            max_bytes: MAX_BYTES,
            max_age: MAX_AGE,
        }
    }

    /// Kept in the file at `path`, past the application's end.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn in_file(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
            ..Self::in_memory()
        }
    }

    /// Kept in the IndexedDB database `name`, past the page's end (in the
    /// browser). Without IndexedDB or Web Locks (a page without HTTPS) the
    /// outbox stays in memory, and the client says so.
    #[cfg(target_arch = "wasm32")]
    pub fn in_indexed_db(name: impl Into<String>) -> Self {
        Self {
            database: Some(name.into()),
            ..Self::in_memory()
        }
    }
}

impl Default for Offline {
    fn default() -> Self {
        Self::in_memory()
    }
}

/// Room the client leaves before a kept message's JSON for its number:
/// `{"s":` and at most 20 digits. The number is written in it, and the text
/// never copied for it.
pub(crate) const HEADROOM: usize = 25;

/// Says what failed in the file: the operation and why.
pub(crate) type Report = Arc<dyn Fn(&'static str, String) + Send + Sync>;

/// The outbox's room, shared with the client's handles: a send takes its
/// place before it is queued, so a full outbox refuses it at once, or the
/// send waits until a place is given back.
pub(crate) struct Room {
    count: AtomicUsize,
    bytes: AtomicUsize,
    max_messages: usize,
    max_bytes: usize,
    /// Sends that wait for a place, woken when one is given back.
    waiting: AtomicUsize,
    freed: tokio::sync::Notify,
    /// The client ended: no send waits any longer.
    closed: std::sync::atomic::AtomicBool,
}

impl Room {
    pub(crate) fn new(offline: &Offline) -> Self {
        Self {
            count: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            max_messages: offline.max_messages.max(1),
            max_bytes: offline.max_bytes.max(1),
            waiting: AtomicUsize::new(0),
            freed: tokio::sync::Notify::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Waits for a place for a message of `bytes`. Fails with
    /// [`Error::Full`](crate::Error::Full) for one larger than the whole
    /// room, and [`Error::Closed`](crate::Error::Closed) once the client
    /// ended.
    pub(crate) async fn wait_take(&self, bytes: usize) -> Result<(), crate::Error> {
        if bytes > self.max_bytes {
            return Err(crate::Error::Full);
        }
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(crate::Error::Closed);
            }
            if self.take(bytes) {
                return Ok(());
            }
            let freed = self.freed.notified();
            tokio::pin!(freed);
            // Woken by any place given back from here on.
            freed.as_mut().enable();
            self.waiting.fetch_add(1, Ordering::AcqRel);
            let taken = !self.closed.load(Ordering::Acquire) && self.take(bytes);
            if !taken {
                freed.await;
            }
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            if taken {
                return Ok(());
            }
        }
    }

    /// The client ended: the sends that wait for a place stop waiting.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.freed.notify_waiters();
    }

    /// A place for a message of `bytes`; false when there is none.
    pub(crate) fn take(&self, bytes: usize) -> bool {
        if self.count.fetch_add(1, Ordering::AcqRel) >= self.max_messages {
            self.count.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        if self.bytes.fetch_add(bytes, Ordering::AcqRel) + bytes > self.max_bytes {
            self.bytes.fetch_sub(bytes, Ordering::AcqRel);
            self.count.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        true
    }

    /// A place whatever the limits: a message read from the file, which is
    /// not dropped because the limits shrank.
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    fn force(&self, bytes: usize) {
        self.count.fetch_add(1, Ordering::AcqRel);
        self.bytes.fetch_add(bytes, Ordering::AcqRel);
    }

    /// A message of `bytes` left: a send that waits takes its place.
    pub(crate) fn give_back(&self, bytes: usize) {
        self.give_back_many(1, bytes);
    }

    /// `count` messages of `bytes` in all left, given back at once: the
    /// counters are shared with the sending threads.
    pub(crate) fn give_back_many(&self, count: usize, bytes: usize) {
        if count == 0 {
            return;
        }
        self.count.fetch_sub(count, Ordering::AcqRel);
        self.bytes.fetch_sub(bytes, Ordering::AcqRel);
        if self.waiting.load(Ordering::Acquire) > 0 {
            self.freed.notify_waiters();
        }
    }

    /// How many wait.
    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

/// A message kept.
#[derive(Clone, Debug)]
struct Record {
    s: u64,
    id: Box<str>,
    /// When it was sent, in milliseconds since the Unix epoch.
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    at: u64,
    /// Until when it may wait to be written first; once written, it waits
    /// for the answer whatever its age.
    until: u64,
    /// Sent after a connection of the last user: it is that user's, and goes
    /// nowhere once the server takes a connection for another. False: sent
    /// before any connection, or after one without a user; it goes on any.
    owned: bool,
    /// Topics each continuous right before it goes (`requires`), every time
    /// it goes: once more after a reconnect too.
    requires: Option<Box<[String]>>,
    /// The message as it goes, numbered: `{"s":N,` before the rest. Shared
    /// with the writer, not copied.
    text: ByteString,
    /// Its place in the room: the size of the message as it was sent.
    size: usize,
    /// A bridge owner's window that sent it ([`Tags`]), in memory alone: 0
    /// for the application's own.
    tag: u64,
}

/// A bridge owner of this crate: told what became of its windows' sends,
/// each tagged with its window, and asked whether a window holds one back.
pub(crate) trait Tags: Send + Sync {
    /// What became of tagged messages, in the order it came.
    fn outcomes(&self, outcomes: Vec<Tagged>);
    /// A message of window `tag` that requires `topics` waits while this
    /// says one of them is not continuous for it.
    fn holds_back(&self, tag: u64, topics: &[String]) -> bool;
}

/// What became of a tagged message: delivered (`reason` None), or not and why.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Tagged {
    pub(crate) tag: u64,
    pub(crate) id: String,
    pub(crate) reason: Option<String>,
}

/// What became of messages: delivered, or not and why.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Outcomes {
    pub(crate) delivered: Vec<String>,
    /// Not delivered: the reason, and the ids.
    pub(crate) not_delivered: Vec<(String, Vec<String>)>,
    /// Those of tagged messages, in the order they came: theirs alone, the
    /// application's events do not name them.
    pub(crate) tagged: Vec<Tagged>,
}

impl Outcomes {
    fn merge(&mut self, other: Self) {
        self.delivered.extend(other.delivered);
        for (why, ids) in other.not_delivered {
            match self.not_delivered.iter_mut().find(|(w, _)| *w == why) {
                Some((_, mine)) => mine.extend(ids),
                None => self.not_delivered.push((why, ids)),
            }
        }
        self.tagged.extend(other.tagged);
    }

    /// `id` (of window `tag`, or the application's: 0) was delivered.
    pub(crate) fn deliver(&mut self, id: Box<str>, tag: u64) {
        if tag == 0 {
            self.delivered.push(id.into());
        } else {
            self.tagged.push(Tagged {
                tag,
                id: id.into(),
                reason: None,
            });
        }
    }

    /// `id` (of window `tag`, or the application's: 0) was not delivered,
    /// for `why`.
    pub(crate) fn refuse(&mut self, why: &str, id: Box<str>, tag: u64) {
        if tag != 0 {
            self.tagged.push(Tagged {
                tag,
                id: id.into(),
                reason: Some(why.to_owned()),
            });
            return;
        }
        match self.not_delivered.iter_mut().find(|(w, _)| w == why) {
            Some((_, ids)) => ids.push(id.into()),
            None => self.not_delivered.push((why.to_owned(), vec![id.into()])),
        }
    }
}

/// The outbox, owned by the connection's task.
pub(crate) struct Outbox {
    records: VecDeque<Record>,
    /// The next message's number: above every number used before.
    next: u64,
    /// Records before this index went to the connection's writer.
    cursor: usize,
    producer: Box<str>,
    /// The node the client was connected to last, as it named itself.
    node: Option<Box<str>>,
    /// The highest number handed to a connection: one above it never
    /// reached a server.
    written: u64,
    /// The highest number written to the current connection's socket.
    flushed: u64,
    /// The user of the last connection, once there was one.
    user: Option<Option<String>>,
    /// The server took the current connection.
    ready: bool,
    /// The server answered the hello on the current connection: messages
    /// go numbered and wait for its `ack`.
    answered: bool,
    /// The server of the current connection does not answer: a message
    /// written is gone.
    plain: bool,
    /// Written on the current connection and cleared before the server
    /// answered for them: its answer on it says what became of them, and
    /// they are not delivered when it ends first.
    limbo: VecDeque<(u64, Box<str>, u64)>,
    /// When the first message not written yet runs out (ms since the
    /// epoch), u64::MAX for none, and when that is for the connection's
    /// timer.
    earliest: u64,
    deadline: Option<Instant>,
    max_age_ms: u64,
    room: Arc<Room>,
    /// Where what waits outlives the client: a file, or IndexedDB in the
    /// browser.
    log: Option<Log>,
    /// The IndexedDB database to open before the first connection.
    #[cfg(target_arch = "wasm32")]
    database: Option<String>,
    /// A write of the store failed: it is written anew, at most once a
    /// second (`heal_at`, ms since the epoch), and when the outbox goes.
    owed: bool,
    heal_at: u64,
    /// When a rewrite owed within that second can go: the driver wakes for
    /// it ([`Outbox::deadline`]), whether or not anything changes meanwhile.
    heal: Option<Instant>,
    report: Report,
}

#[cfg(not(target_arch = "wasm32"))]
use file::Log;
#[cfg(target_arch = "wasm32")]
use idb::Log;

#[cfg(target_arch = "wasm32")]
mod idb;

impl Drop for Outbox {
    fn drop(&mut self) {
        if self.owed || self.log.as_ref().is_some_and(Log::take_broken) {
            self.rewrite();
        } else if let Some(log) = &mut self.log {
            log.flush();
        }
    }
}

/// What a store held when it was opened.
struct Read {
    producer: Box<str>,
    node: Option<Box<str>>,
    written: u64,
    /// Above every number it names.
    next: u64,
    user: Option<String>,
    records: VecDeque<Record>,
}

/// Lines of the file but a message kept, as written: borrowed, nothing
/// copied.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum LineRef<'a> {
    Written(u64),
    Ack(u64),
    Drop(&'a [u64]),
    User(Option<&'a str>),
    Node(&'a str),
}

/// The first line of a file, checked without a salt.
#[derive(Serialize, serde::Deserialize)]
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
struct Head<'a> {
    wse_outbox: u32,
    #[serde(borrow)]
    salt: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    producer: std::borrow::Cow<'a, str>,
    next: u64,
}

impl Outbox {
    /// The outbox `offline` names, with `room` shared with the client's
    /// handles. A file that cannot be opened is said (`report`) and the
    /// outbox kept in memory.
    pub(crate) fn open(offline: &Offline, room: Arc<Room>, report: Report) -> Self {
        let max_age_ms = u64::try_from(offline.max_age.as_millis()).unwrap_or(u64::MAX);
        #[allow(unused_mut)]
        let mut outbox = Self {
            records: VecDeque::new(),
            next: 1,
            cursor: 0,
            producer: new_id().into(),
            node: None,
            written: 0,
            flushed: 0,
            user: None,
            ready: false,
            answered: false,
            plain: false,
            limbo: VecDeque::new(),
            earliest: u64::MAX,
            deadline: None,
            max_age_ms,
            room,
            log: None,
            #[cfg(target_arch = "wasm32")]
            database: offline.database.clone(),
            owed: false,
            heal_at: 0,
            heal: None,
            report,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(path) = &offline.path {
            match file::Log::open(path, Arc::clone(&outbox.report)) {
                Ok((log, read)) => {
                    if let Some(read) = read {
                        outbox.restore(read);
                    }
                    outbox.log = Some(log);
                    outbox.rewrite();
                }
                Err(why) => (outbox.report)("open", why),
            }
        }
        outbox
    }

    /// Opens the IndexedDB database the outbox is kept in, before the first
    /// connection (it opens in its own time): takes over a producer it holds
    /// that no tab holds, else makes one, and takes up what it holds. One
    /// that cannot be opened is said, and the outbox kept in memory.
    #[cfg(target_arch = "wasm32")]
    pub(crate) async fn open_store(&mut self) {
        let Some(name) = self.database.take() else {
            return;
        };
        match idb::open(&name, Arc::clone(&self.report)).await {
            Ok((log, read)) => {
                self.restore(read);
                self.log = Some(log);
            }
            Err(why) => (self.report)("open", why),
        }
    }

    /// Takes up what the store held.
    fn restore(&mut self, read: Read) {
        self.producer = read.producer;
        self.node = read.node;
        self.written = read.written;
        self.user = read.user.map(Some);
        self.next = read.next;
        for record in read.records {
            self.room.force(record.size);
            self.earliest = self.earliest.min(record.until);
            self.records.push_back(record);
        }
        self.set_deadline(now_ms());
    }

    /// What the client's hello says of it: the producer, the node it was
    /// on, the highest number written, and the first it waits to hear of
    /// (the server forgets the refusals before it).
    pub(crate) fn hello(&self) -> serde_json::Value {
        serde_json::json!({
            "producer": &*self.producer,
            "node": self.node.as_deref(),
            "written": self.written,
            "from": self.records.front().map_or(self.next, |r| r.s),
        })
    }

    /// How many messages wait.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    /// When the driver is to wake for the outbox: the first message not
    /// written yet runs out, or a rewrite owed can go.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        match (self.deadline, self.heal) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// A rewrite owed whose time came goes.
    pub(crate) fn heal(&mut self, now: Instant) {
        if self.heal.is_some_and(|at| at <= now) {
            self.heal = None;
            self.flush();
        }
    }

    /// Keeps a message the application sent, `text` a JSON object after
    /// [`HEADROOM`] spaces, its place of `size` bytes in the room taken
    /// already: its number. It goes at the next [`Outbox::due`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add(
        &mut self,
        id: Box<str>,
        text: String,
        size: usize,
        ttl: Option<Duration>,
        now: u64,
        requires: Option<Box<[String]>>,
        tag: u64,
    ) -> u64 {
        let s = self.next;
        self.next += 1;
        let wait = ttl.map_or(self.max_age_ms, |ttl| {
            u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
        });
        let record = Record {
            s,
            id,
            at: now,
            until: now.saturating_add(wait),
            owned: matches!(self.user, Some(Some(_))),
            requires,
            text: number_in_place(s, text),
            size,
            tag,
        };
        if record.until < self.earliest {
            self.earliest = record.until;
            self.set_deadline(now);
        }
        if let Some(log) = &mut self.log {
            log.push_add(&record);
        }
        self.records.push_back(record);
        self.changed();
        s
    }

    /// The server took a connection of `user`: what was kept for another
    /// user goes, and what waited too long to be sent.
    pub(crate) fn connected(&mut self, user: Option<&str>, now: u64) -> Outcomes {
        let mut outcomes = Outcomes::default();
        self.ready = true;
        if let Some(last) = &self.user
            && last.as_deref() != user
        {
            // What was sent before any connection goes on any, until a
            // connection wrote it: then it was that connection's user's (the
            // server may have taken it as theirs), and it never goes as
            // another's.
            let written = self.written;
            self.remove_where("user_changed", &mut outcomes, |_, r| {
                r.owned || r.s <= written
            });
            self.user = Some(user.map(str::to_owned));
            self.put(&LineRef::User(user));
        } else if self.user.is_none() {
            self.user = Some(user.map(str::to_owned));
            self.put(&LineRef::User(user));
        }
        if self.earliest <= now {
            self.expire(now, &mut outcomes);
        }
        self.changed();
        outcomes
    }

    /// The server of this connection answers numbered messages (`acks`),
    /// or it does not.
    pub(crate) fn server_answers(&mut self, answers: bool) {
        self.plain = !answers;
    }

    /// The server's answer to the hello: what it decided, the refusals
    /// among that (first, last, why), and its name. What it decided is
    /// delivered or not, and the rest goes again, in order. It may come
    /// before the server took the connection or after.
    pub(crate) fn answered(
        &mut self,
        acked: u64,
        refused: &[(u64, u64, &str)],
        node: &str,
    ) -> Outcomes {
        let mut outcomes = Outcomes::default();
        for (from, to, why) in refused {
            outcomes.merge(self.refused(*from, *to, why));
        }
        outcomes.merge(self.acked(acked));
        self.answered = true;
        if self.node.as_deref() != Some(node) {
            self.node = Some(node.into());
            self.put(&LineRef::Node(node));
        }
        self.changed();
        outcomes
    }

    /// The server took every message up to and with `s`.
    pub(crate) fn acked(&mut self, s: u64) -> Outcomes {
        let mut outcomes = Outcomes::default();
        let (gone, bytes) = self.take_front(s, &mut outcomes);
        self.room.give_back_many(gone, bytes);
        self.cursor = self.cursor.saturating_sub(gone);
        while self.limbo.front().is_some_and(|(n, _, _)| *n <= s) {
            if let Some((_, id, tag)) = self.limbo.pop_front() {
                outcomes.deliver(id, tag);
            }
        }
        if gone > 0 {
            self.put(&LineRef::Ack(s));
            self.changed();
        }
        outcomes
    }

    /// The server refused the messages `from` to `to` for good, for `why`.
    pub(crate) fn refused(&mut self, from: u64, to: u64, why: &str) -> Outcomes {
        let mut outcomes = Outcomes::default();
        self.remove_where(why, &mut outcomes, |_, r| (from..=to).contains(&r.s));
        self.limbo.retain_mut(|(n, id, tag)| {
            let keep = !(from..=to).contains(n);
            if !keep {
                outcomes.refuse(why, std::mem::take(id), *tag);
            }
            keep
        });
        self.changed();
        outcomes
    }

    /// Hands the connection's writer (`out`) the messages it did not take
    /// yet, in order, each with its number, once the server took the
    /// connection and its mode is known; then the file gets the batch's
    /// lines. What waited too long to be sent goes instead.
    pub(crate) fn due(
        &mut self,
        now: u64,
        outcomes: &mut Outcomes,
        continuous: impl Fn(&[String], u64) -> bool,
        mut out: impl FnMut(u64, ByteString),
    ) {
        if self.ready && (self.answered || self.plain) {
            if self.earliest <= now {
                self.expire(now, outcomes);
            }
            if self.cursor < self.records.len() {
                let numbered = self.answered;
                let mut last = 0;
                for record in self.records.range(self.cursor..) {
                    // Its topics not continuous on this connection (a
                    // reconnect's resync not done): it waits in its place,
                    // and what comes after it with it.
                    if record
                        .requires
                        .as_deref()
                        .is_some_and(|t| !continuous(t, record.tag))
                    {
                        break;
                    }
                    let text = if numbered {
                        record.text.clone()
                    } else {
                        unnumbered(&record.text).into()
                    };
                    out(record.s, text);
                    last = record.s;
                    self.cursor += 1;
                }
                if numbered && last > self.written {
                    self.written = last;
                    self.put(&LineRef::Written(last));
                }
            }
        }
        self.flush();
    }

    /// The connection wrote the messages up to and with `last` to its
    /// socket. A server that does not answer took them.
    pub(crate) fn wrote(&mut self, last: u64) -> Outcomes {
        let mut outcomes = Outcomes::default();
        self.flushed = self.flushed.max(last);
        if self.plain && !self.answered {
            let (gone, bytes) = self.take_front(last, &mut outcomes);
            self.room.give_back_many(gone, bytes);
            self.cursor = self.cursor.saturating_sub(gone);
            if gone > 0 {
                self.put(&LineRef::Ack(last));
                self.changed();
            }
        }
        outcomes
    }

    /// Takes the records up to and with `s` off the front, delivered: how
    /// many, and their bytes.
    fn take_front(&mut self, s: u64, outcomes: &mut Outcomes) -> (usize, usize) {
        let (mut gone, mut bytes) = (0, 0);
        while self.records.front().is_some_and(|r| r.s <= s) {
            if let Some(record) = self.records.pop_front() {
                bytes += record.size;
                outcomes.deliver(record.id, record.tag);
                gone += 1;
            }
        }
        (gone, bytes)
    }

    /// The server owes an answer on this connection: for messages it has
    /// in hand, or to the hello, while messages wait for it to go.
    pub(crate) fn owed(&self) -> bool {
        if self.answered {
            !self.limbo.is_empty() || self.records.front().is_some_and(|r| r.s <= self.flushed)
        } else {
            self.ready && !self.plain && !self.records.is_empty()
        }
    }

    /// The client ends: in memory, what waits goes with it; in a file, it
    /// waits for the next client.
    pub(crate) fn close(&mut self) -> Outcomes {
        let mut outcomes = self.disconnected();
        if self.log.is_none() {
            let (count, mut bytes) = (self.records.len(), 0);
            for record in self.records.drain(..) {
                bytes += record.size;
                outcomes.refuse("closed", record.id, record.tag);
            }
            self.room.give_back_many(count, bytes);
            self.earliest = u64::MAX;
            self.deadline = None;
        }
        outcomes
    }

    /// The connection went: what waits goes on the next one. What was
    /// cleared while the server had it in hand is not delivered.
    pub(crate) fn disconnected(&mut self) -> Outcomes {
        let mut outcomes = Outcomes::default();
        self.ready = false;
        self.answered = false;
        self.plain = false;
        self.cursor = 0;
        self.flushed = 0;
        for (_, id, tag) in self.limbo.drain(..) {
            outcomes.refuse("cleared", id, tag);
        }
        self.flush();
        outcomes
    }

    /// The user signed out: every message goes, and the user is forgotten.
    /// One the server has in hand on this connection is delivered when it
    /// confirms it on it, and not delivered when the connection ends first.
    pub(crate) fn clear(&mut self) -> Outcomes {
        let mut outcomes = Outcomes::default();
        let in_hand = if self.answered { self.flushed } else { 0 };
        let mut numbers = Vec::with_capacity(self.records.len());
        let mut bytes = 0;
        for mut record in self.records.drain(..) {
            bytes += record.size;
            numbers.push(record.s);
            let id = std::mem::take(&mut record.id);
            if record.s <= in_hand {
                self.limbo.push_back((record.s, id, record.tag));
            } else {
                outcomes.refuse("cleared", id, record.tag);
            }
        }
        self.room.give_back_many(numbers.len(), bytes);
        self.cursor = 0;
        self.earliest = u64::MAX;
        self.deadline = None;
        if !numbers.is_empty() {
            self.put(&LineRef::Drop(&numbers));
        }
        self.user = None;
        self.put(&LineRef::User(None));
        self.changed();
        outcomes
    }

    /// Drops what waited longer than it may to be written, and sets the
    /// timer for the next to run out.
    pub(crate) fn expire(&mut self, now: u64, outcomes: &mut Outcomes) {
        let (written, cursor) = (self.written, self.cursor);
        self.remove_where("expired", outcomes, |at, r| {
            at >= cursor && r.s > written && r.until < now
        });
        let first = self.records.partition_point(|r| r.s <= written);
        self.earliest = self
            .records
            .range(first..)
            .map(|r| r.until)
            .min()
            .unwrap_or(u64::MAX);
        self.set_deadline(now);
        self.changed();
    }

    fn set_deadline(&mut self, now: u64) {
        self.deadline = if self.earliest == u64::MAX {
            None
        } else {
            Instant::now().checked_add(Duration::from_millis(self.earliest.saturating_sub(now)))
        };
    }

    /// Removes the records `gone` names (by index and record), telling the
    /// application why.
    fn remove_where(
        &mut self,
        why: &str,
        outcomes: &mut Outcomes,
        mut gone: impl FnMut(usize, &Record) -> bool,
    ) {
        let mut numbers = Vec::new();
        let cursor = self.cursor;
        let (mut at, mut before, mut bytes) = (0, 0, 0);
        self.records.retain_mut(|r| {
            let keep = !gone(at, r);
            if !keep {
                bytes += r.size;
                outcomes.refuse(why, std::mem::take(&mut r.id), r.tag);
                numbers.push(r.s);
                if at < cursor {
                    before += 1;
                }
            }
            at += 1;
            keep
        });
        self.cursor -= before;
        self.room.give_back_many(numbers.len(), bytes);
        if !numbers.is_empty() {
            self.put(&LineRef::Drop(&numbers));
        }
    }

    /// Puts `line` in the store's batch.
    fn put(&mut self, line: &LineRef<'_>) {
        if let Some(log) = &mut self.log {
            log.push(line);
        }
    }

    /// Something changed: with no connection, the file gets it now; with
    /// one, at the connection's next write ([`Outbox::due`]), a batch at
    /// once.
    fn changed(&mut self) {
        if !self.ready {
            self.flush();
        }
    }

    /// Hands the batch's lines to the file's thread; writes the file anew
    /// instead once what went outweighs what is kept, or after a write
    /// failed (the lines are in what is written anew).
    pub(crate) fn flush(&mut self) {
        if let Some(log) = &mut self.log {
            self.owed |= log.take_broken();
            if self.owed {
                let now = now_ms();
                if now >= self.heal_at {
                    self.owed = false;
                    self.heal_at = now.saturating_add(1000);
                    self.heal = None;
                    self.rewrite();
                } else {
                    log.forget();
                    self.heal = Some(Instant::now() + Duration::from_millis(self.heal_at - now));
                }
            } else if log.lines > 2 * self.records.len() + 64 {
                self.rewrite();
            } else {
                log.flush();
            }
        }
    }

    /// Writes the file anew, with what is kept now alone, under a new salt.
    #[cfg(not(target_arch = "wasm32"))]
    fn rewrite(&mut self) {
        let Some(log) = &mut self.log else {
            return;
        };
        let salt = new_id();
        let head = Head {
            wse_outbox: 1,
            salt: salt.as_str().into(),
            producer: (&*self.producer).into(),
            next: self.next,
        };
        let mut out = Vec::with_capacity(
            256 + self
                .records
                .iter()
                .map(|r| r.text.len() + 128)
                .sum::<usize>(),
        );
        file::encode(&mut out, b"", &head);
        let mut lines = 1;
        let salted = salt.as_bytes();
        if let Some(node) = &self.node {
            file::encode(&mut out, salted, &LineRef::Node(node));
            lines += 1;
        }
        if self.written > 0 {
            file::encode(&mut out, salted, &LineRef::Written(self.written));
            lines += 1;
        }
        if let Some(Some(user)) = &self.user {
            file::encode(&mut out, salted, &LineRef::User(Some(user)));
            lines += 1;
        }
        for record in &self.records {
            file::encode_add(&mut out, salted, record);
            lines += 1;
        }
        log.rewrite(out, salt, lines);
    }

    /// Writes the database anew, with what is kept now alone.
    #[cfg(target_arch = "wasm32")]
    fn rewrite(&mut self) {
        let Some(log) = &mut self.log else {
            return;
        };
        let user = match &self.user {
            Some(Some(user)) => Some(user.as_str()),
            _ => None,
        };
        log.rewrite(
            self.next,
            self.written,
            self.node.as_deref(),
            user,
            &self.records,
        );
    }
}

/// The message `text` (its JSON object after [`HEADROOM`] spaces, as the
/// client makes it) with its number written in that room: `{"s":N` right
/// before the object, whose own `{` becomes `,`, or a space for an empty
/// object. The text is not copied, and what is left of the room skipped.
fn number_in_place(s: u64, mut text: String) -> ByteString {
    let bytes = text.as_bytes();
    if bytes.len() <= HEADROOM
        || bytes[HEADROOM] != b'{'
        || bytes[..HEADROOM].iter().any(|b| *b != b' ')
    {
        return numbered(s, &text).into();
    }
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    let mut n = s;
    loop {
        at -= 1;
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    let number = &digits[at..];
    let start = HEADROOM - 5 - number.len();
    let empty = bytes[HEADROOM + 1..]
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        == Some(&b'}');
    let mut prefix = [b' '; HEADROOM + 1];
    prefix[start..start + 5].copy_from_slice(b"{\"s\":");
    prefix[start + 5..HEADROOM].copy_from_slice(number);
    prefix[HEADROOM] = if empty { b' ' } else { b',' };
    // ASCII over ASCII of the same length: written in place, the text
    // still UTF-8.
    if let Ok(prefix) = std::str::from_utf8(&prefix) {
        text.replace_range(..=HEADROOM, prefix);
    }
    let mut text = ByteString::from(text);
    text.advance(start);
    text
}

/// A message with its number: `{"s":N,` before the rest of its JSON object.
fn numbered(s: u64, text: &str) -> String {
    let rest = text.trim_start();
    let rest = rest.strip_prefix('{').unwrap_or(rest);
    let mut out = String::with_capacity(text.len() + 24);
    out.push_str("{\"s\":");
    push_number(&mut out, s);
    if !rest.trim_start().starts_with('}') {
        out.push(',');
    }
    out.push_str(rest);
    out
}

/// The message without its number, for a server that does not answer.
fn unnumbered(text: &str) -> String {
    let Some(rest) = text.strip_prefix("{\"s\":") else {
        return text.to_owned();
    };
    let rest = rest.trim_start_matches(|c: char| c.is_ascii_digit());
    let rest = rest.strip_prefix(',').unwrap_or(rest);
    let mut out = String::with_capacity(rest.len() + 1);
    out.push('{');
    out.push_str(rest);
    out
}

/// Writes `n` in decimal, without the formatting machinery.
fn push_number(out: &mut String, n: u64) {
    // A u64 has 20 digits at most.
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    let mut n = n;
    loop {
        at -= 1;
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    for digit in &digits[at..] {
        out.push(char::from(*digit));
    }
}

/// A random id: a producer's, a salt; 128 bits as 32 hex digits.
fn new_id() -> String {
    let (hi, lo) = crate::client::random_pair();
    format!("{hi:016x}{lo:016x}")
}

/// Milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// CRC-32 (IEEE) tables for eight bytes at a time: the table for a byte
/// followed by `k` zero bytes in row `k`.
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
const CRC_TABLES: [[u32; 256]; 8] = {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        tables[0][i] = c;
        i += 1;
    }
    let mut i = 0;
    while i < 256 {
        let mut k = 1;
        while k < 8 {
            let before = tables[k - 1][i];
            tables[k][i] = (before >> 8) ^ tables[0][(before & 0xFF) as usize];
            k += 1;
        }
        i += 1;
    }
    tables
};

/// CRC-32 (IEEE) of `parts` one after another, as zlib computes it: what
/// checks a line of the file.
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
fn crc32(parts: &[&[u8]]) -> u32 {
    let t = &CRC_TABLES;
    let mut crc = !0u32;
    for part in parts {
        let (chunks, rest) = part.as_chunks::<8>();
        for c in chunks {
            let lo = u32::from_le_bytes([c[0], c[1], c[2], c[3]]) ^ crc;
            let hi = u32::from_le_bytes([c[4], c[5], c[6], c[7]]);
            crc = t[7][(lo & 0xFF) as usize]
                ^ t[6][((lo >> 8) & 0xFF) as usize]
                ^ t[5][((lo >> 16) & 0xFF) as usize]
                ^ t[4][(lo >> 24) as usize]
                ^ t[3][(hi & 0xFF) as usize]
                ^ t[2][((hi >> 8) & 0xFF) as usize]
                ^ t[1][((hi >> 16) & 0xFF) as usize]
                ^ t[0][(hi >> 24) as usize];
        }
        for byte in rest {
            crc = t[0][((crc ^ u32::from(*byte)) & 0xFF) as usize] ^ (crc >> 8);
        }
    }
    !crc
}

#[cfg(not(target_arch = "wasm32"))]
mod file {
    use std::collections::VecDeque;
    use std::fs::{File, OpenOptions, TryLockError};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};

    use serde::{Deserialize, Serialize};
    use serde_json::value::RawValue;

    use super::{Head, LineRef, Read, Record, Report, crc32};

    /// A message kept, as read: its text as it was written.
    #[derive(Deserialize)]
    struct RecordIn<'a> {
        s: u64,
        id: Box<str>,
        at: u64,
        until: u64,
        #[serde(default)]
        owned: bool,
        #[serde(default)]
        requires: Option<Vec<String>>,
        #[serde(borrow)]
        text: &'a RawValue,
    }

    /// A line, as read.
    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Line<'a> {
        #[serde(borrow)]
        Add(RecordIn<'a>),
        Written(u64),
        Ack(u64),
        Drop(Vec<u64>),
        User(Option<String>),
        Node(String),
    }

    /// What the file's thread is handed, in order.
    enum Job {
        Append(Vec<u8>),
        Rewrite(Vec<u8>),
    }

    /// The file, as the connection's task sees it: a batch of lines, handed
    /// to the thread that writes them.
    pub(super) struct Log {
        jobs: Option<mpsc::Sender<Job>>,
        thread: Option<std::thread::JoinHandle<()>>,
        salt: String,
        /// The lines of the batch not handed over yet.
        batch: Vec<u8>,
        /// Lines in the file, the batch's included.
        pub(super) lines: usize,
        /// A write failed: the file is to be written anew.
        broken: Arc<AtomicBool>,
    }

    impl Log {
        /// Opens the file at `path`, the client's alone from now on, and
        /// what it holds (None: a new file, or not one of this format).
        pub(super) fn open(path: &Path, report: Report) -> Result<(Self, Option<Read>), String> {
            let fail = |what: &str, e: std::io::Error| format!("{}: {what}: {e}", path.display());
            let mut lock_path = path.as_os_str().to_owned();
            lock_path.push(".lock");
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&lock_path)
                .map_err(|e| fail("cannot open its lock file", e))?;
            match lock.try_lock() {
                Ok(()) => {}
                Err(TryLockError::WouldBlock) => {
                    return Err(format!("{} is another client's", path.display()));
                }
                Err(TryLockError::Error(e)) => return Err(fail("cannot lock it", e)),
            }
            let read = match std::fs::read(path) {
                Ok(bytes) => read(&bytes),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => return Err(fail("cannot read it", e)),
            };
            let file = append(path).map_err(|e| fail("cannot open it", e))?;
            let mut tmp = path.as_os_str().to_owned();
            tmp.push(".tmp");
            let broken = Arc::new(AtomicBool::new(false));
            let writer = Writer {
                path: path.to_owned(),
                tmp: PathBuf::from(tmp),
                file: Some(file),
                _lock: lock,
                report,
                broken: Arc::clone(&broken),
                skipping: false,
                failing: false,
            };
            let (jobs, taken) = mpsc::channel();
            let thread = std::thread::Builder::new()
                .name("wse-outbox".to_owned())
                .spawn(move || writer.run(&taken))
                .map_err(|e| fail("cannot start its thread", e))?;
            let log = Self {
                jobs: Some(jobs),
                thread: Some(thread),
                salt: String::new(),
                batch: Vec::new(),
                lines: 0,
                broken,
            };
            Ok((log, read))
        }

        /// Puts `line` in the batch.
        pub(super) fn push(&mut self, line: &LineRef<'_>) {
            encode(&mut self.batch, self.salt.as_bytes(), line);
            self.lines += 1;
        }

        /// Puts a message kept in the batch.
        pub(super) fn push_add(&mut self, record: &Record) {
            encode_add(&mut self.batch, self.salt.as_bytes(), record);
            self.lines += 1;
        }

        /// Hands the batch to the thread.
        pub(super) fn flush(&mut self) {
            if self.batch.is_empty() {
                return;
            }
            let capacity = self.batch.capacity().min(64 * 1024);
            let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(capacity));
            if let Some(jobs) = &self.jobs {
                let _ = jobs.send(Job::Append(batch));
            }
        }

        /// The batch is in what is written anew.
        pub(super) fn forget(&mut self) {
            self.batch.clear();
        }

        /// A write failed since the last look: the file is to be written
        /// anew.
        pub(super) fn take_broken(&self) -> bool {
            self.broken.swap(false, Ordering::AcqRel)
        }

        /// The file anew: `bytes`, `lines` lines under `salt`.
        pub(super) fn rewrite(&mut self, bytes: Vec<u8>, salt: String, lines: usize) {
            self.batch.clear();
            self.salt = salt;
            self.lines = lines;
            if let Some(jobs) = &self.jobs {
                let _ = jobs.send(Job::Rewrite(bytes));
            }
        }
    }

    impl Drop for Log {
        /// The thread writes what it was handed and ends; the file is free
        /// for the next client once it has.
        fn drop(&mut self) {
            self.jobs.take();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// The thread that writes the file.
    struct Writer {
        path: PathBuf,
        tmp: PathBuf,
        /// Appended to; None between a failed rewrite and the next.
        file: Option<File>,
        /// Held while the client runs: the file is its alone.
        _lock: File,
        report: Report,
        broken: Arc<AtomicBool>,
        /// A write failed: what is appended waits for the file anew.
        skipping: bool,
        /// The last failure was said; the next is said after a success.
        failing: bool,
    }

    impl Writer {
        /// Writes what comes, each run of batches then synced to the disk
        /// at once.
        fn run(mut self, jobs: &mpsc::Receiver<Job>) {
            while let Ok(job) = jobs.recv() {
                let mut dirty = self.take(job);
                while let Ok(job) = jobs.try_recv() {
                    dirty |= self.take(job);
                }
                if dirty
                    && let Some(file) = &self.file
                    && let Err(e) = file.sync_data()
                {
                    self.fail(&format!("cannot sync it to the disk: {e}"));
                }
            }
        }

        /// True: appended, to be synced.
        fn take(&mut self, job: Job) -> bool {
            match job {
                Job::Append(lines) => {
                    if self.skipping {
                        return false;
                    }
                    let written = match &mut self.file {
                        Some(file) => file.write_all(&lines),
                        None => Err(std::io::Error::other("no file")),
                    };
                    match written {
                        Ok(()) => true,
                        Err(e) => {
                            self.fail(&format!("cannot write it: {e}"));
                            false
                        }
                    }
                }
                Job::Rewrite(bytes) => {
                    match self.rewrite(&bytes) {
                        Ok(()) => {
                            self.skipping = false;
                            self.failing = false;
                        }
                        Err(e) => self.fail(&format!("cannot write it anew: {e}")),
                    }
                    false
                }
            }
        }

        /// The file is behind: written anew at the next change, said once.
        fn fail(&mut self, why: &str) {
            self.skipping = true;
            self.broken.store(true, Ordering::Release);
            if !std::mem::replace(&mut self.failing, true) {
                (self.report)("write", format!("{}: {why}", self.path.display()));
            }
        }

        /// The new file beside the old, on the disk, then in its place.
        fn rewrite(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            let written = File::create(&self.tmp).and_then(|mut tmp| {
                tmp.write_all(bytes)?;
                tmp.sync_all()
            });
            if let Err(e) = written {
                let _ = std::fs::remove_file(&self.tmp);
                return Err(e);
            }
            // Closed before it is replaced: Windows does not replace an
            // open file everywhere.
            self.file = None;
            if let Err(e) = std::fs::rename(&self.tmp, &self.path) {
                let _ = std::fs::remove_file(&self.tmp);
                self.file = append(&self.path).ok();
                return Err(e);
            }
            sync_dir(&self.path);
            self.file = Some(append(&self.path)?);
            Ok(())
        }
    }

    fn append(path: &Path) -> std::io::Result<File> {
        OpenOptions::new().create(true).append(true).open(path)
    }

    /// The directory's entry of a file renamed into it, on the disk.
    fn sync_dir(path: &Path) {
        #[cfg(unix)]
        if let Some(dir) = path.parent() {
            let dir = if dir.as_os_str().is_empty() {
                Path::new(".")
            } else {
                dir
            };
            if let Ok(dir) = File::open(dir) {
                let _ = dir.sync_all();
            }
        }
        #[cfg(not(unix))]
        let _ = path;
    }

    /// Appends `line` to `out`: its check, a space, its JSON, a newline.
    pub(super) fn encode(out: &mut Vec<u8>, salt: &[u8], line: &impl Serialize) {
        let start = begin(out);
        // Into a Vec, of these types: it cannot fail.
        let _ = serde_json::to_writer(&mut *out, line);
        end(out, start, salt);
    }

    /// Appends a message kept to `out`, its text as it is (a JSON object):
    /// `{"add":{"s":..,"id":..,"at":..,"until":..,"owned":..,"text":{..}}}`.
    pub(super) fn encode_add(out: &mut Vec<u8>, salt: &[u8], record: &Record) {
        let start = begin(out);
        out.extend_from_slice(b"{\"add\":{\"s\":");
        number(out, record.s);
        out.extend_from_slice(b",\"id\":");
        let _ = serde_json::to_writer(&mut *out, &*record.id);
        out.extend_from_slice(b",\"at\":");
        number(out, record.at);
        out.extend_from_slice(b",\"until\":");
        number(out, record.until);
        out.extend_from_slice(if record.owned {
            b",\"owned\":true"
        } else {
            b",\"owned\":false"
        });
        if let Some(requires) = &record.requires {
            out.extend_from_slice(b",\"requires\":");
            let _ = serde_json::to_writer(&mut *out, requires);
        }
        out.extend_from_slice(b",\"text\":");
        out.extend_from_slice(record.text.as_bytes());
        out.extend_from_slice(b"}}");
        end(out, start, salt);
    }

    /// Room for the check, a space after it: where the line starts.
    fn begin(out: &mut Vec<u8>) -> usize {
        let start = out.len();
        out.extend_from_slice(b"00000000 ");
        start
    }

    /// The check of the line from `start`, written in its room, and the
    /// newline.
    fn end(out: &mut Vec<u8>, start: usize, salt: &[u8]) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let crc = crc32(&[salt, &out[start + 9..]]);
        for (i, byte) in out[start..start + 8].iter_mut().enumerate() {
            *byte = HEX[((crc >> (28 - 4 * i)) & 0xF) as usize];
        }
        out.push(b'\n');
    }

    fn number(out: &mut Vec<u8>, n: u64) {
        let mut digits = [0u8; 20];
        let mut at = digits.len();
        let mut n = n;
        loop {
            at -= 1;
            digits[at] = b'0' + (n % 10) as u8;
            n /= 10;
            if n == 0 {
                break;
            }
        }
        out.extend_from_slice(&digits[at..]);
    }

    /// One line: its JSON, when its check holds.
    fn checked<'a>(line: &'a [u8], salt: &[u8]) -> Option<&'a [u8]> {
        if line.get(8) != Some(&b' ') {
            return None;
        }
        let (crc, json) = (line.get(..8)?, line.get(9..)?);
        let crc = u32::from_str_radix(std::str::from_utf8(crc).ok()?, 16).ok()?;
        (crc32(&[salt, json]) == crc).then_some(json)
    }

    /// A line of a kind a later version writes, an object of one key this
    /// one does not know: skipped. A change it must not skip is a new
    /// format (`wse_outbox`).
    fn unknown_kind(json: &[u8]) -> bool {
        const KNOWN: [&str; 6] = ["add", "written", "ack", "drop", "user", "node"];
        serde_json::from_slice::<std::collections::BTreeMap<String, &RawValue>>(json)
            .is_ok_and(|line| line.len() == 1 && line.keys().all(|k| !KNOWN.contains(&k.as_str())))
    }

    /// What a file holds, read up to the first line that does not check: a
    /// tail cut short, or left from an older file, is not read. None: not a
    /// file of this format.
    pub(super) fn read(bytes: &[u8]) -> Option<Read> {
        let mut lines = bytes.split(|b| *b == b'\n');
        let head: Head<'_> = serde_json::from_slice(checked(lines.next()?, b"")?).ok()?;
        if head.wse_outbox != 1 {
            return None;
        }
        let salt = head.salt.as_bytes();
        let mut read = Read {
            producer: head.producer.as_ref().into(),
            node: None,
            written: 0,
            next: head.next.max(1),
            user: None,
            records: VecDeque::new(),
        };
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let Some(json) = checked(line, salt) else {
                tracing::warn!("[WSE] the outbox's file ends in a line that does not check");
                break;
            };
            let line = match serde_json::from_slice::<Line<'_>>(json) {
                Ok(line) => line,
                Err(_) if unknown_kind(json) => continue,
                Err(_) => {
                    tracing::warn!("[WSE] the outbox's file ends in a line that does not check");
                    break;
                }
            };
            match line {
                Line::Add(record) => {
                    let text = record.text.get();
                    // A message is a JSON object; what it requires, topics.
                    let requires = match record.requires {
                        None => None,
                        Some(topics)
                            if !topics.is_empty() && topics.iter().all(|t| !t.is_empty()) =>
                        {
                            Some(topics.into_boxed_slice())
                        }
                        Some(_) => {
                            tracing::warn!(
                                "[WSE] the outbox's file ends in a line that does not check"
                            );
                            break;
                        }
                    };
                    if !text.starts_with('{') {
                        tracing::warn!(
                            "[WSE] the outbox's file ends in a line that does not check"
                        );
                        break;
                    }
                    read.next = read.next.max(record.s + 1);
                    read.records.push_back(Record {
                        s: record.s,
                        id: record.id,
                        at: record.at,
                        until: record.until,
                        owned: record.owned,
                        requires,
                        size: super::unnumbered(text).len(),
                        text: text.into(),
                        tag: 0,
                    });
                }
                Line::Written(n) => {
                    read.written = read.written.max(n);
                    read.next = read.next.max(n + 1);
                }
                // The records are in the order of their numbers: what an
                // answer took is at the front.
                Line::Ack(n) => {
                    while read.records.front().is_some_and(|r| r.s <= n) {
                        read.records.pop_front();
                    }
                }
                Line::Drop(mut numbers) => {
                    numbers.sort_unstable();
                    read.records
                        .retain(|r| numbers.binary_search(&r.s).is_err());
                }
                Line::User(user) => read.user = user,
                Line::Node(node) => read.node = Some(node.into()),
            }
        }
        Some(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quiet() -> Report {
        Arc::new(|_, _| {})
    }

    fn outbox(offline: &Offline) -> Outbox {
        Outbox::open(offline, Arc::new(Room::new(offline)), quiet())
    }

    /// `json` as the client keeps it: after the room for its number.
    fn kept(json: &str) -> String {
        format!("{}{json}", " ".repeat(HEADROOM))
    }

    fn add(outbox: &mut Outbox, id: &str) -> u64 {
        let json = format!(r#"{{"t":"m","p":{{}},"id":"{id}"}}"#);
        assert!(outbox.room.take(json.len()));
        outbox.add(id.into(), kept(&json), json.len(), None, 0, None, 0)
    }

    /// What goes to the connection's writer now.
    fn due(outbox: &mut Outbox, now: u64, outcomes: &mut Outcomes) -> Vec<(u64, String)> {
        let mut out = Vec::new();
        outbox.due(
            now,
            outcomes,
            |_, _| true,
            |s, text| out.push((s, text.to_string())),
        );
        out
    }

    /// A connection to a server that answers: taken, and answered with
    /// `acked`.
    fn answering(outbox: &mut Outbox, acked: u64) -> Outcomes {
        outbox.connected(Some("ann"), 0);
        outbox.server_answers(true);
        outbox.answered(acked, &[], "n1")
    }

    fn numbers(out: &[(u64, String)]) -> Vec<u64> {
        out.iter().map(|(s, _)| *s).collect()
    }

    fn ids(list: &[String]) -> Vec<&str> {
        list.iter().map(String::as_str).collect()
    }

    #[test]
    fn a_message_waits_for_the_answer_and_goes_again_after_a_reconnect() {
        let mut outbox = outbox(&Offline::in_memory());
        add(&mut outbox, "a");
        add(&mut outbox, "b");
        let mut outcomes = Outcomes::default();
        assert!(
            due(&mut outbox, 0, &mut outcomes).is_empty(),
            "no connection"
        );
        outbox.connected(Some("ann"), 0);
        outbox.server_answers(true);
        assert!(
            due(&mut outbox, 0, &mut outcomes).is_empty(),
            "no answer yet"
        );
        assert_eq!(outbox.hello()["from"], 1);
        outbox.answered(0, &[], "n1");
        let out = due(&mut outbox, 0, &mut outcomes);
        assert_eq!(
            out,
            [
                (1, r#"{"s":1,"t":"m","p":{},"id":"a"}"#.to_owned()),
                (2, r#"{"s":2,"t":"m","p":{},"id":"b"}"#.to_owned())
            ]
        );
        outbox.wrote(2);
        assert_eq!(ids(&outbox.acked(1).delivered), ["a"]);
        // Lost before 2 was answered: it goes again on the next connection.
        assert_eq!(outbox.disconnected(), Outcomes::default());
        assert_eq!(outbox.hello()["written"], 2);
        assert_eq!(outbox.hello()["node"], "n1");
        assert!(answering(&mut outbox, 1).delivered.is_empty());
        assert_eq!(numbers(&due(&mut outbox, 0, &mut outcomes)), [2]);
        add(&mut outbox, "c");
        assert_eq!(
            numbers(&due(&mut outbox, 0, &mut outcomes)),
            [3],
            "a new one"
        );
        assert_eq!(ids(&outbox.acked(3).delivered), ["b", "c"]);
        assert_eq!(outbox.room.count(), 0);
        assert_eq!(outcomes, Outcomes::default());
    }

    #[test]
    fn the_answer_may_come_before_the_server_takes_the_connection() {
        let mut outbox = outbox(&Offline::in_memory());
        add(&mut outbox, "a");
        let mut outcomes = Outcomes::default();
        outbox.server_answers(true);
        outbox.answered(0, &[], "n1");
        assert!(
            due(&mut outbox, 0, &mut outcomes).is_empty(),
            "not taken yet"
        );
        outbox.connected(Some("ann"), 0);
        assert_eq!(numbers(&due(&mut outbox, 0, &mut outcomes)), [1]);
    }

    #[test]
    fn a_server_without_answers_takes_what_was_written() {
        let mut outbox = outbox(&Offline::in_memory());
        add(&mut outbox, "a");
        outbox.connected(None, 0);
        outbox.server_answers(false);
        let mut outcomes = Outcomes::default();
        let out = due(&mut outbox, 0, &mut outcomes);
        assert_eq!(
            out,
            [(1, r#"{"t":"m","p":{},"id":"a"}"#.to_owned())],
            "not numbered"
        );
        assert_eq!(ids(&outbox.wrote(1).delivered), ["a"]);
        assert_eq!(outbox.len(), 0);
        assert_eq!(outbox.hello()["written"], 0);
    }

    #[test]
    fn a_message_never_goes_again_as_another_user_than_it_was_written_as() {
        let mut outbox = outbox(&Offline::in_memory());
        add(&mut outbox, "before any");
        answering(&mut outbox, 0);
        let mut outcomes = Outcomes::default();
        assert_eq!(numbers(&due(&mut outbox, 0, &mut outcomes)), [1]);
        // Lost before its answer; the next connection is bob's.
        outbox.disconnected();
        let outcomes = outbox.connected(Some("bob"), 0);
        assert_eq!(
            outcomes.not_delivered,
            [("user_changed".to_owned(), vec!["before any".to_owned()])]
        );
        outbox.server_answers(true);
        outbox.answered(0, &[], "n1");
        assert!(due(&mut outbox, 0, &mut Outcomes::default()).is_empty());
    }

    #[test]
    fn another_users_messages_go_when_a_connection_is_taken_for_another() {
        let mut outbox = outbox(&Offline::in_memory());
        add(&mut outbox, "before any");
        outbox.connected(Some("ann"), 0);
        outbox.disconnected();
        add(&mut outbox, "anns");
        let outcomes = outbox.connected(Some("bob"), 0);
        assert_eq!(
            outcomes.not_delivered,
            [("user_changed".to_owned(), vec!["anns".to_owned()])]
        );
        assert_eq!(
            outbox.len(),
            1,
            "sent before any connection: it goes on any"
        );
        assert_eq!(outbox.room.count(), 1);
    }

    #[test]
    fn a_message_not_written_in_its_time_runs_out_on_a_timer() {
        let mut outbox = outbox(&Offline::in_memory());
        assert!(outbox.room.take(2));
        outbox.add(
            "late".into(),
            kept("{}"),
            2,
            Some(Duration::from_millis(5)),
            0,
            None,
            0,
        );
        add(&mut outbox, "on time");
        assert!(outbox.deadline().is_some(), "a timer for the late one");
        let mut outcomes = Outcomes::default();
        outbox.expire(10, &mut outcomes);
        assert_eq!(
            outcomes.not_delivered,
            [("expired".to_owned(), vec!["late".to_owned()])]
        );
        assert!(outbox.deadline().is_some(), "a timer for the one on time");
        let outcomes = answering(&mut outbox, 0);
        assert_eq!(outcomes, Outcomes::default());
        let mut outcomes = Outcomes::default();
        assert_eq!(numbers(&due(&mut outbox, 10, &mut outcomes)), [2]);
    }

    #[test]
    fn a_refusal_not_heard_comes_with_the_answer() {
        let mut outbox = outbox(&Offline::in_memory());
        answering(&mut outbox, 0);
        for id in ["a", "b", "c"] {
            add(&mut outbox, id);
        }
        let mut outcomes = Outcomes::default();
        due(&mut outbox, 0, &mut outcomes);
        outbox.disconnected();
        assert_eq!(outbox.hello()["from"], 1, "waits to hear of 1 on");
        outbox.connected(Some("ann"), 0);
        outbox.server_answers(true);
        let outcomes = outbox.answered(3, &[(2, 2, "busy")], "n1");
        assert_eq!(ids(&outcomes.delivered), ["a", "c"]);
        assert_eq!(
            outcomes.not_delivered,
            [("busy".to_owned(), vec!["b".to_owned()])]
        );
        assert_eq!(outbox.hello()["from"], 4, "nothing waits");
        assert_eq!(outbox.room.count(), 0);
    }

    #[test]
    fn a_refused_message_is_said() {
        let mut outbox = outbox(&Offline::in_memory());
        answering(&mut outbox, 0);
        add(&mut outbox, "x");
        add(&mut outbox, "y");
        let mut outcomes = Outcomes::default();
        assert_eq!(numbers(&due(&mut outbox, 0, &mut outcomes)), [1, 2]);
        let refused = outbox.refused(1, 1, "busy");
        assert_eq!(
            refused.not_delivered,
            [("busy".to_owned(), vec!["x".to_owned()])]
        );
        assert_eq!(ids(&outbox.acked(2).delivered), ["y"]);
        assert_eq!(outbox.room.count(), 0);
    }

    #[test]
    fn a_message_written_waits_for_its_answer_whatever_its_age() {
        let mut outbox = outbox(&Offline::in_memory());
        answering(&mut outbox, 0);
        assert!(outbox.room.take(2));
        outbox.add(
            "a".into(),
            kept("{}"),
            2,
            Some(Duration::from_millis(5)),
            0,
            None,
            0,
        );
        let mut outcomes = Outcomes::default();
        assert_eq!(numbers(&due(&mut outbox, 0, &mut outcomes)), [1]);
        outbox.disconnected();
        let outcomes = answering(&mut outbox, 0);
        assert_eq!(outcomes, Outcomes::default(), "not expired: it was written");
        let mut outcomes = Outcomes::default();
        assert_eq!(numbers(&due(&mut outbox, 100, &mut outcomes)), [1]);
        assert_eq!(outcomes, Outcomes::default());
    }

    #[test]
    fn a_clear_tells_what_the_server_has_in_hand_by_its_answer() {
        let mut outbox = outbox(&Offline::in_memory());
        answering(&mut outbox, 0);
        add(&mut outbox, "a");
        add(&mut outbox, "b");
        let mut outcomes = Outcomes::default();
        due(&mut outbox, 0, &mut outcomes);
        outbox.wrote(2);
        add(&mut outbox, "c");
        let cleared = outbox.clear();
        assert_eq!(
            cleared.not_delivered,
            [("cleared".to_owned(), vec!["c".to_owned()])],
            "c never left"
        );
        assert_eq!(outbox.room.count(), 0);
        assert_eq!(ids(&outbox.acked(1).delivered), ["a"]);
        assert_eq!(
            outbox.disconnected().not_delivered,
            [("cleared".to_owned(), vec!["b".to_owned()])]
        );
        // Nothing of it goes on the next connection.
        answering(&mut outbox, 0);
        assert!(due(&mut outbox, 0, &mut outcomes).is_empty());
    }

    #[test]
    fn the_room_refuses_past_its_limits() {
        let mut offline = Offline::in_memory();
        offline.max_messages = 2;
        offline.max_bytes = 100;
        let room = Room::new(&offline);
        assert!(room.take(10));
        assert!(room.take(80));
        assert!(!room.take(1), "two at most");
        room.give_back(80);
        assert!(!room.take(91), "100 bytes at most");
        assert!(room.take(90));
    }

    #[test]
    fn crc32_is_zlibs() {
        assert_eq!(crc32(&[b"123456789"]), 0xCBF4_3926);
        assert_eq!(crc32(&[b"1234", b"56789"]), 0xCBF4_3926);
        // Eight bytes at a time, as one at a time: every length and split.
        let bytes: Vec<u8> = (0..200u32).map(|n| (n * 37 + 11) as u8).collect();
        let one_at_a_time = |data: &[u8]| {
            let mut crc = !0u32;
            for byte in data {
                crc = CRC_TABLES[0][((crc ^ u32::from(*byte)) & 0xFF) as usize] ^ (crc >> 8);
            }
            !crc
        };
        for len in 0..bytes.len() {
            let data = &bytes[..len];
            assert_eq!(crc32(&[data]), one_at_a_time(data), "{len} bytes");
            let (a, b) = data.split_at(len / 3);
            assert_eq!(crc32(&[a, b]), one_at_a_time(data), "{len} bytes, split");
        }
    }

    #[test]
    fn the_number_is_written_in_the_room_before_the_message() {
        let text = number_in_place(7, kept(r#"{"t":"x"}"#));
        assert_eq!(&*text, r#"{"s":7,"t":"x"}"#);
        assert_eq!(&*number_in_place(7, kept("{}")), r#"{"s":7 }"#);
        assert_eq!(&*number_in_place(12, kept("{ }")), r#"{"s":12  }"#);
        let most = number_in_place(u64::MAX, kept(r#"{"a":1}"#));
        assert_eq!(&*most, format!(r#"{{"s":{},"a":1}}"#, u64::MAX));
        // Without its room, as a text read back is: numbered all the same.
        assert_eq!(
            &*number_in_place(3, r#"{"t":"x"}"#.to_owned()),
            r#"{"s":3,"t":"x"}"#
        );
        for text in [&*text, &*most] {
            serde_json::from_str::<serde_json::Value>(text).unwrap();
        }
    }

    #[test]
    fn numbering_keeps_the_json_whole() {
        assert_eq!(numbered(7, r#"{"t":"x"}"#), r#"{"s":7,"t":"x"}"#);
        assert_eq!(numbered(7, "{}"), r#"{"s":7}"#);
        assert_eq!(numbered(18, r#" { "t":1}"#), r#"{"s":18, "t":1}"#);
        assert_eq!(unnumbered(r#"{"s":7,"t":"x"}"#), r#"{"t":"x"}"#);
        assert_eq!(unnumbered(r#"{"s":7}"#), "{}");
        assert_eq!(numbered(u64::MAX, "{}"), format!(r#"{{"s":{}}}"#, u64::MAX));
    }

    #[cfg(not(target_arch = "wasm32"))]
    mod files {
        use super::*;

        fn dir() -> std::path::PathBuf {
            let dir = std::env::temp_dir().join(format!(
                "wse-outbox-{}-{}",
                std::process::id(),
                fastrand::u64(..)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        }

        #[test]
        fn what_is_kept_outlives_the_client_until_the_server_took_it() {
            let dir = dir();
            let offline = Offline::in_file(dir.join("queue"));
            let producer = {
                let mut outbox = outbox(&offline);
                answering(&mut outbox, 0);
                add(&mut outbox, "a");
                add(&mut outbox, "b");
                add(&mut outbox, "c");
                let mut outcomes = Outcomes::default();
                due(&mut outbox, 0, &mut outcomes);
                outbox.acked(1);
                outbox.producer.clone()
            };
            // The application ended and starts again.
            let mut outbox = outbox(&offline);
            assert_eq!(outbox.producer, producer, "the same producer");
            assert_eq!(outbox.len(), 2);
            assert_eq!(outbox.hello()["written"], 3);
            assert_eq!(outbox.hello()["node"], "n1");
            assert_eq!(outbox.room.count(), 2);
            let mut outcomes = Outcomes::default();
            answering(&mut outbox, 1);
            let out = due(&mut outbox, 0, &mut outcomes);
            assert_eq!(
                out,
                [
                    (2, r#"{"s":2,"t":"m","p":{},"id":"b"}"#.to_owned()),
                    (3, r#"{"s":3,"t":"m","p":{},"id":"c"}"#.to_owned())
                ],
                "the text as it was"
            );
            assert_eq!(add(&mut outbox, "d"), 4, "numbers go on");
            drop(outbox);
            std::fs::remove_dir_all(&dir).unwrap();
        }

        #[test]
        fn a_number_is_never_used_twice() {
            let dir = dir();
            let offline = Offline::in_file(dir.join("queue"));
            {
                let mut outbox = outbox(&offline);
                add(&mut outbox, "a");
                add(&mut outbox, "b");
                outbox.clear();
            }
            let mut outbox = outbox(&offline);
            assert_eq!(outbox.len(), 0);
            assert_eq!(add(&mut outbox, "c"), 3);
            drop(outbox);
            // Written anew at the open: the next number is in its head.
            let mut outbox = self::outbox(&offline);
            assert_eq!(add(&mut outbox, "d"), 4);
            drop(outbox);
            std::fs::remove_dir_all(&dir).unwrap();
        }

        #[test]
        fn a_tail_that_does_not_check_is_not_read() {
            let dir = dir();
            let path = dir.join("queue");
            let offline = Offline::in_file(&path);
            {
                let mut outbox = outbox(&offline);
                add(&mut outbox, "a");
                add(&mut outbox, "b");
            }
            // Cut short as the application ended, then bytes that are not
            // even text, as a power loss can leave.
            let mut bytes = std::fs::read(&path).unwrap();
            bytes.truncate(bytes.len() - 5);
            bytes.extend_from_slice(&[0xff, 0xfe, 0x00, b'\n']);
            std::fs::write(&path, &bytes).unwrap();
            let outbox = outbox(&offline);
            assert_eq!(outbox.len(), 1, "read up to the line cut short");
            drop(outbox);
            std::fs::remove_dir_all(&dir).unwrap();
        }

        #[test]
        fn a_file_that_does_not_open_is_said_and_the_outbox_kept_in_memory() {
            let dir = dir();
            let offline = Offline::in_file(dir.join("queue"));
            let first = outbox(&offline);
            let said = Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = Arc::clone(&said);
            let report: Report = Arc::new(move |operation, why| {
                seen.lock().unwrap().push((operation, why));
            });
            let mut second = Outbox::open(&offline, Arc::new(Room::new(&offline)), report);
            {
                let said = said.lock().unwrap();
                assert_eq!(said.len(), 1, "{said:?}");
                assert_eq!(said[0].0, "open");
                assert!(said[0].1.contains("another client's"), "{said:?}");
            }
            add(&mut second, "a");
            assert_eq!(second.len(), 1, "in memory");
            drop((first, second));
            std::fs::remove_dir_all(&dir).unwrap();
        }

        #[test]
        fn the_file_is_written_anew_once_what_went_outweighs_what_is_kept() {
            let dir = dir();
            let path = dir.join("queue");
            let mut outbox = outbox(&Offline::in_file(&path));
            answering(&mut outbox, 0);
            let mut outcomes = Outcomes::default();
            for round in 0..300 {
                let s = add(&mut outbox, &format!("m{round}"));
                due(&mut outbox, 0, &mut outcomes);
                outbox.acked(s);
            }
            drop(outbox);
            let lines = std::fs::read_to_string(&path).unwrap().lines().count();
            assert!(lines < 80, "{lines} lines");
            let outbox = self::outbox(&Offline::in_file(&path));
            assert_eq!(outbox.len(), 0);
            assert_eq!(outbox.next, 301);
            drop(outbox);
            std::fs::remove_dir_all(&dir).unwrap();
        }

        /// The file format all clients share: each line written byte for
        /// byte as the vector has it, and the file read to its state.
        #[test]
        fn the_shared_vector_is_written_and_read_alike() {
            let vector: serde_json::Value =
                serde_json::from_str(include_str!("../tests/vectors/outbox.json")).unwrap();
            let salt = vector["salt"].as_str().unwrap().as_bytes();
            let mut whole = Vec::new();
            for line in vector["lines"].as_array().unwrap() {
                let json = line["json"].as_str().unwrap();
                let value: serde_json::Value = serde_json::from_str(json).unwrap();
                let mut out = Vec::new();
                match line["kind"].as_str().unwrap() {
                    "head" => {
                        let head = Head {
                            wse_outbox: 1,
                            salt: value["salt"].as_str().unwrap().into(),
                            producer: value["producer"].as_str().unwrap().into(),
                            next: value["next"].as_u64().unwrap(),
                        };
                        file::encode(&mut out, b"", &head);
                    }
                    "add" => {
                        let add = &value["add"];
                        let text = json
                            .split_once(",\"text\":")
                            .and_then(|(_, rest)| rest.strip_suffix("}}"))
                            .unwrap();
                        let record = Record {
                            s: add["s"].as_u64().unwrap(),
                            id: add["id"].as_str().unwrap().into(),
                            at: add["at"].as_u64().unwrap(),
                            until: add["until"].as_u64().unwrap(),
                            owned: add["owned"].as_bool().unwrap(),
                            requires: add["requires"].as_array().map(|topics| {
                                topics
                                    .iter()
                                    .map(|t| t.as_str().unwrap().to_owned())
                                    .collect()
                            }),
                            text: text.into(),
                            size: 0,
                            tag: 0,
                        };
                        file::encode_add(&mut out, salt, &record);
                    }
                    "node" => file::encode(
                        &mut out,
                        salt,
                        &LineRef::Node(value["node"].as_str().unwrap()),
                    ),
                    "written" => {
                        file::encode(
                            &mut out,
                            salt,
                            &LineRef::Written(value["written"].as_u64().unwrap()),
                        );
                    }
                    "user" => file::encode(&mut out, salt, &LineRef::User(value["user"].as_str())),
                    "ack" => file::encode(
                        &mut out,
                        salt,
                        &LineRef::Ack(value["ack"].as_u64().unwrap()),
                    ),
                    "drop" => {
                        let numbers: Vec<u64> = value["drop"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|n| n.as_u64().unwrap())
                            .collect();
                        file::encode(&mut out, salt, &LineRef::Drop(&numbers));
                    }
                    kind => panic!("a line of kind {kind}"),
                }
                let expected = format!("{}\n", line["line"].as_str().unwrap());
                assert_eq!(String::from_utf8(out.clone()).unwrap(), expected);
                whole.extend_from_slice(&out);
            }

            let read = file::read(&whole).unwrap();
            let state = &vector["state"];
            assert_eq!(&*read.producer, state["producer"].as_str().unwrap());
            assert_eq!(read.node.as_deref(), state["node"].as_str());
            assert_eq!(read.written, state["written"].as_u64().unwrap());
            assert_eq!(read.next, state["next"].as_u64().unwrap());
            assert_eq!(read.user.as_deref(), state["user"].as_str());
            type Row<'a> = (u64, &'a str, u64, u64, bool, Option<Vec<String>>, &'a str);
            let records: Vec<Row<'_>> = read
                .records
                .iter()
                .map(|r| {
                    let requires = r.requires.as_ref().map(|t| t.to_vec());
                    (r.s, &*r.id, r.at, r.until, r.owned, requires, &*r.text)
                })
                .collect();
            let expected: Vec<Row<'_>> = state["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r["s"].as_u64().unwrap(),
                        r["id"].as_str().unwrap(),
                        r["at"].as_u64().unwrap(),
                        r["until"].as_u64().unwrap(),
                        r["owned"].as_bool().unwrap(),
                        r["requires"]
                            .as_array()
                            .map(|t| t.iter().map(|t| t.as_str().unwrap().to_owned()).collect()),
                        r["text"].as_str().unwrap(),
                    )
                })
                .collect();
            assert_eq!(records, expected);

            for damaged in vector["damaged"].as_array().unwrap() {
                let read = file::read(damaged["file"].as_str().unwrap().as_bytes());
                let why = damaged["why"].as_str().unwrap();
                match damaged["records"].as_array() {
                    None => assert!(read.is_none(), "{why}"),
                    Some(ids) => {
                        let read = read.unwrap();
                        let got: Vec<&str> = read.records.iter().map(|r| &*r.id).collect();
                        let want: Vec<&str> = ids.iter().map(|id| id.as_str().unwrap()).collect();
                        assert_eq!(got, want, "{why}");
                        assert_eq!(read.user.as_deref(), damaged["user"].as_str(), "{why}");
                    }
                }
            }
        }

        /// A rewrite owed within the second after the last one comes on the
        /// outbox's deadline, with no further change to wait for.
        #[cfg(unix)]
        #[test]
        fn a_rewrite_owed_within_the_second_comes_on_the_deadline() {
            use std::os::unix::fs::PermissionsExt;

            let dir = dir();
            let path = dir.join("queue");
            let said = Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = Arc::clone(&said);
            let report: Report = Arc::new(move |operation, why| {
                seen.lock().unwrap().push((operation, why));
            });
            let offline = Offline::in_file(&path);
            let mut outbox = Outbox::open(&offline, Arc::new(Room::new(&offline)), report);
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
            if std::fs::write(dir.join("probe"), b"").is_ok() {
                std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
                drop(outbox);
                std::fs::remove_dir_all(&dir).unwrap();
                return;
            }
            answering(&mut outbox, 0);
            let mut outcomes = Outcomes::default();
            // Enough went to write the file anew: that fails (the folder
            // takes no file), and so does the rewrite at once after it.
            for round in 0..100 {
                let s = add(&mut outbox, &format!("m{round}"));
                due(&mut outbox, 0, &mut outcomes);
                outbox.acked(s);
            }
            for _ in 0..250 {
                if !said.lock().unwrap().is_empty() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            add(&mut outbox, "a");
            due(&mut outbox, 0, &mut outcomes);
            std::thread::sleep(Duration::from_millis(200));
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            // Within the second after that rewrite: none yet, a deadline for it.
            add(&mut outbox, "b");
            due(&mut outbox, 0, &mut outcomes);
            let at = outbox.deadline().expect("a deadline for the rewrite owed");
            assert!(at <= Instant::now() + Duration::from_secs(1));
            std::thread::sleep(at.saturating_duration_since(Instant::now()));
            outbox.heal(Instant::now());
            // On disk with no further change: its file holds both.
            let mut both = false;
            for _ in 0..250 {
                let text = std::fs::read_to_string(&path).unwrap_or_default();
                if text.contains(r#""id":"a""#) && text.contains(r#""id":"b""#) {
                    both = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(both, "the file written anew with a and b");
            assert_eq!(outbox.heal, None, "nothing owed any more");
            drop(outbox);
            std::fs::remove_dir_all(&dir).unwrap();
        }

        /// A file that cannot be written anew says so once, and is written
        /// anew with all that is kept once it can.
        #[cfg(unix)]
        #[test]
        fn a_file_that_cannot_be_written_says_so_and_heals() {
            use std::os::unix::fs::PermissionsExt;

            let dir = dir();
            let path = dir.join("queue");
            let said = Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = Arc::clone(&said);
            let report: Report = Arc::new(move |operation, why| {
                seen.lock().unwrap().push((operation, why));
            });
            let offline = Offline::in_file(&path);
            let mut outbox = Outbox::open(&offline, Arc::new(Room::new(&offline)), report);
            // The directory takes no new file: writing the file anew fails
            // (not for root, who writes anywhere).
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
            if std::fs::write(dir.join("probe"), b"").is_ok() {
                std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
                drop(outbox);
                std::fs::remove_dir_all(&dir).unwrap();
                return;
            }
            answering(&mut outbox, 0);
            let mut outcomes = Outcomes::default();
            for round in 0..100 {
                let s = add(&mut outbox, &format!("m{round}"));
                due(&mut outbox, 0, &mut outcomes);
                outbox.acked(s);
            }
            add(&mut outbox, "kept");
            due(&mut outbox, 0, &mut outcomes);
            // Each failure after the first waits for a success to be said.
            for _ in 0..250 {
                if !said.lock().unwrap().is_empty() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(said.lock().unwrap().len(), 1, "{:?}", said.lock().unwrap());
            assert_eq!(said.lock().unwrap()[0].0, "write");
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            outbox.disconnected();
            add(&mut outbox, "after");
            drop(outbox);
            let outbox = self::outbox(&offline);
            assert_eq!(outbox.len(), 2, "kept and after, written anew");
            drop(outbox);
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }
}
