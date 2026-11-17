//! Messages the application sent while there was no connection, kept for
//! the next one of the same user: in memory, or in a file that outlives the
//! application.
//!
//! The file is a log of lines, each a JSON object: a message kept
//! (`{"add":{..}}`), messages that went (`{"sent":[ids]}`), the user of the
//! last connection (`{"user":..}`). It is read whole when the client starts
//! and written over, without what went, when it grows past twice what it
//! holds. A line is written as the message is kept (it outlives the
//! application ending); a thread of its own syncs the file to the disk right
//! after, so that it outlives a power loss as well, without the send waiting
//! for the disk.

use std::collections::{HashSet, VecDeque};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// How many messages are kept at most, by default.
const MAX_MESSAGES: usize = 1000;

/// How long a message is kept, by default.
const MAX_AGE: Duration = Duration::from_secs(3600);

/// Where messages sent while there is no connection wait for the next one,
/// and for how long ([`Config::offline`](crate::Config::offline)).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Offline {
    /// The file they are kept in, so that they outlive the application
    /// (outside the browser). It is this client's alone while it runs; a
    /// lock file beside it (`<path>.lock`) says so. None keeps them in
    /// memory, for the client's life.
    #[cfg(not(target_arch = "wasm32"))]
    pub path: Option<std::path::PathBuf>,
    /// How many are kept at most (1,000): past it a send fails with
    /// [`Error::Full`].
    pub max_messages: usize,
    /// How long one is kept (an hour): an older one is dropped, not sent.
    pub max_age: Duration,
}

impl Offline {
    /// Kept in memory, for the client's life.
    pub fn in_memory() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            path: None,
            max_messages: MAX_MESSAGES,
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
}

impl Default for Offline {
    fn default() -> Self {
        Self::in_memory()
    }
}

/// A message kept.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Record {
    id: Box<str>,
    /// When it was sent, in milliseconds since the Unix epoch.
    at: u64,
    /// The user of the last connection when it was sent: only a connection
    /// of that user sends it. None (sent before any connection, or after
    /// one without a user) goes on any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    /// The message as it goes on the wire.
    text: String,
}

/// One line of the file, as it is read.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Line {
    Add(Record),
    Sent(Vec<Box<str>>),
    User(Option<String>),
}

/// One line of the file, as it is written: borrowed, nothing copied.
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum LineRef<'a> {
    Add(&'a Record),
    Sent(&'a [Box<str>]),
    User(Option<&'a str>),
}

/// What went wrong with the file, for [`Event::StorageFailed`](crate::Event::StorageFailed).
pub(crate) type Report = Box<dyn Fn(&'static str, String) + Send + Sync>;

struct Inner {
    records: VecDeque<Record>,
    /// The user of the last connection, once there was one.
    user: Option<String>,
    /// A connection takes what is kept now: a message sent goes to it.
    taking: bool,
    #[cfg(not(target_arch = "wasm32"))]
    file: Option<file::Log>,
}

impl Inner {
    /// The file, when there is one: what a line is written to.
    fn file_or_none(&mut self) -> Option<&mut file::Log> {
        #[cfg(not(target_arch = "wasm32"))]
        return self.file.as_mut();
        #[cfg(target_arch = "wasm32")]
        return None;
    }
}

/// The messages kept, shared by the client's handles and its connection.
pub(crate) struct Store {
    inner: Mutex<Inner>,
    max_messages: usize,
    max_age: Duration,
    /// Where a failure of the file is said.
    #[cfg(not(target_arch = "wasm32"))]
    report: Report,
}

impl Store {
    /// Opens the store `offline` names; a file that cannot be opened or is
    /// another client's fails.
    pub(crate) fn open(offline: &Offline, report: Report) -> Result<Self, Error> {
        if offline.max_messages == 0 {
            return Err(Error::Config(
                "the offline queue must hold at least one message".to_owned(),
            ));
        }
        #[allow(unused_mut)]
        let mut inner = Inner {
            records: VecDeque::new(),
            user: None,
            taking: false,
            #[cfg(not(target_arch = "wasm32"))]
            file: None,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(path) = &offline.path {
            let (log, read) = file::Log::open(path)?;
            inner.records = read.records;
            inner.user = read.user;
            inner.file = Some(log);
        }
        let store = Self {
            inner: Mutex::new(inner),
            max_messages: offline.max_messages,
            max_age: offline.max_age,
            #[cfg(not(target_arch = "wasm32"))]
            report,
        };
        #[cfg(target_arch = "wasm32")]
        drop(report);
        // What ran out while the application was away goes now, and the
        // file is written without what went.
        {
            let mut inner = store.lock();
            store.drop_old(&mut inner, now_ms());
            #[cfg(not(target_arch = "wasm32"))]
            store.rewrite(&mut inner);
        }
        Ok(store)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Keeps a message sent with no connection (`id`, the wire `text`):
    /// `None`. One that comes while a connection takes them is given back,
    /// to go on it.
    pub(crate) fn keep(&self, id: &str, text: String) -> Result<Option<String>, Error> {
        let mut inner = self.lock();
        if inner.taking {
            return Ok(Some(text));
        }
        let now = now_ms();
        self.drop_old(&mut inner, now);
        if inner.records.len() >= self.max_messages {
            return Err(Error::Full);
        }
        let record = Record {
            id: id.into(),
            at: now,
            owner: inner.user.clone(),
            text,
        };
        let inner = &mut *inner;
        self.write(&mut inner.file_or_none(), &LineRef::Add(&record));
        inner.records.push_back(record);
        Ok(None)
    }

    /// A connection of `user` takes what is kept for it, in the order it
    /// was sent; what is another user's or ran out goes. Until it ends
    /// ([`Store::release`]) messages sent go to it.
    pub(crate) fn take_for(&self, user: Option<&str>) -> Vec<(Box<str>, String)> {
        let mut guard = self.lock();
        let inner = &mut *guard;
        inner.taking = true;
        if inner.user.as_deref() != user {
            inner.user = user.map(str::to_owned);
            self.write(&mut inner.file_or_none(), &LineRef::User(user));
        }
        self.drop_old(inner, now_ms());
        let mut foreign = Vec::new();
        inner.records.retain(|r| {
            let theirs = r.owner.is_some() && r.owner.as_deref() != user;
            if theirs {
                foreign.push(r.id.clone());
            }
            !theirs
        });
        if !foreign.is_empty() {
            tracing::warn!(
                "[WSE] {} message(s) kept for another user were dropped",
                foreign.len()
            );
            self.write(&mut inner.file_or_none(), &LineRef::Sent(&foreign));
        }
        inner
            .records
            .iter()
            .map(|r| (r.id.clone(), r.text.clone()))
            .collect()
    }

    /// The connection that took messages ended: what is sent from now on is
    /// kept again.
    pub(crate) fn release(&self) {
        self.lock().taking = false;
    }

    /// The messages `ids` went out: they are no longer kept.
    pub(crate) fn sent(&self, ids: &[Box<str>]) {
        if ids.is_empty() {
            return;
        }
        let mut guard = self.lock();
        let inner = &mut *guard;
        let gone: HashSet<&str> = ids.iter().map(AsRef::as_ref).collect();
        inner.records.retain(|r| !gone.contains(r.id.as_ref()));
        self.write(&mut inner.file_or_none(), &LineRef::Sent(ids));
        #[cfg(not(target_arch = "wasm32"))]
        if inner
            .file
            .as_ref()
            .is_some_and(|f| f.lines > 2 * inner.records.len() + 64)
        {
            self.rewrite(inner);
        }
    }

    /// How many are kept.
    pub(crate) fn len(&self) -> usize {
        self.lock().records.len()
    }

    /// Drops what is older than `max_age`.
    fn drop_old(&self, inner: &mut Inner, now: u64) {
        let max_age = u64::try_from(self.max_age.as_millis()).unwrap_or(u64::MAX);
        let mut old = Vec::new();
        while inner
            .records
            .front()
            .is_some_and(|r| now.saturating_sub(r.at) > max_age)
        {
            if let Some(r) = inner.records.pop_front() {
                old.push(r.id);
            }
        }
        if !old.is_empty() {
            tracing::warn!(
                "[WSE] {} message(s) kept longer than {:?} were dropped",
                old.len(),
                self.max_age
            );
            self.write(&mut inner.file_or_none(), &LineRef::Sent(&old));
        }
    }

    /// Writes `line` to the file, when there is one.
    #[cfg_attr(target_arch = "wasm32", allow(clippy::unused_self))]
    fn write(&self, file: &mut Option<&mut file::Log>, line: &LineRef<'_>) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(log) = file
            && let Err(e) = log.append(line)
        {
            (self.report)("write", e);
        }
        #[cfg(target_arch = "wasm32")]
        let _ = (file, line);
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn rewrite(&self, inner: &mut Inner) {
        let Inner {
            records,
            user,
            file,
            ..
        } = inner;
        if let Some(log) = file
            && let Err(e) = log.rewrite(records, user.as_deref())
        {
            (self.report)("write", e);
        }
    }
}

/// Milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// No file in the browser.
#[cfg(target_arch = "wasm32")]
mod file {
    pub(super) enum Log {}
}

#[cfg(not(target_arch = "wasm32"))]
mod file {
    use std::fs::{File, OpenOptions, TryLockError};
    use std::io::{BufRead, BufReader, Write};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Condvar, Mutex};

    use super::{Line, LineRef, Record, VecDeque};
    use crate::error::Error;

    /// What the file held when it was opened.
    pub(super) struct Read {
        pub(super) records: VecDeque<Record>,
        pub(super) user: Option<String>,
    }

    /// The file, appended to; a thread of its own syncs it to the disk.
    pub(super) struct Log {
        path: PathBuf,
        file: File,
        /// Lines in the file.
        pub(super) lines: usize,
        sync: Arc<Sync>,
        /// Held while the client runs: the file is its alone.
        _lock: File,
    }

    /// Asks the syncing thread to sync the file it holds.
    struct Sync {
        state: Mutex<SyncState>,
        wake: Condvar,
    }

    struct SyncState {
        /// The file to sync: written since the last sync.
        dirty: Option<File>,
        /// The log went: the thread ends.
        closed: bool,
        /// Why the last sync failed, for the next write to say.
        failed: Option<String>,
    }

    impl Log {
        pub(super) fn open(path: &Path) -> Result<(Self, Read), Error> {
            let fail = |what: &str, e: std::io::Error| {
                Error::Config(format!("the offline queue {}: {what}: {e}", path.display()))
            };
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
                    return Err(Error::Config(format!(
                        "the offline queue {} is another client's",
                        path.display()
                    )));
                }
                Err(TryLockError::Error(e)) => return Err(fail("cannot lock it", e)),
            }
            let read = match File::open(path) {
                Ok(file) => read(BufReader::new(file)).map_err(|e| fail("cannot read it", e))?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Read {
                    records: VecDeque::new(),
                    user: None,
                },
                Err(e) => return Err(fail("cannot open it", e)),
            };
            let file = append(path).map_err(|e| fail("cannot open it", e))?;
            let sync = Arc::new(Sync {
                state: Mutex::new(SyncState {
                    dirty: None,
                    closed: false,
                    failed: None,
                }),
                wake: Condvar::new(),
            });
            let syncing = Arc::clone(&sync);
            std::thread::Builder::new()
                .name("wse-offline".to_owned())
                .spawn(move || syncing.run())
                .map_err(|e| fail("cannot start its thread", e))?;
            let log = Self {
                path: path.to_owned(),
                file,
                lines: 0,
                sync,
                _lock: lock,
            };
            Ok((log, read))
        }

        /// Writes `line`; the thread syncs it right after.
        pub(super) fn append(&mut self, line: &LineRef<'_>) -> Result<(), String> {
            let mut text = serde_json::to_string(line).map_err(|e| e.to_string())?;
            text.push('\n');
            self.file
                .write_all(text.as_bytes())
                .map_err(|e| e.to_string())?;
            self.lines += 1;
            self.sync.dirty(&self.file)
        }

        /// Writes the file over with `records` alone, the old one kept until
        /// the new one is on the disk.
        pub(super) fn rewrite(
            &mut self,
            records: &VecDeque<Record>,
            user: Option<&str>,
        ) -> Result<(), String> {
            let mut tmp_path = self.path.as_os_str().to_owned();
            tmp_path.push(".tmp");
            let tmp_path = PathBuf::from(tmp_path);
            let written = (|| {
                let mut out = std::io::BufWriter::new(File::create(&tmp_path)?);
                let mut lines = 0;
                if user.is_some() {
                    serde_json::to_writer(&mut out, &LineRef::User(user))?;
                    out.write_all(b"\n")?;
                    lines += 1;
                }
                for record in records {
                    serde_json::to_writer(&mut out, &LineRef::Add(record))?;
                    out.write_all(b"\n")?;
                    lines += 1;
                }
                let file = out
                    .into_inner()
                    .map_err(std::io::IntoInnerError::into_error)?;
                file.sync_all()?;
                drop(file);
                std::fs::rename(&tmp_path, &self.path)?;
                sync_dir(&self.path);
                Ok::<_, std::io::Error>(lines)
            })();
            match written {
                Ok(lines) => {
                    self.file = append(&self.path).map_err(|e| e.to_string())?;
                    self.lines = lines;
                    Ok(())
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&tmp_path);
                    Err(e.to_string())
                }
            }
        }
    }

    impl Drop for Log {
        fn drop(&mut self) {
            self.sync.close();
        }
    }

    impl Sync {
        /// `file` was written: the thread syncs it. A sync that failed
        /// before is said now.
        fn dirty(&self, file: &File) -> Result<(), String> {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(failed) = state.failed.take() {
                return Err(failed);
            }
            if state.dirty.is_none() {
                state.dirty = Some(file.try_clone().map_err(|e| e.to_string())?);
                self.wake.notify_one();
            }
            Ok(())
        }

        fn close(&self) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            self.wake.notify_one();
        }

        fn run(&self) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            loop {
                if let Some(file) = state.dirty.take() {
                    drop(state);
                    let synced = file.sync_data();
                    state = self
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Err(e) = synced {
                        state.failed = Some(format!("cannot sync it to the disk: {e}"));
                    }
                    continue;
                }
                if state.closed {
                    return;
                }
                state = self
                    .wake
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }

    fn append(path: &Path) -> std::io::Result<File> {
        OpenOptions::new().create(true).append(true).open(path)
    }

    /// The directory's entry of a file renamed into it, on the disk.
    fn sync_dir(path: &Path) {
        #[cfg(unix)]
        if let Some(dir) = path.parent()
            && let Ok(dir) = File::open(if dir.as_os_str().is_empty() {
                Path::new(".")
            } else {
                dir
            })
        {
            let _ = dir.sync_all();
        }
        #[cfg(not(unix))]
        let _ = path;
    }

    /// The records and the user a file holds. A line that does not read (the
    /// last one cut short as the application ended, say) is left out.
    fn read(file: impl BufRead) -> std::io::Result<Read> {
        let mut records: VecDeque<Record> = VecDeque::new();
        let mut user = None;
        let mut unread = 0;
        for line in file.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Line>(&line) {
                Ok(Line::Add(record)) => {
                    if !records.iter().any(|r| r.id == record.id) {
                        records.push_back(record);
                    }
                }
                Ok(Line::Sent(ids)) => {
                    let gone: std::collections::HashSet<&str> =
                        ids.iter().map(AsRef::as_ref).collect();
                    records.retain(|r| !gone.contains(r.id.as_ref()));
                }
                Ok(Line::User(u)) => user = u,
                Err(_) => unread += 1,
            }
        }
        if unread > 0 {
            tracing::warn!(
                "[WSE] the offline queue had {unread} line(s) that do not read: left out"
            );
        }
        Ok(Read { records, user })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn a_line_cut_short_is_left_out() {
            let text = concat!(
                r#"{"user":"ann"}"#,
                "\n",
                r#"{"add":{"id":"a","at":1,"owner":"ann","text":"{\"t\":\"x\"}"}}"#,
                "\n",
                r#"{"add":{"id":"b","at":2,"text":"{\"t\":\"y\"}"}}"#,
                "\n",
                r#"{"sent":["a"]}"#,
                "\n",
                r#"{"add":{"id":"c","at":3,"te"#,
            );
            let read = read(text.as_bytes()).unwrap();
            assert_eq!(read.user.as_deref(), Some("ann"));
            let ids: Vec<&str> = read.records.iter().map(|r| r.id.as_ref()).collect();
            assert_eq!(ids, ["b"]);
            assert_eq!(read.records[0].owner, None);
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn quiet() -> Report {
        Box::new(|_, _| {})
    }

    fn dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "wse-offline-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn ids(taken: &[(Box<str>, String)]) -> Vec<&str> {
        taken.iter().map(|(id, _)| id.as_ref()).collect()
    }

    #[test]
    fn what_is_kept_outlives_the_client_until_it_went() {
        let dir = dir();
        let path = dir.join("queue");
        let offline = Offline::in_file(&path);
        {
            let store = Store::open(&offline, quiet()).unwrap();
            store.take_for(Some("ann"));
            store.release();
            for id in ["1", "2", "3"] {
                assert_eq!(store.keep(id, format!("m{id}")).unwrap(), None);
            }
        }
        // The application ended and starts again.
        let store = Store::open(&offline, quiet()).unwrap();
        assert_eq!(store.len(), 3);
        let taken = store.take_for(Some("ann"));
        assert_eq!(ids(&taken), ["1", "2", "3"]);
        // While a connection takes them, a send goes to it.
        assert_eq!(store.keep("4", "m4".into()).unwrap().as_deref(), Some("m4"));
        store.sent(&["1".into(), "2".into()]);
        drop(store);
        let store = Store::open(&offline, quiet()).unwrap();
        assert_eq!(ids(&store.take_for(Some("ann"))), ["3"]);
        drop(store);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn another_users_connection_drops_what_was_kept_for_the_last() {
        let store = Store::open(&Offline::in_memory(), quiet()).unwrap();
        // Before any connection: for whoever connects.
        store.keep("anyone", "a".into()).unwrap();
        store.take_for(Some("ann"));
        store.release();
        store.keep("anns", "b".into()).unwrap();
        let taken = store.take_for(Some("bob"));
        assert_eq!(ids(&taken), ["anyone"]);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn it_holds_as_many_as_it_may() {
        let mut offline = Offline::in_memory();
        offline.max_messages = 2;
        let store = Store::open(&offline, quiet()).unwrap();
        store.keep("1", String::new()).unwrap();
        store.keep("2", String::new()).unwrap();
        assert!(matches!(store.keep("3", String::new()), Err(Error::Full)));
    }

    #[test]
    fn what_ran_out_is_dropped() {
        let mut offline = Offline::in_memory();
        offline.max_age = Duration::ZERO;
        let store = Store::open(&offline, quiet()).unwrap();
        store.keep("1", String::new()).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert!(store.take_for(None).is_empty());
    }

    #[test]
    fn the_file_is_one_clients_at_a_time() {
        let dir = dir();
        let offline = Offline::in_file(dir.join("queue"));
        let first = Store::open(&offline, quiet()).unwrap();
        let second = Store::open(&offline, quiet());
        assert!(
            matches!(&second, Err(Error::Config(why)) if why.contains("another client's")),
            "{:?}",
            second.err()
        );
        drop(first);
        Store::open(&offline, quiet()).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_file_is_written_over_once_what_went_outweighs_what_is_kept() {
        let dir = dir();
        let path = dir.join("queue");
        let store = Store::open(&Offline::in_file(&path), quiet()).unwrap();
        for round in 0..100 {
            let id = format!("{round}");
            store.keep(&id, "x".repeat(100)).unwrap();
            store.take_for(None);
            store.sent(&[id.into()]);
            store.release();
        }
        let lines = std::fs::read_to_string(&path).unwrap().lines().count();
        assert!(lines < 80, "{lines} lines");
        drop(store);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_failure_is_reported() {
        let dir = dir();
        let path = dir.join("queue");
        let failures = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&failures);
        let report: Report = Box::new(move |operation, why| {
            seen.lock().unwrap().push((operation, why));
        });
        let store = Store::open(&Offline::in_file(&path), report).unwrap();
        // The directory goes: the next rewrite cannot be written.
        std::fs::remove_dir_all(&dir).unwrap();
        for round in 0..100 {
            let id = format!("{round}");
            let _ = store.keep(&id, String::new());
            store.take_for(None);
            store.sent(&[id.into()]);
            store.release();
        }
        assert!(!failures.lock().unwrap().is_empty());
    }
}
