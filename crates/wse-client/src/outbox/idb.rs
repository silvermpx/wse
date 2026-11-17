//! The outbox in IndexedDB, in the browser: the database the TypeScript
//! client keeps, version 3 -- `producers` (key `producer`: `next`,
//! `written`, `node`, `user`) and `outbox` (key `[producer, s]`: `id`, `at`,
//! `until`, `owned`, `json`, the message without its number).
//!
//! A tab's outbox is one producer, its while the tab holds the producer's
//! Web Lock; the browser lets go of the lock when the tab goes, however it
//! goes. At open the tab takes over a producer whose lock is free and which
//! holds messages (a closed or crashed tab's, or the page it reloads), else
//! it makes a new one. The changes of a batch go in one transaction, one
//! transaction at a time (the browsers' relaxed durability: past a crash of
//! the tab or the browser, not a power cut); a failed one is said, and the
//! outbox writes the database anew once it can.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;

use js_sys::{Array, Function, Object, Promise, Reflect};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    IdbDatabase, IdbFactory, IdbKeyRange, IdbObjectStoreParameters, IdbRequest, IdbTransaction,
    IdbTransactionMode,
};

use super::{LineRef, Read, Record, Report, Wake, new_id, numbered, unnumbered};

const VERSION: u32 = 3;
const PRODUCERS: &str = "producers";
const OUTBOX: &str = "outbox";

/// What a JavaScript error says: a `DOMException`'s name and message.
fn describe(value: &JsValue) -> String {
    let field = |name: &str| {
        Reflect::get(value, &JsValue::from_str(name))
            .ok()
            .and_then(|v| v.as_string())
    };
    match (field("name"), field("message")) {
        (Some(name), Some(message)) => format!("{name}: {message}"),
        _ => value.as_string().unwrap_or_else(|| format!("{value:?}")),
    }
}

/// The request's result, once it succeeded.
async fn result(request: &IdbRequest) -> Result<JsValue, String> {
    let done = Promise::new(&mut |resolve, reject| {
        request.set_onsuccess(Some(&resolve));
        request.set_onerror(Some(&reject));
    });
    let outcome = JsFuture::from(done).await;
    request.set_onsuccess(None);
    request.set_onerror(None);
    match outcome {
        Ok(_) => request.result().map_err(|e| describe(&e)),
        Err(_) => Err(match request.error() {
            Ok(Some(error)) => describe(&error.into()),
            _ => "the request failed".to_owned(),
        }),
    }
}

/// The transaction, committed.
async fn committed(tx: &IdbTransaction) -> Result<(), String> {
    let done = Promise::new(&mut |resolve, reject| {
        tx.set_oncomplete(Some(&resolve));
        tx.set_onabort(Some(&reject));
    });
    match JsFuture::from(done).await {
        Ok(_) => Ok(()),
        Err(_) => Err(tx.error().map_or_else(
            || "the transaction was aborted".to_owned(),
            |e| describe(&e.into()),
        )),
    }
}

fn global(name: &str) -> Option<JsValue> {
    Reflect::get(&js_sys::global(), &JsValue::from_str(name))
        .ok()
        .filter(|v| !v.is_undefined() && !v.is_null())
}

/// The browser's Web Locks, where it has them (HTTPS).
fn locks() -> Option<JsValue> {
    let locks = Reflect::get(&global("navigator")?, &JsValue::from_str("locks")).ok()?;
    (!locks.is_undefined() && !locks.is_null()).then_some(locks)
}

/// A Web Lock taken at once and held until its release is called; None:
/// another holds it.
async fn try_lock(name: &str) -> Result<Option<Function>, String> {
    let locks = locks().ok_or_else(no_locks)?;
    let request: Function = Reflect::get(&locks, &JsValue::from_str("request"))
        .and_then(JsCast::dyn_into)
        .map_err(|e| describe(&e))?;
    let options = Object::new();
    let _ = Reflect::set(&options, &JsValue::from_str("ifAvailable"), &JsValue::TRUE);
    let taken = Promise::new(&mut |taken, _| {
        // Called once: with the lock, or null when another holds it.
        let callback = Closure::once_into_js(move |lock: JsValue| -> JsValue {
            if lock.is_null() {
                let _ = taken.call1(&JsValue::NULL, &JsValue::NULL);
                return JsValue::NULL;
            }
            let mut release = JsValue::UNDEFINED;
            let held = Promise::new(&mut |resolve, _| release = resolve.into());
            let _ = taken.call1(&JsValue::NULL, &release);
            held.into()
        });
        let _ = request.call3(&locks, &JsValue::from_str(name), &options, &callback);
    });
    let release = JsFuture::from(taken).await.map_err(|e| describe(&e))?;
    Ok(release.dyn_into::<Function>().ok())
}

fn no_locks() -> String {
    "no Web Locks here (a page needs HTTPS for them): the outbox stays in memory".to_owned()
}

fn lock_name(database: &str, producer: &str) -> String {
    format!("wse-outbox:{database}:{producer}")
}

/// The key of message `s` of `producer`.
fn key(producer: &str, s: f64) -> JsValue {
    Array::of2(&JsValue::from_str(producer), &JsValue::from_f64(s)).into()
}

/// The messages of `producer` from `from` to `to`.
fn range(producer: &str, from: f64, to: f64) -> Result<IdbKeyRange, String> {
    IdbKeyRange::bound(&key(producer, from), &key(producer, to)).map_err(|e| describe(&e))
}

/// A number of the database, as JavaScript keeps it (whole up to 2^53).
#[allow(clippy::cast_precision_loss)]
fn float(n: u64) -> f64 {
    n as f64
}

fn number(n: u64) -> JsValue {
    JsValue::from_f64(float(n))
}

/// A field of `value` that is a whole number, not below zero.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn count(value: &JsValue, name: &str) -> Option<u64> {
    Reflect::get(value, &JsValue::from_str(name))
        .ok()?
        .as_f64()
        .filter(|n| n.is_finite() && *n >= 0.0 && n.fract() == 0.0)
        .map(|n| n as u64)
}

fn text(value: &JsValue, name: &str) -> Option<String> {
    Reflect::get(value, &JsValue::from_str(name))
        .ok()?
        .as_string()
}

/// A producer, as the database keeps it.
struct Meta {
    producer: String,
    next: u64,
    written: u64,
    node: Option<String>,
    user: Option<String>,
}

impl Meta {
    fn of(value: &JsValue) -> Option<Self> {
        Some(Self {
            producer: text(value, "producer")?,
            next: count(value, "next").unwrap_or(1),
            written: count(value, "written").unwrap_or(0),
            node: text(value, "node"),
            user: text(value, "user"),
        })
    }

    fn to_js(&self) -> JsValue {
        let meta = Object::new();
        let set = |name: &str, value: &JsValue| {
            let _ = Reflect::set(&meta, &JsValue::from_str(name), value);
        };
        let or_null = |value: Option<&str>| value.map_or(JsValue::NULL, JsValue::from_str);
        set("producer", &JsValue::from_str(&self.producer));
        set("next", &number(self.next));
        set("written", &number(self.written));
        set("node", &or_null(self.node.as_deref()));
        set("user", &or_null(self.user.as_deref()));
        meta.into()
    }
}

/// A message kept, as the database keeps it.
fn row(producer: &str, record: &Record) -> JsValue {
    let row = Object::new();
    let set = |name: &str, value: &JsValue| {
        let _ = Reflect::set(&row, &JsValue::from_str(name), value);
    };
    set("producer", &JsValue::from_str(producer));
    set("s", &number(record.s));
    set("id", &JsValue::from_str(&record.id));
    set("at", &number(record.at));
    set("until", &number(record.until));
    set("owned", &JsValue::from_bool(record.owned));
    if let Some(requires) = &record.requires {
        let topics = Array::new();
        for topic in requires {
            topics.push(&JsValue::from_str(topic));
        }
        set("requires", &topics);
    }
    set("json", &JsValue::from_str(&unnumbered(&record.text)));
    row.into()
}

/// The topics a kept message requires: Some(None) when it says none; None
/// when what it says is not topics (the row is not taken).
fn requires_of(row: &JsValue) -> Option<Option<Box<[String]>>> {
    let value = Reflect::get(row, &JsValue::from_str("requires")).ok()?;
    if value.is_undefined() || value.is_null() {
        return Some(None);
    }
    let topics: Vec<String> = Array::from(&value)
        .iter()
        .map(|t| t.as_string().filter(|t| !t.is_empty()))
        .collect::<Option<_>>()?;
    (!topics.is_empty()).then(|| Some(topics.into_boxed_slice()))
}

/// A message kept, as read: numbered again as it goes.
fn record_of(row: &JsValue) -> Option<Record> {
    let json = text(row, "json")?;
    if !json.trim_start().starts_with('{') {
        return None;
    }
    let s = count(row, "s")?;
    Some(Record {
        s,
        id: text(row, "id")?.into(),
        at: count(row, "at")?,
        until: count(row, "until")?,
        owned: Reflect::get(row, &JsValue::from_str("owned"))
            .ok()?
            .as_bool()?,
        requires: requires_of(row)?,
        size: json.len(),
        text: numbered(s, &json).into(),
        tag: 0,
    })
}

async fn open_database(name: &str) -> Result<IdbDatabase, String> {
    let factory: IdbFactory = global("indexedDB")
        .and_then(|v| v.dyn_into().ok())
        .ok_or_else(|| "no IndexedDB here: the outbox stays in memory".to_owned())?;
    let request = factory
        .open_with_u32(name, VERSION)
        .map_err(|e| describe(&e))?;
    let upgrade = {
        let request = request.clone();
        Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            let Ok(db) = request.result().and_then(JsCast::dyn_into::<IdbDatabase>) else {
                return;
            };
            let stores = db.object_store_names();
            if !stores.contains(PRODUCERS) {
                let parameters = IdbObjectStoreParameters::new();
                parameters.set_key_path(&JsValue::from_str("producer"));
                let _ = db.create_object_store_with_optional_parameters(PRODUCERS, &parameters);
            }
            if !stores.contains(OUTBOX) {
                let parameters = IdbObjectStoreParameters::new();
                parameters.set_key_path(&Array::of2(&"producer".into(), &"s".into()));
                let _ = db.create_object_store_with_optional_parameters(OUTBOX, &parameters);
            }
        })
    };
    request.set_onupgradeneeded(Some(upgrade.as_ref().unchecked_ref()));
    let opened = result(&request).await;
    // Opened, or not: no upgrade comes any more.
    request.set_onupgradeneeded(None);
    drop(upgrade);
    opened?.dyn_into::<IdbDatabase>().map_err(|e| describe(&e))
}

fn readwrite(db: &IdbDatabase, stores: &[&str]) -> Result<IdbTransaction, String> {
    let names = Array::new();
    for store in stores {
        names.push(&JsValue::from_str(store));
    }
    db.transaction_with_str_sequence_and_mode(&names, IdbTransactionMode::Readwrite)
        .map_err(|e| describe(&e))
}

/// What the database holds of `producer` now, its row and its messages
/// read at once; None when it is gone.
async fn read(db: &IdbDatabase, producer: &str) -> Result<Option<(Meta, Read)>, String> {
    let names = Array::new();
    names.push(&JsValue::from_str(PRODUCERS));
    names.push(&JsValue::from_str(OUTBOX));
    let tx = db
        .transaction_with_str_sequence(&names)
        .map_err(|e| describe(&e))?;
    let row = tx
        .object_store(PRODUCERS)
        .and_then(|store| store.get(&JsValue::from_str(producer)))
        .map_err(|e| describe(&e))?;
    let all = tx
        .object_store(OUTBOX)
        .map_err(|e| describe(&e))?
        .get_all_with_key(&range(producer, 0.0, f64::INFINITY)?.into())
        .map_err(|e| describe(&e))?;
    let row = result(&row).await?;
    let rows: Array = result(&all).await?.unchecked_into();
    let Some(meta) = Meta::of(&row) else {
        return Ok(None);
    };
    let mut next = meta.next.max(meta.written + 1).max(1);
    let mut records = VecDeque::with_capacity(rows.length() as usize);
    for row in rows.iter() {
        let Some(record) = record_of(&row) else {
            continue;
        };
        next = next.max(record.s + 1);
        records.push_back(record);
    }
    let read = Read {
        producer: meta.producer.as_str().into(),
        node: meta.node.as_deref().map(Into::into),
        written: meta.written,
        next,
        user: meta.user.clone(),
        records,
    };
    Ok(Some((meta, read)))
}

/// Takes over a producer no tab holds and which holds messages (one that
/// holds none goes), else makes one: it, its lock's release, what it holds.
async fn take_over(db: &IdbDatabase, name: &str) -> Result<(Meta, Function, Read), String> {
    let tx = db
        .transaction_with_str(PRODUCERS)
        .map_err(|e| describe(&e))?;
    let all = tx
        .object_store(PRODUCERS)
        .and_then(|store| store.get_all())
        .map_err(|e| describe(&e))?;
    let metas: Array = result(&all).await?.unchecked_into();
    for value in metas.iter() {
        let Some(listed) = Meta::of(&value) else {
            continue;
        };
        let Some(release) = try_lock(&lock_name(name, &listed.producer)).await? else {
            continue;
        };
        // Read again now that it is this tab's: the tab that held it may
        // have used it meanwhile (its user named, a message written).
        let (meta, read) = match read(db, &listed.producer).await {
            Ok(Some(taken)) => taken,
            Ok(None) => {
                let _ = release.call0(&JsValue::NULL);
                continue;
            }
            Err(why) => {
                let _ = release.call0(&JsValue::NULL);
                return Err(why);
            }
        };
        if read.records.is_empty() {
            // Nobody holds it and nothing waits in it: it goes.
            let gone = readwrite(db, &[PRODUCERS, OUTBOX]).and_then(|tx| {
                let delete = |store: &str, key: &JsValue| {
                    tx.object_store(store)
                        .and_then(|s| s.delete(key))
                        .map_err(|e| describe(&e))
                };
                delete(PRODUCERS, &JsValue::from_str(&meta.producer))?;
                delete(OUTBOX, &range(&meta.producer, 0.0, f64::INFINITY)?.into())?;
                Ok(tx)
            });
            if let Ok(tx) = gone {
                let _ = committed(&tx).await;
            }
            let _ = release.call0(&JsValue::NULL);
            continue;
        }
        let meta = Meta {
            next: read.next,
            ..meta
        };
        return Ok((meta, release, read));
    }
    // None to take over: a producer of this page's own.
    let meta = Meta {
        producer: new_id(),
        next: 1,
        written: 0,
        node: None,
        user: None,
    };
    let release = try_lock(&lock_name(name, &meta.producer))
        .await?
        .ok_or_else(|| "a new producer is held already".to_owned())?;
    let made = readwrite(db, &[PRODUCERS]).and_then(|tx| {
        tx.object_store(PRODUCERS)
            .and_then(|store| store.put(&meta.to_js()))
            .map_err(|e| describe(&e))?;
        Ok(tx)
    });
    let made = match made {
        Ok(tx) => committed(&tx).await,
        Err(why) => Err(why),
    };
    if let Err(why) = made {
        let _ = release.call0(&JsValue::NULL);
        return Err(why);
    }
    let read = Read {
        producer: meta.producer.as_str().into(),
        node: None,
        written: 0,
        next: 1,
        user: None,
        records: VecDeque::new(),
    };
    Ok((meta, release, read))
}

/// Opens the database `name` for this tab: its log, and what it holds.
pub(super) async fn open(name: &str, report: Report, wake: Wake) -> Result<(Log, Read), String> {
    if locks().is_none() {
        return Err(no_locks());
    }
    let db = open_database(name).await?;
    match take_over(&db, name).await {
        Ok((meta, release, read)) => Ok((Log::new(db, meta, release, report, wake), read)),
        Err(why) => {
            db.close();
            Err(why)
        }
    }
}

/// A step of a batch.
enum Op {
    Put(JsValue),
    Delete(u64),
    DeleteTo(u64),
    Rewrite(Vec<JsValue>),
}

struct Inner {
    db: IdbDatabase,
    meta: Meta,
    /// The batch.
    ops: Vec<Op>,
    meta_changed: bool,
    /// Batches handed and not in a transaction yet.
    queued: Vec<Op>,
    queued_meta: bool,
    busy: bool,
    broken: bool,
    /// A transaction failed: the database lacks what it held, and is
    /// written anew before anything is added.
    stale: bool,
    failing: bool,
    closed: bool,
    /// The highest mark the database holds (that messages up to it may have
    /// gone), and who hears it rose.
    written: u64,
    wake: Wake,
    release: Function,
    report: Report,
    _versionchange: Option<Closure<dyn FnMut(web_sys::Event)>>,
}

impl Inner {
    fn fail(&mut self, why: String) {
        self.broken = true;
        self.stale = true;
        if !self.failing {
            self.failing = true;
            (self.report)("write", why);
        }
    }

    /// One transaction for `ops` and the producer.
    fn transaction(&self, ops: &[Op]) -> Result<IdbTransaction, String> {
        let tx = readwrite(&self.db, &[PRODUCERS, OUTBOX])?;
        let store = tx.object_store(OUTBOX).map_err(|e| describe(&e))?;
        let producer = self.meta.producer.as_str();
        let all = || range(producer, 0.0, f64::INFINITY);
        for op in ops {
            let done = match op {
                Op::Put(row) => store.put(row).map(drop),
                Op::Delete(s) => store.delete(&key(producer, float(*s))).map(drop),
                Op::DeleteTo(s) => store
                    .delete(&range(producer, 0.0, float(*s))?.into())
                    .map(drop),
                Op::Rewrite(rows) => store.delete(&all()?.into()).map(drop).and_then(|()| {
                    for row in rows {
                        store.put(row)?;
                    }
                    Ok(())
                }),
            };
            done.map_err(|e| describe(&e))?;
        }
        tx.object_store(PRODUCERS)
            .and_then(|s| s.put(&self.meta.to_js()))
            .map_err(|e| describe(&e))?;
        Ok(tx)
    }
}

/// What is queued, in one transaction; the transactions of one database run
/// in the order they were made.
fn commit(shared: &Rc<RefCell<Inner>>) {
    let mut inner = shared.borrow_mut();
    if inner.queued.is_empty() && !inner.queued_meta {
        return;
    }
    let ops = std::mem::take(&mut inner.queued);
    inner.queued_meta = false;
    let rewrites = ops.iter().any(|op| matches!(op, Op::Rewrite(_)));
    if inner.stale && !rewrites {
        return;
    }
    // The producer's row goes with each transaction: its mark with it.
    let mark = inner.meta.written;
    let tx = match inner.transaction(&ops) {
        Ok(tx) => tx,
        Err(why) => {
            inner.fail(why);
            return;
        }
    };
    inner.busy = true;
    drop(inner);
    let shared = Rc::clone(shared);
    wasm_bindgen_futures::spawn_local(async move {
        let outcome = committed(&tx).await;
        let mut inner = shared.borrow_mut();
        inner.busy = false;
        let mut rose = false;
        match outcome {
            Ok(()) => {
                if rewrites {
                    inner.stale = false;
                }
                inner.failing = false;
                rose = mark > inner.written;
                inner.written = inner.written.max(mark);
            }
            Err(why) => inner.fail(why),
        }
        let closed = inner.closed;
        let wake = Arc::clone(&inner.wake);
        drop(inner);
        // In the database: what waited for the mark may go.
        if rose {
            wake();
        }
        if !closed {
            commit(&shared);
        }
    });
}

/// The database, as the connection's task sees it: a batch of changes,
/// handed to a transaction.
pub(super) struct Log {
    /// The database compacts itself: no lines to count.
    pub(super) lines: usize,
    inner: Rc<RefCell<Inner>>,
}

impl Log {
    fn new(db: IdbDatabase, meta: Meta, release: Function, report: Report, wake: Wake) -> Self {
        let written = meta.written;
        let inner = Rc::new(RefCell::new(Inner {
            db: db.clone(),
            meta,
            ops: Vec::new(),
            meta_changed: false,
            queued: Vec::new(),
            queued_meta: false,
            busy: false,
            broken: false,
            stale: false,
            failing: false,
            closed: false,
            written,
            wake,
            release,
            report,
            _versionchange: None,
        }));
        // Another page deletes the database (its user signed out) or
        // upgrades it: this one lets it, and keeps what waits in memory.
        let weak = Rc::downgrade(&inner);
        let versionchange = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            if let Some(inner) = weak.upgrade() {
                let mut inner = inner.borrow_mut();
                inner.db.close();
                inner.fail("another page deleted or upgraded the database".to_owned());
            }
        });
        db.set_onversionchange(Some(versionchange.as_ref().unchecked_ref()));
        inner.borrow_mut()._versionchange = Some(versionchange);
        Self { lines: 0, inner }
    }

    pub(super) fn push(&mut self, line: &LineRef<'_>) {
        let mut inner = self.inner.borrow_mut();
        match line {
            LineRef::Ack(s) => inner.ops.push(Op::DeleteTo(*s)),
            LineRef::Drop(numbers) => inner.ops.extend(numbers.iter().map(|s| Op::Delete(*s))),
            LineRef::Written(n) => {
                inner.meta.written = *n;
                inner.meta_changed = true;
            }
            LineRef::User(user) => {
                inner.meta.user = user.map(str::to_owned);
                inner.meta_changed = true;
            }
            LineRef::Node(node) => {
                inner.meta.node = Some((*node).to_owned());
                inner.meta_changed = true;
            }
        }
    }

    pub(super) fn push_add(&mut self, record: &Record) {
        let mut inner = self.inner.borrow_mut();
        let row = row(&inner.meta.producer, record);
        inner.ops.push(Op::Put(row));
        if record.s >= inner.meta.next {
            inner.meta.next = record.s + 1;
            inner.meta_changed = true;
        }
    }

    /// Hands the batch to a transaction; its mark is the producer's row's.
    pub(super) fn flush(&mut self, _mark: u64) {
        let mut inner = self.inner.borrow_mut();
        if inner.ops.is_empty() && !inner.meta_changed {
            return;
        }
        let ops = std::mem::take(&mut inner.ops);
        inner.queued.extend(ops);
        inner.queued_meta |= std::mem::take(&mut inner.meta_changed);
        let busy = inner.busy;
        drop(inner);
        if !busy {
            commit(&self.inner);
        }
    }

    /// The batch is in what is written anew.
    pub(super) fn forget(&mut self) {
        self.inner.borrow_mut().ops.clear();
    }

    pub(super) fn take_broken(&self) -> bool {
        std::mem::take(&mut self.inner.borrow_mut().broken)
    }

    /// The highest mark the database holds.
    pub(super) fn written(&self) -> u64 {
        self.inner.borrow().written
    }

    /// The database is behind: it lacks what a failed transaction held, and
    /// messages do not wait for it until it is written anew.
    pub(super) fn behind(&self) -> bool {
        self.inner.borrow().stale
    }

    /// Writes the database anew, with what is kept now alone.
    pub(super) fn rewrite(
        &mut self,
        next: u64,
        written: u64,
        node: Option<&str>,
        user: Option<&str>,
        records: &VecDeque<Record>,
    ) {
        let mut inner = self.inner.borrow_mut();
        inner.meta.next = inner.meta.next.max(next);
        inner.meta.written = written;
        inner.meta.node = node.map(str::to_owned);
        inner.meta.user = user.map(str::to_owned);
        // What waits to be written is in it.
        inner.ops.clear();
        inner.meta_changed = false;
        let producer = inner.meta.producer.clone();
        inner.queued = vec![Op::Rewrite(
            records.iter().map(|r| row(&producer, r)).collect(),
        )];
        inner.queued_meta = true;
        let busy = inner.busy;
        drop(inner);
        if !busy {
            commit(&self.inner);
        }
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        {
            let mut inner = self.inner.borrow_mut();
            inner.closed = true;
            let ops = std::mem::take(&mut inner.ops);
            inner.queued.extend(ops);
            inner.queued_meta |= std::mem::take(&mut inner.meta_changed);
            // The last transaction is made now, behind any that runs: a
            // page that takes the producer over reads after it.
            inner.busy = false;
        }
        commit(&self.inner);
        let inner = self.inner.borrow();
        let _ = inner.release.call0(&JsValue::NULL);
        // Closes once its transactions are done.
        inner.db.close();
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    /// IndexedDB in this Node (fake-indexeddb, the repository's development
    /// dependency: `npm ci` first), and its Web Locks, each lock asked for
    /// held while `globalThis.__wseGate` is a promise.
    fn storage() {
        let install = Function::new_with_args(
            "root",
            r#"
            if (!globalThis.indexedDB) {
              const { createRequire } = process.getBuiltinModule('node:module');
              createRequire(root)('fake-indexeddb/auto');
            }
            const held = new Set();
            const waiting = new Map();
            const locks = {
              async request(name, options, callback) {
                if (typeof options === 'function') { callback = options; options = {}; }
                if (globalThis.__wseGate) { globalThis.__wseGateHit = true; await globalThis.__wseGate; }
                if (held.has(name)) {
                  if (options.ifAvailable) return callback(null);
                  await new Promise((resolve) => waiting.set(name, [...(waiting.get(name) ?? []), resolve]));
                }
                held.add(name);
                try { return await callback({ name, mode: 'exclusive' }); }
                finally { held.delete(name); waiting.get(name)?.shift()?.(); }
              },
            };
            Object.defineProperty(globalThis.navigator, 'locks', { value: locks, configurable: true });
            "#,
        );
        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../package.json");
        install
            .call1(&JsValue::NULL, &root.into())
            .expect("fake-indexeddb from the repository's node_modules");
    }

    fn global(name: &str) -> JsValue {
        Reflect::get(&js_sys::global(), &name.into()).unwrap_or(JsValue::UNDEFINED)
    }

    #[wasm_bindgen_test]
    async fn a_producer_is_taken_over_as_it_stands_once_its_lock_is_held() {
        storage();
        let name = format!("wse-test-takeover-{}", fastrand::u64(..));
        let quiet: Report = Arc::new(|_, _| {});
        let (mut first, _) = open(&name, Arc::clone(&quiet), Arc::new(|| {}))
            .await
            .unwrap();
        let json = r#"{"t":"order","p":{},"id":"alice-command"}"#;
        first.push_add(&Record {
            s: 1,
            id: "alice-command".into(),
            at: 0,
            until: u64::MAX,
            owned: false,
            requires: None,
            text: numbered(1, json).into(),
            size: json.len(),
            tag: 0,
        });
        first.flush(0);
        gloo_timers::future::sleep(std::time::Duration::from_millis(50)).await;

        // The next tab lists the producers, and asks for this one's lock
        // while this tab holds it still.
        let mut opened = None;
        let gate = js_sys::Promise::new(&mut |resolve, _| opened = Some(resolve));
        Reflect::set(&js_sys::global(), &"__wseGate".into(), &gate).unwrap();
        let next = open(&name, Arc::clone(&quiet), Arc::new(|| {}));
        let meanwhile = async {
            while global("__wseGateHit").is_undefined() {
                gloo_timers::future::sleep(std::time::Duration::from_millis(5)).await;
            }
            // Meanwhile this tab's connection names Alice, writes the message,
            // and the tab goes.
            first.push(&LineRef::User(Some("alice")));
            first.push(&LineRef::Written(1));
            first.flush(1);
            drop(first);
            Reflect::delete_property(&js_sys::global(), &"__wseGate".into()).unwrap();
            opened.take().unwrap().call0(&JsValue::NULL).unwrap();
        };
        let (taken, ()) = futures_util::future::join(next, meanwhile).await;
        let (_second, read) = taken.unwrap();
        assert_eq!(read.records.len(), 1);
        // Alice's, written: a connection of another user drops it.
        assert_eq!(read.user.as_deref(), Some("alice"));
        assert_eq!(read.written, 1);
    }
}
