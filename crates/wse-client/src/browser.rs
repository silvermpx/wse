//! The connection's task in the browser: the browser's WebSocket, its
//! events and timers, around the same session as everywhere else.
//!
//! The browser sends the page's cookies with the upgrade and no header, so
//! the server takes the client by its cookie. It answers the server's
//! protocol pings itself, so the client's heartbeat is a message.

use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use tokio::sync::{mpsc, watch};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use web_sys::{BinaryType, CloseEvent, Event as DomEvent, MessageEvent, WebSocket};
use web_time::Instant;

use crate::client::{Control, Queued, Shared, Status};
use crate::config::Config;
use crate::decode::Decoder;
use crate::error::Error;
use crate::frames::{self, Data};
use crate::handler::{Event, Handler, Paused};
use crate::policy::{First, Lost, Next, Policy};
use crate::security::{Keys, Sealing};
use crate::session::{self, Outgoing, Session};

/// Messages the application queued that go out in one turn.
const SEND_BATCH: usize = 256;
/// Events taken in one go when they came together.
const READ_BATCH: usize = 64;
/// The close code a browser may send of the normal ones.
const CLOSE_NORMAL: u16 = 1000;
/// Bytes the browser may hold unsent before the application's messages
/// wait: the browser's WebSocket never waits itself.
const MAX_BUFFERED: u32 = 1 << 20;
/// How often a full socket is looked at again.
const BUFFERED_CHECK: Duration = Duration::from_millis(20);

/// What the WebSocket tells.
enum Happened {
    Open,
    Text(ByteString),
    Binary(Bytes),
    Error,
    Close { code: u16, reason: String },
}

/// A browser WebSocket and its handlers, which report to `events`.
struct Socket {
    ws: WebSocket,
    events: mpsc::UnboundedReceiver<Happened>,
    _open: Closure<dyn FnMut(DomEvent)>,
    _message: Closure<dyn FnMut(MessageEvent)>,
    _error: Closure<dyn FnMut(DomEvent)>,
    _close: Closure<dyn FnMut(CloseEvent)>,
}

impl Socket {
    fn open(url: &str) -> Result<Self, Error> {
        let ws = WebSocket::new(url).map_err(|e| Error::Connect(describe(&e)))?;
        ws.set_binary_type(BinaryType::Arraybuffer);
        let (tx, events) = mpsc::unbounded_channel();
        let on_open = {
            let tx = tx.clone();
            Closure::<dyn FnMut(DomEvent)>::new(move |_| {
                let _ = tx.send(Happened::Open);
            })
        };
        let on_message = {
            let tx = tx.clone();
            Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
                let data = event.data();
                let happened = match data.as_string() {
                    Some(text) => Happened::Text(ByteString::from(text)),
                    None => match data.dyn_into::<js_sys::ArrayBuffer>() {
                        Ok(buffer) => {
                            Happened::Binary(Bytes::from(js_sys::Uint8Array::new(&buffer).to_vec()))
                        }
                        Err(_) => return,
                    },
                };
                let _ = tx.send(happened);
            })
        };
        let on_error = {
            let tx = tx.clone();
            Closure::<dyn FnMut(DomEvent)>::new(move |_| {
                let _ = tx.send(Happened::Error);
            })
        };
        let on_close = Closure::<dyn FnMut(CloseEvent)>::new(move |event: CloseEvent| {
            let _ = tx.send(Happened::Close {
                code: event.code(),
                reason: event.reason(),
            });
        });
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        Ok(Self {
            ws,
            events,
            _open: on_open,
            _message: on_message,
            _error: on_error,
            _close: on_close,
        })
    }

    fn send_text(&self, text: &str) -> Result<(), Error> {
        self.ws
            .send_with_str(text)
            .map_err(|e| Error::Connect(describe(&e)))
    }

    fn send_binary(&self, data: &[u8]) -> Result<(), Error> {
        self.ws
            .send_with_u8_array(data)
            .map_err(|e| Error::Connect(describe(&e)))
    }

    fn close(&self, reason: &str) {
        let _ = self.ws.close_with_code_and_reason(CLOSE_NORMAL, reason);
    }

    /// Whether the browser holds so much unsent that more should wait.
    fn is_full(&self) -> bool {
        self.ws.buffered_amount() >= MAX_BUFFERED
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // The handlers go with the socket: nothing calls into them after.
        self.ws.set_onopen(None);
        self.ws.set_onmessage(None);
        self.ws.set_onerror(None);
        self.ws.set_onclose(None);
        let _ = self.ws.close();
    }
}

fn describe(error: &wasm_bindgen::JsValue) -> String {
    error
        .dyn_ref::<js_sys::Error>()
        .map(|e| String::from(e.message()))
        .or_else(|| error.as_string())
        .unwrap_or_else(|| "the browser refused the WebSocket".to_owned())
}

/// Waits until `deadline`; forever without one.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep(deadline.saturating_duration_since(Instant::now())).await,
        None => std::future::pending().await,
    }
}

/// The browser's timers take at most 2^31 - 1 ms (24.8 days); a longer
/// wait is several.
async fn sleep(duration: Duration) {
    const MAX_MS: u128 = i32::MAX as u128;
    let mut left = duration.as_millis();
    loop {
        let ms = left.min(MAX_MS);
        gloo_timers::future::TimeoutFuture::new(ms as u32).await;
        left -= ms;
        if left == 0 {
            return;
        }
    }
}

/// A timer kept across turns of the loop, made again only when its moment
/// changes: a browser timer costs a JavaScript callback each.
struct Timer {
    at: Option<Instant>,
    sleep: Pin<Box<dyn std::future::Future<Output = ()>>>,
}

impl Timer {
    fn new() -> Self {
        Self {
            at: None,
            sleep: Box::pin(std::future::pending()),
        }
    }

    /// Fires at `at`; never without one.
    fn set(&mut self, at: Option<Instant>) {
        if self.at != at {
            self.at = at;
            self.sleep = Box::pin(sleep_until(at));
        }
    }

    async fn fired(&mut self) {
        (&mut self.sleep).await;
        self.at = None;
        self.sleep = Box::pin(std::future::pending());
    }
}

pub(crate) struct Driver<H> {
    config: Config,
    shared: Arc<Shared>,
    frames: mpsc::Receiver<Queued>,
    control: mpsc::UnboundedReceiver<Control>,
    status: watch::Sender<Status>,
    decoder: Decoder,
    keys: Arc<Keys>,
    session: Session<H>,
    policy: Policy,
    /// The reasons the application paused the client for: no connection
    /// while any holds.
    paused: BTreeSet<String>,
}

impl<H: Handler> Driver<H> {
    pub(crate) fn new(
        config: Config,
        handler: crate::delivery::Delivered<H>,
        shared: Arc<Shared>,
        keys: Arc<Keys>,
        frames: mpsc::Receiver<Queued>,
        control: mpsc::UnboundedReceiver<Control>,
        status: watch::Sender<Status>,
    ) -> Self {
        let mut session = Session::new(
            handler,
            Arc::clone(&shared),
            Arc::clone(&keys),
            &config.topics,
            config.recovery_timeout,
            config.idle_timeout,
        );
        session.committed = config.committed.clone();
        session.status = Some(status.clone());
        let paused = config.paused.iter().cloned().collect();
        Self {
            decoder: Decoder::new(
                config.max_message_size,
                config.format == crate::config::Format::Msgpack,
            ),
            keys,
            config,
            shared,
            frames,
            control,
            status,
            session,
            policy: Policy::default(),
            paused,
        }
    }

    pub(crate) async fn run(mut self, mut first: First) {
        // The outbox's database opens in its own time: what it holds waits
        // for the first connection, and what is sent meanwhile after it. A
        // close meanwhile does not wait for it (an older page may hold its
        // upgrade); what else the application asks meanwhile is done once
        // it is open.
        let mut asked = Vec::new();
        let mut closed = false;
        if let Some(outbox) = &mut self.session.outbox {
            let opening = outbox.open_store();
            let mut opening = std::pin::pin!(opening);
            loop {
                tokio::select! {
                    () = &mut opening => break,
                    control = self.control.recv() => match control {
                        None | Some(Control::Close) => {
                            closed = true;
                            break;
                        }
                        Some(control) => asked.push(control),
                    },
                }
            }
        }
        for control in asked {
            self.asked(control);
        }
        let end = if closed {
            None
        } else {
            loop {
                if !self.paused.is_empty() {
                    self.status.send_replace(Status::Paused);
                    self.session.handler.event(Event::Paused(Paused {
                        reasons: self.paused.iter().cloned().collect(),
                    }));
                    if !self.wait_paused().await {
                        break None;
                    }
                    // Back from a pause: at once, the counts start over.
                    self.policy.connected();
                }
                let lost = match self.open().await {
                    None => Lost::Shutdown,
                    Some(Ok(socket)) => self.serve(socket, &mut first).await,
                    Some(Err(Some(error))) => Lost::Failed(error),
                    Some(Err(None)) => Lost::Paused,
                };
                self.session.close();
                if matches!(lost, Lost::Paused) {
                    continue;
                }
                // What the application sent and the lost connection did not
                // write goes out on the next one, once the server takes it.
                match self.policy.after(lost, &self.config, &mut first) {
                    Next::Stop(end) => break end,
                    Next::Retry(disconnected) => {
                        let retry_in = disconnected.retry_in;
                        self.status.send_replace(Status::Reconnecting);
                        self.session
                            .handler
                            .event(Event::Disconnected(disconnected));
                        if !self.wait(retry_in).await {
                            break None;
                        }
                    }
                }
            }
        };
        // What was sent before the close goes to the outbox.
        self.frames.close();
        while let Ok(frame) = self.frames.try_recv() {
            self.session.queue(frame);
        }
        self.session.finish();
        self.session.outbox = None;
        // A send that waits for a place in the outbox waits no longer.
        if let Some(room) = &self.shared.room {
            room.close();
        }
        self.shared
            .ready
            .store(false, std::sync::atomic::Ordering::Release);
        self.session.handler.event(Event::Closed(end.clone()));
        self.status.send_replace(Status::Closed(end));
    }

    /// What the application asked while the outbox's database opened.
    fn asked(&mut self, control: Control) {
        match control {
            Control::Close | Control::Reconnect => {}
            Control::Pause(reason) => {
                self.paused.insert(reason);
            }
            Control::Resume(reason) => {
                self.paused.remove(&reason);
            }
            Control::Subscribe(topics) => self.session.subscribe(topics),
            Control::Unsubscribe(topics) => self.session.unsubscribe(topics),
            Control::Drain => self.session.handler.drain(),
            Control::Clear => self.session.catch_up_clears(),
        }
    }

    /// Opens a socket; `None` when the application closed the client
    /// meanwhile, `Some(Err(None))` when it paused it.
    async fn open(&mut self) -> Option<Result<Socket, Option<Error>>> {
        let deadline = Instant::now() + self.config.connect_timeout;
        if let Some(provider) = self.config.token_provider.clone() {
            // Asked to refresh the page's cookie; the client may close, or
            // give up on it, meanwhile.
            let fetch = provider.token();
            let mut fetch = std::pin::pin!(fetch);
            let fetched = loop {
                tokio::select! {
                    biased;
                    control = self.control.recv() => match control {
                        None | Some(Control::Close) => return None,
                        Some(Control::Reconnect) => {}
                        Some(Control::Pause(reason)) => {
                            self.paused.insert(reason);
                            return Some(Err(None));
                        }
                        Some(Control::Resume(reason)) => {
                            self.paused.remove(&reason);
                        }
                        Some(Control::Subscribe(topics)) => self.session.subscribe(topics),
                        Some(Control::Unsubscribe(topics)) => self.session.unsubscribe(topics),
                        Some(Control::Drain) => self.session.handler.drain(),
                        Some(Control::Clear) => self.session.catch_up_clears(),
                    },
                    // The outbox takes what is sent meanwhile.
                    Some(frame) = self.frames.recv(), if self.session.keeps() => self.session.queue(frame),
                    token = &mut fetch => break token,
                    _ = sleep_until(Some(deadline)) => {
                        return Some(Err(Some(Error::Timeout(format!(
                            "no token from the token provider within {:?}",
                            self.config.connect_timeout
                        )))));
                    }
                }
            };
            if let Err(e) = fetched {
                return Some(Err(Some(Error::Connect(format!("token provider: {e}")))));
            }
        }
        let mut socket = match Socket::open(&self.config.url(self.keys.e2e)) {
            Ok(socket) => socket,
            Err(error) => return Some(Err(Some(error))),
        };
        loop {
            tokio::select! {
                biased;
                control = self.control.recv() => match control {
                    None | Some(Control::Close) => return None,
                    Some(Control::Reconnect) => {}
                    Some(Control::Pause(reason)) => {
                        self.paused.insert(reason);
                        socket.close("paused");
                        return Some(Err(None));
                    }
                    Some(Control::Resume(reason)) => {
                        self.paused.remove(&reason);
                    }
                    Some(Control::Subscribe(topics)) => self.session.subscribe(topics),
                    Some(Control::Unsubscribe(topics)) => self.session.unsubscribe(topics),
                    Some(Control::Drain) => self.session.handler.drain(),
                    Some(Control::Clear) => self.session.catch_up_clears(),
                },
                // The outbox takes what is sent meanwhile.
                Some(frame) = self.frames.recv(), if self.session.keeps() => self.session.queue(frame),
                // What is held for a gap is not held past its wait.
                () = sleep_until(self.session.next_deadline()) => {
                    self.session.on_deadline(Instant::now());
                }
                happened = socket.events.recv() => match happened {
                    Some(Happened::Open) => return Some(Ok(socket)),
                    Some(Happened::Close { code, reason }) => {
                        return Some(Err(Some(Error::Connect(format!(
                            "closed before it opened ({code}{}{reason})",
                            if reason.is_empty() { "" } else { ": " }
                        )))));
                    }
                    // An attempt that failed: a WebSocket that says so with
                    // no close after (Node's) is not waited out.
                    Some(Happened::Error) => {
                        return Some(Err(Some(Error::Connect(
                            "the connection failed before it opened".to_owned(),
                        ))));
                    }
                    Some(_) => {}
                    None => {
                        return Some(Err(Some(Error::Connect("the socket is gone".to_owned()))));
                    }
                },
                _ = sleep_until(Some(deadline)) => {
                    return Some(Err(Some(Error::Timeout(format!(
                        "no upgrade within {:?}",
                        self.config.connect_timeout
                    )))));
                }
            }
        }
    }

    /// Waits while paused, doing what the application asks meanwhile; what
    /// is held for a gap waits too. False when it closed the client.
    async fn wait_paused(&mut self) -> bool {
        while !self.paused.is_empty() {
            tokio::select! {
                control = self.control.recv() => match control {
                    None | Some(Control::Close) => return false,
                    Some(Control::Pause(reason)) => {
                        self.paused.insert(reason);
                    }
                    Some(Control::Resume(reason)) => {
                        self.paused.remove(&reason);
                    }
                    Some(Control::Reconnect) => {}
                    Some(Control::Subscribe(topics)) => self.session.subscribe(topics),
                    Some(Control::Unsubscribe(topics)) => self.session.unsubscribe(topics),
                    Some(Control::Drain) => self.session.handler.drain(),
                    Some(Control::Clear) => self.session.catch_up_clears(),
                },
                // The outbox takes what is sent meanwhile; what waited too
                // long to be sent goes on time.
                Some(frame) = self.frames.recv(), if self.session.keeps() => self.session.queue(frame),
                () = sleep_until(self.session.outbox_deadline()) => {
                    self.session.expire_outbox(Instant::now());
                }
            }
        }
        true
    }

    /// Waits before the next attempt, doing what the application asks
    /// meanwhile. False when it closed the client; a pause ends the wait.
    async fn wait(&mut self, delay: Duration) -> bool {
        let until = Instant::now() + delay;
        loop {
            tokio::select! {
                biased;
                control = self.control.recv() => match control {
                    None | Some(Control::Close) => return false,
                    Some(Control::Reconnect) => return true,
                    Some(Control::Pause(reason)) => {
                        self.paused.insert(reason);
                        return true;
                    }
                    Some(Control::Resume(reason)) => {
                        self.paused.remove(&reason);
                    }
                    Some(Control::Subscribe(topics)) => self.session.subscribe(topics),
                    Some(Control::Unsubscribe(topics)) => self.session.unsubscribe(topics),
                    Some(Control::Drain) => self.session.handler.drain(),
                    Some(Control::Clear) => self.session.catch_up_clears(),
                },
                // The outbox takes what is sent meanwhile.
                Some(frame) = self.frames.recv(), if self.session.keeps() => self.session.queue(frame),
                _ = sleep_until(Some(until)) => return true,
                () = sleep_until(self.session.next_deadline()) => {
                    self.session.on_deadline(Instant::now());
                }
            }
        }
    }

    /// One connection, until it ends.
    async fn serve(&mut self, mut socket: Socket, first: &mut First) -> Lost {
        let sealing = match self.keys.offer() {
            Ok(sealing) => sealing,
            Err(error) => return Lost::Fatal(error),
        };
        let hello = session::client_hello(
            self.config.compression,
            sealing.as_ref().and_then(Sealing::hello),
            self.session.outbox_hello(),
        );
        self.session.open(sealing);
        self.session.to_write.push(Outgoing::Handshake(hello));
        let ready_by = Instant::now() + self.config.ready_timeout;
        let mut last_seen = Instant::now();
        let mut beat = Timer::new();
        beat.set(Some(Instant::now() + self.config.heartbeat));
        let mut wake = Timer::new();
        let mut recheck = Timer::new();

        loop {
            if let Some(error) = self.session.fatal.take() {
                // Nothing the failed frame asked for goes out.
                self.session.to_write.clear();
                socket.close("client error");
                return Lost::Fatal(error);
            }
            self.session.before_write();
            if let Err(lost) = self.write(&socket) {
                return lost;
            }
            if std::mem::take(&mut self.session.became_ready) {
                self.policy.connected();
                if let Some(first) = first.take() {
                    let _ = first.send(Ok(()));
                }
            }
            wake.set(if self.session.ready {
                self.session.next_deadline()
            } else {
                Some(ready_by)
            });
            // The application's messages wait while the browser holds a
            // megabyte unsent; the socket is looked at again shortly.
            let room = !socket.is_full();
            recheck.set((!room).then(|| Instant::now() + BUFFERED_CHECK));

            tokio::select! {
                biased;
                control = self.control.recv() => match control {
                    None | Some(Control::Close) => {
                        self.take_queued();
                        self.settle(&mut socket).await;
                        socket.close("client closed");
                        return Lost::Shutdown;
                    }
                    Some(Control::Reconnect) => {
                        self.take_queued();
                        self.settle(&mut socket).await;
                        socket.close("reconnecting");
                        return Lost::Requested;
                    }
                    Some(Control::Pause(reason)) => {
                        self.paused.insert(reason);
                        self.take_queued();
                        self.settle(&mut socket).await;
                        socket.close("paused");
                        return Lost::Paused;
                    }
                    Some(Control::Resume(reason)) => {
                        self.paused.remove(&reason);
                    }
                    Some(Control::Subscribe(topics)) => self.session.subscribe(topics),
                    Some(Control::Unsubscribe(topics)) => self.session.unsubscribe(topics),
                    Some(Control::Drain) => self.session.handler.drain(),
                    Some(Control::Clear) => self.session.catch_up_clears(),
                },
                happened = socket.events.recv() => {
                    // What already came is taken in one go, up to a batch.
                    let now = Instant::now();
                    self.session.clock = now;
                    let mut happened = happened;
                    let mut alive = false;
                    let mut taken = 1;
                    // The browser takes a frame of any size: one past the
                    // limit (a sealed one's with its overhead) ends the
                    // connection, as the native client does.
                    loop {
                        let (size, sealed) = match &happened {
                            Some(Happened::Text(text)) => (text.len(), false),
                            Some(Happened::Binary(data)) => (data.len(), data.starts_with(b"E:")),
                            _ => (0, false),
                        };
                        if frames::past_limit(size, sealed, self.config.max_message_size) {
                            socket.close("message too big");
                            return Lost::Closed {
                                code: None,
                                reason: format!(
                                    "a message of {size} bytes, past max_message_size ({})",
                                    self.config.max_message_size
                                ),
                            };
                        }
                        match happened {
                            Some(Happened::Text(text)) => {
                                alive |= frames::on_data(
                                    &mut self.decoder,
                                    &mut self.session,
                                    Data::Text(&text, Some(text.as_bytes())),
                                );
                            }
                            Some(Happened::Binary(data)) => {
                                alive |= frames::on_data(
                                    &mut self.decoder,
                                    &mut self.session,
                                    Data::Binary(&data),
                                );
                            }
                            Some(Happened::Close { code, reason }) => {
                                return Lost::Closed { code: Some(code), reason };
                            }
                            Some(Happened::Open | Happened::Error) => {}
                            None => {
                                return Lost::Closed {
                                    code: None,
                                    reason: "connection closed".to_owned(),
                                };
                            }
                        }
                        // What the frame asked for goes out first.
                        if !self.session.to_write.is_empty()
                            || self.session.fatal.is_some()
                            || self.session.became_ready
                        {
                            break;
                        }
                        // A batch at most: the next event is taken off the
                        // channel only to be handled, never taken and left.
                        if taken == READ_BATCH {
                            break;
                        }
                        match socket.events.try_recv() {
                            Ok(next) => {
                                happened = Some(next);
                                taken += 1;
                            }
                            Err(_) => break,
                        }
                    }
                    if alive {
                        last_seen = now;
                        self.session.alive_at(now);
                    }
                }
                Some(frame) = self.frames.recv(), if self.takes_frames() && room => {
                    self.session.queue(frame);
                    for _ in 1..SEND_BATCH {
                        match self.frames.try_recv() {
                            Ok(frame) => self.session.queue(frame),
                            Err(_) => break,
                        }
                    }
                }
                () = beat.fired() => {
                    beat.set(Some(Instant::now() + self.config.heartbeat));
                    if last_seen.elapsed() >= self.config.idle_timeout {
                        socket.close("idle");
                        return Lost::Closed {
                            code: None,
                            reason: format!("nothing from the server for {:?}", self.config.idle_timeout),
                        };
                    }
                    // Sealed in an end-to-end session, like every message.
                    if self.session.ready || self.session.sealing.is_none() {
                        self.session.to_write.push(Outgoing::Text(session::ping()));
                    }
                }
                () = recheck.fired() => {}
                () = wake.fired() => {
                    if !self.session.ready {
                        socket.close("not ready");
                        if self.session.sealing.as_ref().is_some_and(|s| !s.is_open()) {
                            return Lost::Fatal(Error::Encryption(format!(
                                "the server did not answer the end-to-end offer within {:?}",
                                self.config.ready_timeout
                            )));
                        }
                        return Lost::Failed(Error::Timeout(format!(
                            "the server did not take the connection within {:?}",
                            self.config.ready_timeout
                        )));
                    }
                    self.session.on_deadline(Instant::now());
                }
            }
        }
    }

    /// Before a close: what is queued goes, and what the server has of the
    /// outbox in hand is waited for, its answer coming before the close, so
    /// the application hears what became of it. A second at most.
    async fn settle(&mut self, socket: &mut Socket) {
        let until = Instant::now() + Duration::from_secs(1);
        let _ = self.write(socket);
        while self.session.awaits_answers() {
            tokio::select! {
                happened = socket.events.recv() => match happened {
                    Some(Happened::Text(text)) => {
                        frames::on_data(
                            &mut self.decoder,
                            &mut self.session,
                            Data::Text(&text, Some(text.as_bytes())),
                        );
                    }
                    Some(Happened::Binary(data)) => {
                        frames::on_data(&mut self.decoder, &mut self.session, Data::Binary(&data));
                    }
                    Some(Happened::Open | Happened::Error) => {}
                    Some(Happened::Close { .. }) | None => return,
                },
                _ = sleep_until(Some(until)) => return,
            }
            let _ = self.write(socket);
        }
    }

    /// Sends are taken from the queue: the connection is ready, and the sends
    /// that wait for their topics leave room (they count against the queue).
    fn takes_frames(&self) -> bool {
        (self.session.ready || self.session.keeps())
            && self.session.waiting() < self.config.send_queue
    }

    /// The connection's task with the outbox `outbox` (`Config::offline`).
    pub(crate) fn with_outbox(mut self, outbox: Option<crate::outbox::Outbox>) -> Self {
        self.session.outbox = outbox;
        self
    }

    /// What the application sent before it closed goes out first.
    fn take_queued(&mut self) {
        if self.session.ready || self.session.keeps() {
            while let Ok(frame) = self.frames.try_recv() {
                self.session.queue(frame);
            }
        }
    }

    /// Sends what the session queued: sealed once an end-to-end session is
    /// open. On a connection with a session nothing goes in plaintext but
    /// the handshake.
    fn write(&mut self, socket: &Socket) -> Result<(), Lost> {
        self.session.before_write();
        let mut last_kept = None;
        let written = self.write_out(socket, &mut last_kept);
        self.session.wrote(last_kept);
        written
    }

    /// Hands what the session queued to the socket; the last of the
    /// outbox's messages handed over in `last_kept`.
    fn write_out(&mut self, socket: &Socket, last_kept: &mut Option<u64>) -> Result<(), Lost> {
        let mut to_write = std::mem::take(&mut self.session.to_write);
        for outgoing in to_write.drain(..) {
            let (text, handshake, kept): (&str, bool, Option<u64>) = match &outgoing {
                Outgoing::Text(text) => (text, false, None),
                Outgoing::Handshake(text) => (text, true, None),
                Outgoing::Kept { s, text } => (text, false, Some(*s)),
            };
            let sent = match self.session.sealing.as_mut() {
                Some(sealing) => match sealing.seal(text) {
                    Ok(Some(sealed)) => socket.send_binary(&sealed),
                    Ok(None) if handshake => socket.send_text(text),
                    Ok(None) => {
                        tracing::error!("[WSE] a message before the end-to-end session not sent");
                        continue;
                    }
                    Err(error @ Error::Encryption(_)) => return Err(Lost::Fatal(error)),
                    Err(error) => return Err(Lost::Failed(error)),
                },
                None => socket.send_text(text),
            };
            sent.map_err(|e| Lost::Closed {
                code: None,
                reason: e.to_string(),
            })?;
            if kept.is_some() {
                *last_kept = kept;
            }
        }
        // The queue's room, kept for the next batch.
        if self.session.to_write.is_empty() {
            self.session.to_write = to_write;
        }
        Ok(())
    }
}
