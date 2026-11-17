//! The connection's task outside the browser: makes the connection, reads
//! and writes its frames, keeps it alive, and makes it again when it is
//! lost.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{self, Message as Frame, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
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

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Messages the application queued that go out in one write.
const SEND_BATCH: usize = 256;
/// Frames read in one go when they came together.
const READ_BATCH: usize = 64;
/// What a frame may carry past the message itself: a sealed frame's `E:`,
/// header and tag.
const FRAME_OVERHEAD: usize = 32;

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
        let end = loop {
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
            // The attempt goes on while the application asks for things; a
            // close ends it at once, a pause gives it up.
            let attempt = {
                let opening = open_socket(&self.config, &self.shared, &self.keys);
                tokio::pin!(opening);
                loop {
                    tokio::select! {
                        result = &mut opening => break Some(result.map_err(Some)),
                        control = self.control.recv() => match control {
                            None | Some(Control::Close) => break None,
                            Some(Control::Reconnect) => {}
                            Some(Control::Pause(reason)) => {
                                self.paused.insert(reason);
                                break Some(Err(None));
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
                    }
                }
            };
            let lost = match attempt {
                None => Lost::Shutdown,
                Some(Ok(ws)) => self.serve(ws, &mut first).await,
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
        };
        // What was sent before the close goes to the outbox: a file keeps it
        // for the next client.
        self.frames.close();
        while let Ok(frame) = self.frames.try_recv() {
            self.session.queue(frame);
        }
        self.session.finish();
        // The file is the next client's once this one says it closed.
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
        // What the handler still holds (the stream's last items, its end)
        // reaches a reader that goes on reading.
        let _ = tokio::time::timeout(self.config.idle_timeout, self.session.handler.ready()).await;
    }

    /// Waits before the next attempt, doing what the application asks
    /// meanwhile. False when it closed the client; a pause ends the wait.
    async fn wait(&mut self, delay: Duration) -> bool {
        let until = tokio::time::Instant::now() + delay;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(until) => return true,
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
                () = sleep_until(self.session.next_deadline()) => {
                    self.session.on_deadline(Instant::now());
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

    /// One connection, until it ends.
    async fn serve(&mut self, mut ws: Ws, first: &mut First) -> Lost {
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
        let beat = self.config.heartbeat;
        let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + beat, beat);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_seen = Instant::now();
        let mut asleep = session::SleepWatch::new(web_time::SystemTime::now(), last_seen);
        let mut flush = false;

        loop {
            if let Some(error) = self.session.fatal.take() {
                // Nothing the failed frame asked for goes out.
                self.session.to_write.clear();
                self.close(&mut ws, CloseCode::Normal, "client error").await;
                return Lost::Fatal(error);
            }
            self.session.before_write();
            if !self.session.to_write.is_empty() || flush {
                flush = false;
                if let Err(lost) = self.write_in_time(&mut ws).await {
                    return lost;
                }
            }
            if std::mem::take(&mut self.session.became_ready) {
                self.policy.connected();
                self.status.send_replace(Status::Connected);
                if let Some(first) = first.take() {
                    let _ = first.send(Ok(()));
                }
            }
            let deadline = if self.session.ready {
                self.session.next_deadline()
            } else {
                Some(ready_by)
            };
            // Reads wait for the application once the server took the
            // connection: the handshake is never held back.
            let gate = self.session.ready;

            tokio::select! {
                frame = read(&mut self.session.handler, &mut ws, gate) => {
                    // What already came is read in one go, up to a batch.
                    let now = Instant::now();
                    self.session.clock = now;
                    let mut frame = frame;
                    let mut alive = false;
                    let mut taken = 1;
                    loop {
                        match frame {
                            Some(Ok(Frame::Close(close))) => {
                                // tungstenite queued the answer: send it.
                                let _ = ws.flush().await;
                                let (code, reason) = close.map_or((None, String::new()), |c| {
                                    (Some(u16::from(c.code)), c.reason.to_string())
                                });
                                return Lost::Closed { code, reason };
                            }
                            Some(Ok(Frame::Ping(_))) => {
                                flush = true;
                                // In an end-to-end session only sealed
                                // frames show the server is alive.
                                alive |= self.session.sealing.is_none();
                            }
                            Some(Ok(frame)) => alive |= self.on_frame(&frame),
                            Some(Err(e)) => {
                                return Lost::Closed { code: None, reason: e.to_string() };
                            }
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
                        // A batch at most: the next frame is read only to be
                        // taken, never read and left.
                        if taken == READ_BATCH {
                            break;
                        }
                        match poll_once(read(&mut self.session.handler, &mut ws, gate)).await {
                            Some(next) => {
                                frame = next;
                                taken += 1;
                            }
                            None => break,
                        }
                    }
                    if alive {
                        last_seen = now;
                        self.session.alive_at(now);
                    }
                }
                Some(frame) = self.frames.recv(), if self.takes_frames() => {
                    self.session.queue(frame);
                    for _ in 1..SEND_BATCH {
                        match self.frames.try_recv() {
                            Ok(frame) => self.session.queue(frame),
                            Err(_) => break,
                        }
                    }
                }
                control = self.control.recv() => match control {
                    None | Some(Control::Close) => {
                        self.take_queued();
                        self.close(&mut ws, CloseCode::Normal, "client closed").await;
                        return Lost::Shutdown;
                    }
                    Some(Control::Reconnect) => {
                        self.take_queued();
                        self.close(&mut ws, CloseCode::Away, "reconnecting").await;
                        return Lost::Requested;
                    }
                    Some(Control::Pause(reason)) => {
                        self.paused.insert(reason);
                        self.take_queued();
                        self.close(&mut ws, CloseCode::Away, "paused").await;
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
                _ = heartbeat.tick() => {
                    // The machine slept: a new connection recovers what was
                    // missed, rather than waiting out the idle timeout.
                    if let Some(slept) =
                        asleep.slept(web_time::SystemTime::now(), Instant::now(), self.config.heartbeat)
                    {
                        self.close(&mut ws, CloseCode::Away, "slept").await;
                        return Lost::Closed {
                            code: None,
                            reason: format!("the machine slept for about {}s", slept.as_secs()),
                        };
                    }
                    // While the application is behind, reads wait for it,
                    // not for the server: silence then says nothing.
                    if poll_once(self.session.handler.ready()).await.is_none() {
                        last_seen = Instant::now();
                    }
                    if last_seen.elapsed() >= self.config.idle_timeout {
                        self.close(&mut ws, CloseCode::Away, "idle").await;
                        return Lost::Closed {
                            code: None,
                            reason: format!("nothing from the server for {:?}", self.config.idle_timeout),
                        };
                    }
                    if self.session.sealing.is_some() {
                        // In an end-to-end session only sealed frames show
                        // the server is alive: a sealed ping gets a sealed
                        // answer.
                        if self.session.ready {
                            self.session.to_write.push(Outgoing::Text(session::ping()));
                        }
                    } else {
                        let now = session::now_ms().to_be_bytes();
                        let ping = ws.feed(Frame::Ping(Bytes::copy_from_slice(&now)));
                        match tokio::time::timeout(self.config.idle_timeout, ping).await {
                            Ok(Ok(())) => flush = true,
                            Ok(Err(e)) => return Lost::Closed { code: None, reason: e.to_string() },
                            Err(_) => return self.stalled(),
                        }
                    }
                }
                _ = sleep_until(deadline) => {
                    if !self.session.ready {
                        self.close(&mut ws, CloseCode::Away, "not ready").await;
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

    /// A data frame; true when it shows the server is alive.
    fn on_frame(&mut self, frame: &Frame) -> bool {
        let data = match frame {
            Frame::Text(text) => Data::Text(text.as_str(), Some(text.as_ref())),
            Frame::Binary(data) => Data::Binary(data),
            _ => return self.session.sealing.is_none(),
        };
        frames::on_data(&mut self.decoder, &mut self.session, data)
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

    /// `write`, unless the server takes nothing for `idle_timeout`: a link
    /// gone dark fills the socket's buffer, and the write would wait for as
    /// long as the system keeps the connection.
    async fn write_in_time(&mut self, ws: &mut Ws) -> Result<(), Lost> {
        match tokio::time::timeout(self.config.idle_timeout, self.write(ws)).await {
            Ok(written) => written,
            Err(_) => Err(self.stalled()),
        }
    }

    fn stalled(&self) -> Lost {
        Lost::Closed {
            code: None,
            reason: format!("the server took nothing for {:?}", self.config.idle_timeout),
        }
    }

    /// Writes what the session queued, in one flush: sealed once an
    /// end-to-end session is open. On a connection with a session nothing
    /// goes in plaintext but the handshake.
    async fn write(&mut self, ws: &mut Ws) -> Result<(), Lost> {
        let lost = |e: tungstenite::Error| Lost::Closed {
            code: None,
            reason: e.to_string(),
        };
        self.session.before_write();
        let mut last_kept = None;
        let mut to_write = std::mem::take(&mut self.session.to_write);
        for outgoing in to_write.drain(..) {
            let (text, handshake, kept) = match outgoing {
                Outgoing::Text(text) => (Utf8Bytes::from(text), false, None),
                Outgoing::Handshake(text) => (Utf8Bytes::from(text), true, None),
                // The outbox's text, shared, not copied.
                Outgoing::Kept { s, text } => match Utf8Bytes::try_from(text.into_bytes()) {
                    Ok(text) => (text, false, Some(s)),
                    Err(_) => continue,
                },
            };
            let frame = match self.session.sealing.as_mut() {
                Some(sealing) => match sealing.seal(&text) {
                    Ok(Some(sealed)) => Frame::Binary(sealed.into()),
                    Ok(None) if handshake => Frame::Text(text),
                    Ok(None) => {
                        tracing::error!("[WSE] a message before the end-to-end session not sent");
                        continue;
                    }
                    Err(error @ Error::Encryption(_)) => return Err(Lost::Fatal(error)),
                    Err(error) => return Err(Lost::Failed(error)),
                },
                None => Frame::Text(text),
            };
            ws.feed(frame).await.map_err(lost)?;
            if kept.is_some() {
                last_kept = kept;
            }
        }
        // The queue's room, kept for the next batch.
        if self.session.to_write.is_empty() {
            self.session.to_write = to_write;
        }
        ws.flush().await.map_err(lost)?;
        self.session.wrote(last_kept);
        Ok(())
    }

    /// Closes the connection on the client's side, with what is queued
    /// written first.
    async fn close(&mut self, ws: &mut Ws, code: CloseCode, reason: &str) {
        let frame = CloseFrame {
            code,
            reason: reason.into(),
        };
        // The server answers within its own 5 s; past one, neither it nor a
        // write it does not take is waited for.
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            let _ = self.write(ws).await;
            // What the server owes the outbox: its answers come before the
            // close, what they let go out goes, and the application hears
            // what became of each.
            while self.session.awaits_answers() {
                match read(&mut self.session.handler, ws, true).await {
                    Some(Ok(Frame::Close(_)) | Err(_)) | None => break,
                    Some(Ok(frame)) => {
                        self.on_frame(&frame);
                    }
                }
                let _ = self.write(ws).await;
            }
            ws.close(Some(frame)).await?;
            while ws.next().await.is_some() {}
            Ok::<_, tungstenite::Error>(())
        })
        .await;
    }
}

/// Opens the socket and upgrades it.
async fn open_socket(config: &Config, shared: &Shared, keys: &Keys) -> Result<Ws, Error> {
    let token = match &config.token_provider {
        Some(provider) => {
            let token = tokio::time::timeout(config.connect_timeout, provider.token())
                .await
                .map_err(|_| {
                    Error::Timeout(format!(
                        "no token from the token provider within {:?}",
                        config.connect_timeout
                    ))
                })?
                .map_err(|e| Error::Connect(format!("token provider: {e}")))?;
            Some(token)
        }
        None => shared.token.lock().ok().and_then(|t| t.clone()),
    };
    let request = config.request(token.as_deref(), keys.e2e)?;
    let mut ws_config = WebSocketConfig::default();
    let limit = config.max_message_size.saturating_add(FRAME_OVERHEAD);
    ws_config.max_message_size = Some(limit);
    ws_config.max_frame_size = Some(limit);
    #[cfg(feature = "aws-lc")]
    let open = tokio_tungstenite::connect_async_tls_with_config(
        request,
        Some(ws_config),
        true,
        tls(config),
    );
    #[cfg(not(feature = "aws-lc"))]
    let open = tokio_tungstenite::connect_async_with_config(request, Some(ws_config), true);
    match tokio::time::timeout(config.connect_timeout, open).await {
        Err(_) => Err(Error::Timeout(format!(
            "no upgrade within {:?}",
            config.connect_timeout
        ))),
        Ok(Err(tungstenite::Error::Http(response))) => {
            let status = response.status().as_u16();
            if status == 401 || status == 403 {
                Err(Error::Auth(format!(
                    "the upgrade was refused with HTTP {status}"
                )))
            } else {
                Err(Error::Connect(format!(
                    "the upgrade was refused with HTTP {status}"
                )))
            }
        }
        Ok(Err(e)) => Err(Error::Connect(e.to_string())),
        Ok(Ok((ws, _))) => Ok(ws),
    }
}

#[cfg(feature = "aws-lc")]
fn tls(config: &Config) -> Option<tokio_tungstenite::Connector> {
    if !config.url.starts_with("wss://") {
        return None;
    }
    // `Config::check` made sure it is a rustls::ClientConfig.
    let tls = config
        .tls
        .clone()
        .and_then(|tls| tls.downcast::<rustls::ClientConfig>().ok())
        .unwrap_or_else(default_tls);
    Some(tokio_tungstenite::Connector::Rustls(tls))
}

/// The next frame, once the handler is ready for it when `gate`.
async fn read<H: Handler>(
    handler: &mut H,
    ws: &mut Ws,
    gate: bool,
) -> Option<Result<Frame, tungstenite::Error>> {
    if gate {
        handler.ready().await;
    }
    ws.next().await
}

/// `future` polled once: its output, or `None` (and it dropped) when it is
/// not ready.
async fn poll_once<F: std::future::Future>(future: F) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| match future.as_mut().poll(cx) {
        std::task::Poll::Ready(output) => std::task::Poll::Ready(Some(output)),
        std::task::Poll::Pending => std::task::Poll::Ready(None),
    })
    .await
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
        None => std::future::pending().await,
    }
}

/// TLS with the Mozilla root certificates, on aws-lc-rs.
#[cfg(feature = "aws-lc")]
pub(crate) fn default_tls() -> Arc<rustls::ClientConfig> {
    use std::sync::OnceLock;
    static TLS: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    TLS.get_or_init(|| {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let config = tls_builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Arc::new(config)
    })
    .clone()
}

/// A TLS client configuration on aws-lc-rs (its FIPS module with `fips`),
/// the safe default versions, its verifier still to be chosen.
#[cfg(feature = "aws-lc")]
pub(crate) fn tls_builder() -> rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier> {
    #[cfg(feature = "fips")]
    let provider = Arc::new(rustls::crypto::default_fips_provider());
    #[cfg(not(feature = "fips"))]
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports the default protocol versions")
}
