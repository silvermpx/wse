//! The connection's task: makes the connection, reads and writes its
//! frames, keeps it alive, and makes it again when it is lost.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{self, Message as Frame, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::client::{Control, Shared, Status};
use crate::config::Config;
use crate::decode::{Body, Decoder};
use crate::error::Error;
use crate::handler::{Disconnected, Event, Handler};
use crate::security::{Keys, Sealing};
use crate::session::{self, Outgoing, Session};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Close codes that mean the credentials were refused.
const AUTH_CLOSE_CODES: [u16; 3] = [4401, 4403, 1008];
/// No end-to-end suite in common: trying again is refused the same way.
const CLOSE_E2E_UNSUPPORTED: u16 = 4800;
/// Refused tokens in a row a token provider may replace before the client
/// gives up.
const AUTH_ATTEMPTS: u32 = 3;
/// Messages the application queued that go out in one write.
const SEND_BATCH: usize = 256;
/// Frames read in one go when they came together.
const READ_BATCH: usize = 64;

/// How a connection ended.
enum Lost {
    /// The application closed the client.
    Shutdown,
    /// The connection went away.
    Closed { code: Option<u16>, reason: String },
    /// An attempt failed.
    Failed(Error),
    /// The client cannot go on.
    Fatal(Error),
}

pub(crate) struct Driver<H> {
    config: Config,
    shared: Arc<Shared>,
    frames: mpsc::Receiver<Utf8Bytes>,
    control: mpsc::UnboundedReceiver<Control>,
    status: watch::Sender<Status>,
    decoder: Decoder,
    keys: Arc<Keys>,
    session: Session<H>,
    /// Attempts since the last connection the server took.
    attempts: u32,
    auth_failures: u32,
}

impl<H: Handler> Driver<H> {
    pub(crate) fn new(
        config: Config,
        handler: H,
        shared: Arc<Shared>,
        keys: Arc<Keys>,
        frames: mpsc::Receiver<Utf8Bytes>,
        control: mpsc::UnboundedReceiver<Control>,
        status: watch::Sender<Status>,
    ) -> Self {
        let session = Session::new(
            handler,
            Arc::clone(&shared),
            Arc::clone(&keys),
            &config.topics,
            config.recovery_timeout,
        );
        Self {
            decoder: Decoder::new(config.max_message_size),
            keys,
            config,
            shared,
            frames,
            control,
            status,
            session,
            attempts: 0,
            auth_failures: 0,
        }
    }

    pub(crate) async fn run(mut self, first: oneshot::Sender<Result<(), Error>>) {
        let mut first = Some(first);
        let end = loop {
            // The attempt goes on while the application asks for things; a
            // close ends it at once.
            let attempt = {
                let opening = open_socket(&self.config, &self.shared, &self.keys);
                tokio::pin!(opening);
                loop {
                    tokio::select! {
                        result = &mut opening => break Some(result),
                        control = self.control.recv() => match control {
                            None | Some(Control::Close) => break None,
                            Some(Control::Reconnect) => {}
                            Some(Control::Subscribe(topics)) => self.session.subscribe(topics),
                            Some(Control::Unsubscribe(topics)) => self.session.unsubscribe(topics),
                        },
                    }
                }
            };
            let lost = match attempt {
                None => Lost::Shutdown,
                Some(Ok(ws)) => self.serve(ws, &mut first).await,
                Some(Err(error)) => Lost::Failed(error),
            };
            self.session.close();
            // Messages queued for the lost connection go with it.
            while self.frames.try_recv().is_ok() {}
            let (code, reason, error) = match lost {
                Lost::Shutdown => break None,
                Lost::Closed { code, reason } => {
                    let error = close_error(code, &reason);
                    (code, reason, error)
                }
                Lost::Failed(error) => (None, error.to_string(), error),
                Lost::Fatal(error) => {
                    if let Some(first) = first.take() {
                        let _ = first.send(Err(error));
                        break None;
                    }
                    break Some(error);
                }
            };
            if let Some(first) = first.take() {
                // The first connection is the caller's to retry.
                let _ = first.send(Err(error));
                break None;
            }
            match &error {
                Error::Auth(_) => {
                    self.auth_failures += 1;
                    if self.config.token_provider.is_none() || self.auth_failures >= AUTH_ATTEMPTS {
                        break Some(error);
                    }
                }
                Error::Encryption(_) | Error::Config(_) => break Some(error),
                _ => {}
            }
            if !self.config.reconnect.enabled {
                break Some(error);
            }
            if let Some(max) = self.config.reconnect.max_attempts
                && self.attempts >= max
            {
                break Some(Error::GaveUp(format!(
                    "{} attempt(s); last: {error}",
                    self.attempts
                )));
            }
            let retry_in = self.config.reconnect.delay(self.attempts);
            self.attempts += 1;
            self.status.send_replace(Status::Reconnecting);
            tracing::info!(
                "[WSE] connection lost ({reason}); attempt {} in {retry_in:?}",
                self.attempts
            );
            self.session
                .handler
                .event(Event::Disconnected(Disconnected {
                    code,
                    reason,
                    retry_in,
                    attempt: self.attempts,
                }));
            if !self.wait(retry_in).await {
                break None;
            }
        };
        self.session.finish();
        self.shared
            .ready
            .store(false, std::sync::atomic::Ordering::Release);
        self.session.handler.event(Event::Closed(end.clone()));
        self.status.send_replace(Status::Closed(end));
    }

    /// Waits before the next attempt, doing what the application asks
    /// meanwhile. False when it closed the client.
    async fn wait(&mut self, delay: Duration) -> bool {
        let until = Instant::now() + delay;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(until) => return true,
                control = self.control.recv() => match control {
                    None | Some(Control::Close) => return false,
                    Some(Control::Reconnect) => return true,
                    Some(Control::Subscribe(topics)) => self.session.subscribe(topics),
                    Some(Control::Unsubscribe(topics)) => self.session.unsubscribe(topics),
                },
                Some(_) = self.frames.recv() => {}
            }
        }
    }

    /// One connection, until it ends.
    async fn serve(
        &mut self,
        mut ws: Ws,
        first: &mut Option<oneshot::Sender<Result<(), Error>>>,
    ) -> Lost {
        let sealing = match self.keys.offer() {
            Ok(sealing) => sealing,
            Err(error) => return Lost::Fatal(error),
        };
        let hello = session::client_hello(
            self.config.compression,
            sealing.as_ref().and_then(Sealing::hello),
        );
        self.session.open(sealing);
        self.session.outbox.push(Outgoing::Text(hello));
        let ready_by = Instant::now() + self.config.ready_timeout;
        let beat = self.config.heartbeat;
        let mut heartbeat = tokio::time::interval_at(Instant::now() + beat, beat);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_seen = Instant::now();
        let mut flush = false;

        loop {
            if !self.session.outbox.is_empty() || flush {
                flush = false;
                if let Err(lost) = self.write(&mut ws).await {
                    return lost;
                }
            }
            if let Some(error) = self.session.fatal.take() {
                self.close(&mut ws, CloseCode::Normal, "client error").await;
                return Lost::Fatal(error);
            }
            if std::mem::take(&mut self.session.became_ready) {
                self.attempts = 0;
                self.auth_failures = 0;
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

            tokio::select! {
                frame = read(&mut self.session.handler, &mut ws) => {
                    // What already came is read in one go, up to a batch.
                    let mut frame = frame;
                    let mut alive = false;
                    for _ in 0..READ_BATCH {
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
                            Some(Ok(frame)) => alive |= self.on_frame(frame),
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
                        if !self.session.outbox.is_empty()
                            || self.session.fatal.is_some()
                            || self.session.became_ready
                        {
                            break;
                        }
                        match poll_once(read(&mut self.session.handler, &mut ws)).await {
                            Some(next) => frame = next,
                            None => break,
                        }
                    }
                    if alive {
                        last_seen = Instant::now();
                    }
                }
                Some(text) = self.frames.recv(), if self.session.ready => {
                    self.session.outbox.push(Outgoing::Text(text));
                    for _ in 1..SEND_BATCH {
                        match self.frames.try_recv() {
                            Ok(text) => self.session.outbox.push(Outgoing::Text(text)),
                            Err(_) => break,
                        }
                    }
                }
                control = self.control.recv() => match control {
                    None | Some(Control::Close) => {
                        self.close(&mut ws, CloseCode::Normal, "client closed").await;
                        return Lost::Shutdown;
                    }
                    Some(Control::Reconnect) => {
                        self.close(&mut ws, CloseCode::Away, "reconnecting").await;
                        return Lost::Closed { code: None, reason: "reconnect asked".to_owned() };
                    }
                    Some(Control::Subscribe(topics)) => self.session.subscribe(topics),
                    Some(Control::Unsubscribe(topics)) => self.session.unsubscribe(topics),
                },
                _ = heartbeat.tick() => {
                    if last_seen.elapsed() >= self.config.idle_timeout {
                        self.close(&mut ws, CloseCode::Away, "idle").await;
                        return Lost::Closed {
                            code: None,
                            reason: format!("nothing from the server for {:?}", self.config.idle_timeout),
                        };
                    }
                    let now = session::now_ms().to_be_bytes();
                    if let Err(e) = ws.feed(Frame::Ping(Bytes::copy_from_slice(&now))).await {
                        return Lost::Closed { code: None, reason: e.to_string() };
                    }
                    flush = true;
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
    fn on_frame(&mut self, frame: Frame) -> bool {
        if self.session.sealing.is_some() {
            return self.on_session_frame(frame);
        }
        match frame {
            Frame::Text(text) => {
                let bytes: &Bytes = text.as_ref();
                self.session.on_json(text.as_str(), Some(bytes));
            }
            Frame::Binary(data) => match self.decoder.binary(&data) {
                Ok(Body::Json(json)) => self.session.on_json(json, Some(&data)),
                Ok(Body::Msgpack(bytes)) => self.session.on_msgpack(bytes, Some(&data)),
                Ok(Body::Sealed) => {
                    tracing::warn!("[WSE] sealed frame without an end-to-end session dropped");
                }
                Ok(Body::Binary) => self.session.handler.binary(&data),
                Err(e) => tracing::warn!("[WSE] frame of {} bytes dropped: {e}", data.len()),
            },
            _ => {}
        }
        true
    }

    /// A frame on a connection with an end-to-end session: sealed, or one
    /// of the few that may come before the session opens.
    fn on_session_frame(&mut self, frame: Frame) -> bool {
        match frame {
            Frame::Binary(data) if data.starts_with(b"E:") => {
                let Some(sealing) = self.session.sealing.as_mut() else {
                    return false;
                };
                match sealing.open(&data) {
                    Ok(Some(plain)) => {
                        match plain.strip_prefix(b"M:") {
                            Some(msgpack) => self.session.on_msgpack(msgpack, None),
                            None => match std::str::from_utf8(&plain) {
                                Ok(text) => self.session.on_json(text, None),
                                Err(_) => tracing::warn!("[WSE] sealed frame not UTF-8 dropped"),
                            },
                        }
                        true
                    }
                    // The server's close notice: its close frame follows.
                    Ok(None) => true,
                    Err(error) => {
                        self.session.fail(error);
                        false
                    }
                }
            }
            Frame::Text(text) => {
                self.session.on_unsealed(Some(text.as_str()));
                false
            }
            Frame::Binary(data) => {
                self.session.on_unsealed(std::str::from_utf8(&data).ok());
                false
            }
            _ => false,
        }
    }

    /// Writes what the session queued, in one flush: sealed once an
    /// end-to-end session is open.
    async fn write(&mut self, ws: &mut Ws) -> Result<(), Lost> {
        let lost = |e: tungstenite::Error| Lost::Closed {
            code: None,
            reason: e.to_string(),
        };
        for outgoing in self.session.outbox.drain(..) {
            let Outgoing::Text(text) = outgoing;
            let frame = match self.session.sealing.as_mut() {
                Some(sealing) => match sealing.seal(text.as_str()) {
                    Ok(Some(sealed)) => Frame::Binary(sealed.into()),
                    Ok(None) => Frame::Text(text),
                    Err(error @ Error::Encryption(_)) => return Err(Lost::Fatal(error)),
                    Err(error) => return Err(Lost::Failed(error)),
                },
                None => Frame::Text(text),
            };
            ws.feed(frame).await.map_err(lost)?;
        }
        ws.flush().await.map_err(lost)
    }

    /// Closes the connection on the client's side.
    async fn close(&mut self, ws: &mut Ws, code: CloseCode, reason: &str) {
        let _ = self.write(ws).await;
        let frame = CloseFrame {
            code,
            reason: reason.into(),
        };
        // The server answers within its own 5 s; past one, it is not waited
        // for.
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
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
            let token = provider
                .token()
                .await
                .map_err(|e| Error::Connect(format!("token provider: {e}")))?;
            Some(token)
        }
        None => shared.token.lock().ok().and_then(|t| t.clone()),
    };
    let request = config.request(token.as_deref(), keys.e2e)?;
    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(config.max_message_size + 16);
    ws_config.max_frame_size = Some(config.max_message_size + 16);
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
    let tls = config.tls.clone().unwrap_or_else(default_tls);
    Some(tokio_tungstenite::Connector::Rustls(tls))
}

/// The next frame, once the handler is ready for it.
async fn read<H: Handler>(
    handler: &mut H,
    ws: &mut Ws,
) -> Option<Result<Frame, tungstenite::Error>> {
    handler.ready().await;
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
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// What a close means for the client.
fn close_error(code: Option<u16>, reason: &str) -> Error {
    let what = match code {
        Some(code) if reason.is_empty() => format!("closed with {code}"),
        Some(code) => format!("closed with {code}: {reason}"),
        None => reason.to_owned(),
    };
    match code {
        Some(code) if AUTH_CLOSE_CODES.contains(&code) => Error::Auth(what),
        Some(CLOSE_E2E_UNSUPPORTED) => Error::Encryption(what),
        _ => Error::Connect(what),
    }
}

/// TLS with the Mozilla root certificates, on aws-lc-rs.
#[cfg(feature = "aws-lc")]
fn default_tls() -> Arc<rustls::ClientConfig> {
    use std::sync::OnceLock;
    static TLS: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    TLS.get_or_init(|| {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        #[cfg(feature = "fips")]
        let provider = Arc::new(rustls::crypto::default_fips_provider());
        #[cfg(not(feature = "fips"))]
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("aws-lc-rs supports the default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        Arc::new(config)
    })
    .clone()
}
