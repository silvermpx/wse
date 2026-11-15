//! The connection's task outside the browser: makes the connection, reads
//! and writes its frames, keeps it alive, and makes it again when it is
//! lost.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{self, Message as Frame};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use web_time::Instant;

use crate::client::{Control, Shared, Status};
use crate::config::Config;
use crate::decode::Decoder;
use crate::error::Error;
use crate::frames::{self, Data};
use crate::handler::{Event, Handler};
use crate::policy::{First, Lost, Next, Policy};
use crate::security::{Keys, Sealing};
use crate::session::{self, Outgoing, Session};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Messages the application queued that go out in one write.
const SEND_BATCH: usize = 256;
/// Frames read in one go when they came together.
const READ_BATCH: usize = 64;

pub(crate) struct Driver<H> {
    config: Config,
    shared: Arc<Shared>,
    frames: mpsc::Receiver<String>,
    control: mpsc::UnboundedReceiver<Control>,
    status: watch::Sender<Status>,
    decoder: Decoder,
    keys: Arc<Keys>,
    session: Session<H>,
    policy: Policy,
}

impl<H: Handler> Driver<H> {
    pub(crate) fn new(
        config: Config,
        handler: H,
        shared: Arc<Shared>,
        keys: Arc<Keys>,
        frames: mpsc::Receiver<String>,
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
            policy: Policy::default(),
        }
    }

    pub(crate) async fn run(mut self, mut first: First) {
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
        let until = tokio::time::Instant::now() + delay;
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
    async fn serve(&mut self, mut ws: Ws, first: &mut First) -> Lost {
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
        let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + beat, beat);
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
    fn on_frame(&mut self, frame: &Frame) -> bool {
        let data = match frame {
            Frame::Text(text) => Data::Text(text.as_str(), Some(text.as_ref())),
            Frame::Binary(data) => Data::Binary(data),
            _ => return self.session.sealing.is_none(),
        };
        frames::on_data(&mut self.decoder, &mut self.session, data)
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
                Some(sealing) => match sealing.seal(&text) {
                    Ok(Some(sealed)) => Frame::Binary(sealed.into()),
                    Ok(None) => Frame::Text(text.into()),
                    Err(error @ Error::Encryption(_)) => return Err(Lost::Fatal(error)),
                    Err(error) => return Err(Lost::Failed(error)),
                },
                None => Frame::Text(text.into()),
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
        Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
        None => std::future::pending().await,
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
