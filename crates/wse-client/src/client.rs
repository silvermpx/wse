//! The application's handle on a connection.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};

#[cfg(target_arch = "wasm32")]
use crate::browser::Driver;
use crate::config::Config;
#[cfg(not(target_arch = "wasm32"))]
use crate::connection::Driver;
use crate::delivery::{self, Gate, TopicState};
use crate::error::Error;
use crate::handler::Handler;
use crate::message::Message;
use crate::security::Keys;

/// Where the client stands.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Status {
    /// Making the first connection.
    Connecting,
    /// The server took the connection.
    Connected,
    /// The connection was lost; making it again.
    Reconnecting,
    /// Paused by the application: no connection until it resumes.
    Paused,
    /// Stopped for good: closed by the application (`None`), or the reason.
    Closed(Option<Error>),
}

/// What the application asks of the connection, besides its messages.
pub(crate) enum Control {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Reconnect,
    Pause(String),
    Resume(String),
    /// The delivery's gate made something for the handler (held messages
    /// handed on): it goes to the handler now.
    Drain,
    Close,
}

/// Where what the delivery's gate makes outside the messages goes to the
/// application.
#[derive(Clone)]
pub(crate) enum Wake {
    /// The handler, from the connection's task.
    Session,
    /// The [`Events`](crate::Events) stream, at its next read.
    Reader(mpsc::UnboundedSender<()>),
}

/// A message the application sent, on its way to the connection's task.
pub(crate) struct Queued {
    pub(crate) text: String,
    /// The user it was sent as ([`Shared::users`]): it goes nowhere once the
    /// server took a connection for another user.
    pub(crate) user: u64,
    /// A signed or live message goes on the connection it was sent on
    /// ([`Shared::connections`]), or not at all.
    pub(crate) only_on: Option<u64>,
    /// Topics that must each be continuous when it goes: until then it
    /// waits, in order with the others that wait.
    pub(crate) requires: Option<Box<[String]>>,
}

/// The user and connection a message was sent for.
struct SentFor {
    user: u64,
    /// What a signed or live message goes on.
    connection: u64,
}

impl SentFor {
    fn frame(&self, text: String) -> Queued {
        Queued {
            text,
            user: self.user,
            only_on: None,
            requires: None,
        }
    }

    /// For when each of `topics` is continuous.
    fn requiring(&self, text: String, topics: Box<[String]>) -> Queued {
        Queued {
            requires: Some(topics),
            ..self.frame(text)
        }
    }

    /// For this connection only.
    fn live(&self, text: String) -> Queued {
        Queued {
            only_on: Some(self.connection),
            ..self.frame(text)
        }
    }
}

/// What the handles and the connection's task share.
pub(crate) struct Shared {
    /// The server took the current connection: messages may go.
    pub(crate) ready: AtomicBool,
    /// Counts the times the server took a connection for another user than
    /// the one before.
    pub(crate) users: AtomicU64,
    /// Counts the connections the server took.
    pub(crate) connections: AtomicU64,
    /// The current connection's id and user.
    pub(crate) connection: Mutex<Option<(String, Option<String>)>>,
    /// The token for the next connection attempt.
    pub(crate) token: Mutex<Option<String>>,
    /// The server's signing key: what signed messages are for.
    pub(crate) server_kid: Mutex<Option<String>>,
    /// Stops the connection's task where it stands: a close past its
    /// deadline.
    stop: Mutex<Option<futures_util::future::AbortHandle>>,
}

impl Shared {
    pub(crate) fn new(token: Option<String>) -> Self {
        Self {
            ready: AtomicBool::new(false),
            users: AtomicU64::new(0),
            connections: AtomicU64::new(0),
            connection: Mutex::new(None),
            token: Mutex::new(token),
            server_kid: Mutex::new(None),
            stop: Mutex::new(None),
        }
    }
}

/// A connection to a WSE server, made again when it is lost.
///
/// Clones share the connection, which lives as long as the application
/// holds a handle (or until [`close`](Self::close)); the client a handler
/// is given by [`connect_with`](Self::connect_with) does not keep it.
/// Sends go out in the order they were made; what the server sends goes to
/// the [`Handler`] the client was made with, or to the
/// [`Events`](crate::Events) of [`connect`](crate::connect).
///
/// A message still queued when the connection is lost goes out on the next
/// one, unless the server takes that one for another user: then it goes
/// nowhere, and the topics start over. A signed message goes out on the
/// connection it was signed on, or not at all.
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
    #[cfg_attr(not(feature = "aws-lc"), allow(dead_code))]
    keys: Arc<Keys>,
    frames: mpsc::Sender<Queued>,
    control: mpsc::UnboundedSender<Control>,
    status: watch::Receiver<Status>,
    /// The delivery: the topics' states, what was held and committed.
    gate: delivery::Shared,
    wake: Wake,
    /// The application's hold on the connection; none for a handler's.
    _owner: Option<Arc<Owner>>,
}

/// The application's handles hold one of these: when the last goes, the
/// connection is closed.
struct Owner(mpsc::UnboundedSender<Control>);

impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.0.send(Control::Close);
    }
}

impl Client {
    /// Connects, and returns once the server took the connection. What it
    /// sends goes to `handler`.
    ///
    /// The first connection is not made again when it fails: the error is
    /// returned. Later ones are, as [`Config::reconnect`] says.
    pub async fn connect<H: Handler>(config: Config, handler: H) -> Result<Self, Error> {
        Self::connect_with(config, |_| handler).await
    }

    /// [`connect`](Self::connect) with a handler that is given the client,
    /// to send from it.
    pub async fn connect_with<H, F>(config: Config, make: F) -> Result<Self, Error>
    where
        H: Handler,
        F: FnOnce(Client) -> H,
    {
        Self::start(config, make, None)
            .await
            .map(|(client, _)| client)
    }

    /// Connects. The delivery's gate stands before the handler, or, with
    /// `reader` (the [`Events`](crate::Events) stream's wake), where the
    /// stream is read: it is returned for it.
    pub(crate) async fn start<H, F>(
        mut config: Config,
        make: F,
        reader: Option<mpsc::UnboundedSender<()>>,
    ) -> Result<(Self, delivery::Shared), Error>
    where
        H: Handler,
        F: FnOnce(Client) -> H,
    {
        config.check()?;
        let gate = Gate::new(config.delivery.clone(), &config.url, &config.topics)?;
        let gate = Arc::new(std::sync::Mutex::new(gate));
        // Recovery starts from what the application committed.
        let committed = Arc::clone(&gate);
        config.committed = Some(Arc::new(move |user| {
            delivery::lock(&committed).committed(user)
        }));
        let before_handler = reader.is_none();
        let wake = match reader {
            Some(reader) => Wake::Reader(reader),
            None => Wake::Session,
        };
        let keys = Arc::new(Keys::from_config(&config)?);
        // Read into the keys: the bytes of this copy go.
        #[cfg(feature = "aws-lc")]
        if let Some(mut key) = config.signing_key.take() {
            zeroize::Zeroize::zeroize(&mut key);
        }
        let shared = Arc::new(Shared::new(config.token.clone()));
        let (frames_tx, frames_rx) = mpsc::channel(config.send_queue);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (status_tx, status_rx) = watch::channel(Status::Connecting);
        let for_handler = Client {
            shared: Arc::clone(&shared),
            keys: Arc::clone(&keys),
            frames: frames_tx,
            control: control_tx.clone(),
            status: status_rx,
            gate: Arc::clone(&gate),
            wake,
            _owner: None,
        };
        let client = Client {
            _owner: Some(Arc::new(Owner(control_tx))),
            ..for_handler.clone()
        };
        let handler = make(for_handler);
        let (first_tx, first_rx) = oneshot::channel();
        let ended = (Arc::clone(&shared), status_tx.clone());
        let (stop, stopped) = futures_util::future::AbortHandle::new_pair();
        if let Ok(mut slot) = shared.stop.lock() {
            *slot = Some(stop);
        }
        let handler = crate::delivery::Delivered::new(handler, Arc::clone(&gate), before_handler);
        let driver = Driver::new(
            config, handler, shared, keys, frames_rx, control_rx, status_tx,
        );
        let run = async move {
            use futures_util::FutureExt;
            let driven = futures_util::future::Abortable::new(
                std::panic::AssertUnwindSafe(driver.run(Some(first_tx))).catch_unwind(),
                stopped,
            )
            .await;
            let (shared, status) = ended;
            match driven {
                Ok(Ok(())) => {}
                // A handler that panics ends the client, which says so.
                Ok(Err(_)) => {
                    shared.ready.store(false, Ordering::Release);
                    status.send_replace(Status::Closed(Some(Error::Panicked)));
                }
                // Stopped by a close past its deadline: the connection went
                // with the task.
                Err(_) => {
                    shared.ready.store(false, Ordering::Release);
                    status.send_replace(Status::Closed(None));
                }
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        tokio::spawn(run);
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(run);
        match first_rx.await {
            Ok(Ok(())) => Ok((client, gate)),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::Closed),
        }
    }

    /// Sends a message `{"t": kind, "p": payload}`; fails at once when the
    /// queue is full.
    pub fn try_send<P: Serialize + ?Sized>(&self, kind: &str, payload: &P) -> Result<(), Error> {
        let for_now = self.ready()?;
        let text = encode(kind, payload)?;
        self.frames
            .try_send(for_now.frame(text))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Full,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }

    /// Sends a message `{"t": kind, "p": payload}`, waiting for room in the
    /// queue.
    pub async fn send<P: Serialize + ?Sized>(&self, kind: &str, payload: &P) -> Result<(), Error> {
        let for_now = self.ready()?;
        let text = encode(kind, payload)?;
        self.frames
            .send(for_now.frame(text))
            .await
            .map_err(|_| Error::Closed)
    }

    /// Sends a message `{"t": kind, "p": payload}` on the connection that is
    /// ready now, or not at all: one this connection did not write is
    /// dropped, never sent on a later connection. For commands that must not
    /// run late (a valve, a door, an order). Fails at once when the queue is
    /// full.
    pub fn try_send_live<P: Serialize + ?Sized>(
        &self,
        kind: &str,
        payload: &P,
    ) -> Result<(), Error> {
        let for_now = self.ready()?;
        let text = encode(kind, payload)?;
        self.frames
            .try_send(for_now.live(text))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Full,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }

    /// [`Client::try_send_live`], waiting for room in the queue.
    pub async fn send_live<P: Serialize + ?Sized>(
        &self,
        kind: &str,
        payload: &P,
    ) -> Result<(), Error> {
        let for_now = self.ready()?;
        let text = encode(kind, payload)?;
        self.frames
            .send(for_now.live(text))
            .await
            .map_err(|_| Error::Closed)
    }

    /// Sends a message `{"t": kind, "p": payload}` once each of `topics` is
    /// continuous on the connection (the delivery's
    /// [`TopicState`](crate::TopicState)): until then it waits, in order with
    /// the others that wait, while sends that require nothing go past it.
    /// It is checked as it is taken to go out, so one waiting does not go
    /// after a reconnect before the resync; it is the user's who made it, and
    /// goes nowhere once the server takes a connection for another. Waits
    /// for room in the queue.
    pub async fn send_requiring<P, I, T>(
        &self,
        kind: &str,
        payload: &P,
        topics: I,
    ) -> Result<(), Error>
    where
        P: Serialize + ?Sized,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let for_now = self.ready()?;
        let text = encode(kind, payload)?;
        let topics: Box<[String]> = topics.into_iter().map(Into::into).collect();
        self.frames
            .send(for_now.requiring(text, topics))
            .await
            .map_err(|_| Error::Closed)
    }

    /// [`Client::send_requiring`], failing at once when the queue is full.
    pub fn try_send_requiring<P, I, T>(
        &self,
        kind: &str,
        payload: &P,
        topics: I,
    ) -> Result<(), Error>
    where
        P: Serialize + ?Sized,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let for_now = self.ready()?;
        let text = encode(kind, payload)?;
        let topics: Box<[String]> = topics.into_iter().map(Into::into).collect();
        self.frames
            .try_send(for_now.requiring(text, topics))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Full,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }

    /// Sends JSON text as it is: a message the application built itself.
    pub async fn send_raw(&self, json: impl Into<String>) -> Result<(), Error> {
        let for_now = self.ready()?;
        self.frames
            .send(for_now.frame(json.into()))
            .await
            .map_err(|_| Error::Closed)
    }

    /// Sends a signed message `{"t": kind, "p": payload}`: signed with
    /// [`Config::signing_key`] for the server's signing key, so whoever
    /// keeps it can show who sent it. `n` names it once, 1 to 128
    /// characters (an order id, say; a random one when `None`). Returns the
    /// `n` it went with: the server's signed `receipt`, or its error
    /// `SIGNATURE_REFUSED`, names it.
    #[cfg(feature = "aws-lc")]
    pub async fn send_signed<P: Serialize + ?Sized>(
        &self,
        kind: &str,
        payload: &P,
        n: Option<&str>,
    ) -> Result<String, Error> {
        let for_now = self.ready()?;
        let audience = self
            .shared
            .server_kid
            .lock()
            .ok()
            .and_then(|kid| kid.clone())
            .ok_or_else(|| Error::Signing("the server named no signing key".to_owned()))?;
        let payload =
            serde_json::value::to_raw_value(payload).map_err(|e| Error::Encode(e.to_string()))?;
        let n = match n {
            Some(n)
                if n.is_empty() || n.chars().count() > wse::protocol::signing::MAX_NONCE_LEN =>
            {
                return Err(Error::Signing(format!(
                    "n must be 1 to {} characters",
                    wse::protocol::signing::MAX_NONCE_LEN
                )));
            }
            Some(n) => n.to_owned(),
            None => {
                let mut bytes = [0u8; 16];
                wse::protocol::crypto::fill_random(&mut bytes)
                    .map_err(|_| Error::Signing("no random bytes".to_owned()))?;
                bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
            }
        };
        let text = self
            .keys
            .sign(kind, &payload, &audience, &n, crate::session::now_ms())?;
        // Signed for this connection: it goes on it or not at all.
        self.frames
            .send(for_now.live(text))
            .await
            .map_err(|_| Error::Closed)?;
        Ok(n)
    }

    /// Ends the connection for `reason` (`"background"`, `"offline"`,
    /// ...) and makes no attempt while any reason holds: topics and
    /// positions stay, and what is queued goes out first. The last reason
    /// cleared with [`resume`](Self::resume), the client connects at once
    /// and recovers what it missed.
    pub fn pause(&self, reason: impl Into<String>) -> Result<(), Error> {
        self.control(Control::Pause(reason.into()))
    }

    /// Clears `reason`; once none is left the client connects again.
    pub fn resume(&self, reason: impl Into<String>) -> Result<(), Error> {
        self.control(Control::Resume(reason.into()))
    }

    /// Subscribes to topics, on this connection and every later one; after
    /// a reconnect the client asks for them from where it left off.
    pub fn subscribe<I, T>(&self, topics: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        delivery::lock(&self.gate).track(&topics);
        self.control(Control::Subscribe(topics))
    }

    /// Unsubscribes, and forgets where the client stood in the topics.
    pub fn unsubscribe<I, T>(&self, topics: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        delivery::lock(&self.gate).untrack(&topics);
        self.control(Control::Unsubscribe(topics))
    }

    /// Each topic held: whether the server took it, and whether its
    /// messages run without a break.
    pub fn topic_states(&self) -> Vec<TopicState> {
        delivery::lock(&self.gate).topic_states()
    }

    /// The consumer applied `message` (a topic message): the checkpoint
    /// moves on through what was applied without a hole. A message handed
    /// out before the events showed another user counts for nobody. An
    /// [`OwnedMessage`](crate::OwnedMessage) commits as `owned.as_message()`.
    pub fn commit(&self, message: &Message<'_>) {
        if let (Some(topic), Some(at)) = (message.topic(), message.position()) {
            self.commit_at(topic, at.epoch, at.offset, message.generation);
        }
    }

    pub(crate) fn commit_at(&self, topic: &str, epoch: u32, offset: u64, generation: u32) {
        delivery::lock(&self.gate).commit(topic, epoch, offset, generation);
    }

    /// What the consumer committed, bound to the realm, the user, the
    /// consumer and the schema: to save with the application's own state and
    /// give back as [`Delivery::checkpoint`](crate::Delivery::checkpoint).
    pub fn checkpoint(&self) -> String {
        delivery::lock(&self.gate).checkpoint()
    }

    /// recover-or-resync: the application brought the topics of `round`
    /// ([`Event::Resync`](crate::Event::Resync)) up to date; what they held
    /// comes next.
    pub fn resynced(&self, round: u64) -> Result<(), Error> {
        delivery::lock(&self.gate).resynced(round);
        self.wake()
    }

    /// Bring `topics` up to date: in recover-or-resync a resync round asks
    /// for it; in best effort the application did so itself, and they read
    /// continuous again.
    pub fn resync<I, T>(&self, topics: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        delivery::lock(&self.gate).resync(&topics);
        self.wake()
    }

    /// What the gate made goes to the application, and the sends that wait
    /// for their topics are looked at again.
    fn wake(&self) -> Result<(), Error> {
        if let Wake::Reader(reader) = &self.wake {
            // A stream nobody reads any more has nobody to tell.
            let _ = reader.send(());
        }
        self.control(Control::Drain)
    }

    /// Drops the connection and makes it again at once.
    pub fn reconnect(&self) -> Result<(), Error> {
        self.control(Control::Reconnect)
    }

    /// The token for the next connection attempt.
    pub fn set_token(&self, token: impl Into<String>) {
        if let Ok(mut current) = self.shared.token.lock() {
            *current = Some(token.into());
        }
    }

    /// Closes the connection for good, and waits until it is.
    pub async fn close(&self) {
        let _ = self.control.send(Control::Close);
        self.closed().await;
    }

    /// Closes the client, giving what is queued `deadline` to go out: past
    /// it the connection is dropped where it stands (a link that takes
    /// nothing holds a close for as long as the system keeps the
    /// connection).
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn close_within(&self, deadline: std::time::Duration) {
        let _ = self.control.send(Control::Close);
        if tokio::time::timeout(deadline, self.closed()).await.is_err() {
            if let Some(stop) = self.shared.stop.lock().ok().and_then(|slot| slot.clone()) {
                stop.abort();
            }
            self.closed().await;
        }
    }

    /// Waits until the client stops for good; the reason, unless the
    /// application closed it.
    pub async fn closed(&self) -> Option<Error> {
        let mut status = self.status.clone();
        let closed = status
            .wait_for(|s| matches!(s, Status::Closed(_)))
            .await
            .map(|s| s.clone());
        match closed {
            Ok(Status::Closed(error)) => error,
            _ => None,
        }
    }

    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }

    /// The server took the current connection: messages go out.
    pub fn is_connected(&self) -> bool {
        self.shared.ready.load(Ordering::Acquire)
    }

    /// The current connection's id, as the server named it.
    pub fn connection_id(&self) -> Option<String> {
        let connection = self.shared.connection.lock().ok()?;
        connection.as_ref().map(|(id, _)| id.clone())
    }

    /// The user the server took the connection for.
    pub fn user_id(&self) -> Option<String> {
        let connection = self.shared.connection.lock().ok()?;
        connection.as_ref().and_then(|(_, user)| user.clone())
    }

    /// The user and connection a message is sent for, read before the flag:
    /// one taken in between leaves the message with those before it, which
    /// it does not go out for.
    fn ready(&self) -> Result<SentFor, Error> {
        let sent_for = SentFor {
            user: self.shared.users.load(Ordering::Acquire),
            connection: self.shared.connections.load(Ordering::Acquire),
        };
        if self.shared.ready.load(Ordering::Acquire) {
            Ok(sent_for)
        } else if matches!(*self.status.borrow(), Status::Closed(_)) {
            Err(Error::Closed)
        } else {
            Err(Error::NotConnected)
        }
    }

    fn control(&self, control: Control) -> Result<(), Error> {
        self.control.send(control).map_err(|_| Error::Closed)
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("status", &self.status())
            .field("connection_id", &self.connection_id())
            .finish()
    }
}

/// `{"t": kind, "p": payload}` in one pass.
fn encode<P: Serialize + ?Sized>(kind: &str, payload: &P) -> Result<String, Error> {
    #[derive(Serialize)]
    struct Out<'a, P: ?Sized> {
        t: &'a str,
        p: &'a P,
    }
    serde_json::to_string(&Out {
        t: kind,
        p: payload,
    })
    .map_err(|e| Error::Encode(e.to_string()))
}
