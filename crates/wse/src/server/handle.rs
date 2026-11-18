//! A server for Rust applications: started on the application's own tokio
//! runtime, stopped from it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;

use super::config::{Config, ConfigError};
use super::handler::Handler;
use super::publisher::{Error, Publisher};
use super::run;
use super::state::{ServerCommand, SharedState};

/// A WSE server: its configuration and state, from `new` until it is
/// dropped. It serves on a listener once started, and can start again after
/// `stop`. It speaks plain WebSocket (`ws://`): `wss://` is a reverse proxy
/// or a load balancer in front of it.
///
/// ```no_run
/// # async fn run(handler: impl wse::Handler) -> Result<(), Box<dyn std::error::Error>> {
/// let server = wse::Server::new(wse::Config::default())?;
/// let listener = tokio::net::TcpListener::bind("127.0.0.1:5006").await?;
/// server.start(listener, handler)?;
/// server.publisher().broadcast(r#"{"t":"hello"}"#)?;
/// server.stop().await;
/// # Ok(()) }
/// ```
pub struct Server {
    shared: Arc<SharedState>,
    running: Arc<AtomicBool>,
    /// The running server's task; held by `stop` until the run has ended.
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Server {
    /// A server for `config`, checked as a whole now.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        Ok(Self {
            shared: Arc::new(config.into_state()?),
            running: Arc::new(AtomicBool::new(false)),
            task: tokio::sync::Mutex::new(None),
        })
    }

    /// What the application reaches the server with. It works across
    /// restarts; while the server is stopped, sends fail with
    /// `Error::Stopped`.
    pub fn publisher(&self) -> Publisher {
        Publisher::new(Arc::clone(&self.shared))
    }

    /// Serve on `listener`, telling `handler` of every connection: the
    /// server's tasks run on the current tokio runtime until `stop`.
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime.
    pub fn start<H: Handler>(&self, listener: TcpListener, handler: H) -> Result<(), Error> {
        // A `stop` still under way holds the lock: the server runs until it
        // has returned.
        let Ok(mut task) = self.task.try_lock() else {
            return Err(Error::Running);
        };
        if task.as_ref().is_some_and(|task| !task.is_finished()) {
            return Err(Error::Running);
        }
        let handler = Arc::new(handler);
        let port = listener.local_addr().map_or(0, |addr| addr.port());
        let started = run::start(&self.shared, &handler, port);
        self.running.store(true, Ordering::SeqCst);
        *task = Some(tokio::spawn(run::serve(
            Arc::clone(&self.shared),
            started,
            listener,
            handler,
            Arc::clone(&self.running),
        )));
        Ok(())
    }

    /// Whether the server is serving.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Stop: every connection is closed (1001, going away), the cluster
    /// links too. The server's tasks end, and the handler's `disconnect`
    /// calls are made, before this returns.
    pub async fn stop(&self) {
        let mut task = self.task.lock().await;
        let Some(running) = task.as_mut() else {
            return;
        };
        self.shut_down();
        // Awaited in its place: a stop given up while it waits leaves the
        // run to the next one, and `start` refuses until it ended.
        let _ = running.await;
        *task = None;
        // The run cleans up after itself; this covers one that panicked.
        run::stopped(&self.shared);
    }

    /// The run ends once its command task took what came before: it leaves
    /// the cluster then, after every publish it took.
    fn shut_down(&self) {
        let _ = self.publisher().command(ServerCommand::Shutdown);
        self.running.store(false, Ordering::SeqCst);
    }

    /// Close every connection with `code` and `reason`, take no new one, and
    /// wait up to `timeout` for the clients to go; the rest are then dropped.
    /// Returns how many had to be dropped. The server keeps running: `stop`
    /// ends it.
    pub async fn drain(&self, code: u16, reason: &str, timeout: Duration) -> Result<usize, Error> {
        let publisher = self.publisher();
        // No connection registers from here on, before the first count: a
        // registration checks this under the connections' write lock, and
        // the count is taken under their read lock.
        self.shared.draining.store(true, Ordering::SeqCst);
        publisher.command(ServerCommand::Drain {
            close_code: code,
            close_reason: reason.to_owned(),
        })?;
        // A timeout too far to count is no deadline at all.
        let deadline = tokio::time::Instant::now().checked_add(timeout);
        loop {
            let left = self.shared.connections.read().len();
            if left == 0 {
                return Ok(0);
            }
            if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                let _ = publisher.command(ServerCommand::AbortAll);
                return Ok(left);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Connections registered now.
    pub fn connection_count(&self) -> usize {
        self.shared.connection_count.load(Ordering::Relaxed)
    }
}

impl Drop for Server {
    /// Dropped while serving: the server is told to stop. Its run ends by
    /// itself, and what it set up goes with it.
    fn drop(&mut self) {
        if self.task.get_mut().is_some() {
            self.shut_down();
        }
    }
}
