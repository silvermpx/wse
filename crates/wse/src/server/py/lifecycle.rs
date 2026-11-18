//! Lifecycle of the server: callbacks, start, graceful drain, stop. The
//! server runs on a runtime of its own, on a thread of its own (`run`).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::RwLockExt;
use tokio::net::TcpListener;

use super::RustWSEServer;
use crate::pylock::lock;
use crate::server::run;
use crate::server::{Publisher, ServerCommand};

impl RustWSEServer {
    pub(super) fn set_callbacks_impl(
        &self,
        py: Python<'_>,
        on_connect: Py<PyAny>,
        on_message: Py<PyAny>,
        on_disconnect: Py<PyAny>,
        on_signed_message: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        if !on_connect.bind(py).is_callable()
            || !on_message.bind(py).is_callable()
            || !on_disconnect.bind(py).is_callable()
            || on_signed_message
                .as_ref()
                .is_some_and(|cb| !cb.bind(py).is_callable())
        {
            return Err(PyRuntimeError::new_err("All callbacks must be callable"));
        }
        // Waited for detached (`write_py_attached`), so a reader on another
        // thread is never kept from its attach. The callbacks being replaced
        // are dropped after the guards: that may run Python code.
        let shared = self.callbacks.clone();
        let old_connect = shared
            .on_connect
            .write_py_attached(py)
            .unwrap()
            .replace(Arc::new(on_connect));
        let old_message = shared
            .on_message
            .write_py_attached(py)
            .unwrap()
            .replace(Arc::new(on_message));
        let old_disconnect = shared
            .on_disconnect
            .write_py_attached(py)
            .unwrap()
            .replace(Arc::new(on_disconnect));
        let old_signed = std::mem::replace(
            &mut *shared.on_signed_message.write_py_attached(py).unwrap(),
            on_signed_message.map(Arc::new),
        );
        drop((old_connect, old_message, old_disconnect, old_signed));
        Ok(())
    }

    pub(super) fn start_impl(&self, py: Python<'_>) -> PyResult<()> {
        let mut server_thread = lock(py, &self.lifecycle);
        if server_thread
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            if self.running.load(Ordering::SeqCst) {
                return Err(PyRuntimeError::new_err("Server is already running"));
            }
            // Stopped from one of its callbacks, the run still ends: a
            // callback of that run cannot wait for it.
            let here = tokio::runtime::Handle::try_current().ok().map(|h| h.id());
            if here.is_some() && here == *self.stopping.lock() {
                return Err(PyRuntimeError::new_err(
                    "the server is still stopping: a callback of the run that stops \
                     cannot start it again (start it from another thread)",
                ));
            }
        }
        // The last run's thread is waited for, detached, whether it ended by
        // itself (it could not bind) or still ends (stopped from a callback):
        // nothing it does as it ends reaches the new run.
        if let Some(handle) = server_thread.take() {
            let _ = py.detach(move || handle.join());
        }
        *self.stopping.lock() = None;
        // The engine's log targets start with its crate name, `wse`.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("wse=info")),
            )
            .with_target(false)
            .try_init();
        // The runtime and the listener are made here, so that a port in use
        // or a runtime that cannot start is an error of start().
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("wse-runtime")
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("cannot start the runtime: {e}")))?;
        let listener = std::net::TcpListener::bind((self.host.as_str(), self.port))
            .and_then(|listener| {
                listener.set_nonblocking(true)?;
                let _runtime = rt.enter();
                TcpListener::from_std(listener)
            })
            .map_err(|e| {
                pyo3::exceptions::PyOSError::new_err(format!(
                    "cannot listen on {}:{}: {e}",
                    self.host, self.port
                ))
            })?;
        tracing::info!("[WSE] Listening on {}:{}", self.host, self.port);
        // What the application hears of its connections: drain or callbacks.
        let handler = Arc::new(super::handler::PyHandler {
            state: self.shared.clone(),
            callbacks: self.callbacks.clone(),
        });
        // The lanes run from here on, on the server's runtime.
        let started = {
            let _runtime = rt.enter();
            let port = listener.local_addr().map_or(0, |addr| addr.port());
            run::start(&self.shared, &handler, port)
        };
        self.cmd_tx
            .store(Some(Arc::new(Publisher::waiting(Arc::clone(&self.shared)))));
        let running = self.running.clone();
        let shared = self.shared.clone();
        let rt_handle = rt.handle().clone();

        self.running.store(true, Ordering::SeqCst);
        let handle = thread::Builder::new()
            .name("wse-server".into())
            .spawn(move || {
                rt.block_on(run::serve(shared, started, listener, handler, running));
                // Callbacks still running get five seconds; a callback stuck
                // in slow I/O does not hold the shutdown forever.
                rt.shutdown_timeout(Duration::from_secs(5));
            })
            .map_err(|e| {
                self.running.store(false, Ordering::SeqCst);
                self.cmd_tx.store(None);
                PyRuntimeError::new_err(format!("Spawn error: {e}"))
            })?;
        *self.rt_handle.write() = Some(rt_handle);
        *server_thread = Some(handle);
        Ok(())
    }

    pub(super) fn drain_impl(
        &self,
        py: Python<'_>,
        close_code: u16,
        close_reason: &str,
        timeout: u64,
    ) -> PyResult<usize> {
        let tx = self.command_tx()?;
        // No connection registers from here on, before the first count: a
        // registration checks this under the connections' write lock, and
        // the count is taken under their read lock.
        self.shared.draining.store(true, Ordering::SeqCst);
        tx.command(ServerCommand::Drain {
            close_code,
            close_reason: close_reason.to_owned(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        // Wait detached for the clients to go; the rest are closed at the
        // deadline.
        let shared = self.shared.clone();
        let remaining = py.detach(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(timeout);
            loop {
                let left = shared.connections.read().len();
                if left == 0 || std::time::Instant::now() >= deadline {
                    return left;
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        if remaining > 0 {
            tracing::warn!("[WSE] Drain timeout: closing the {remaining} remaining connections");
            let _ = tx.command(ServerCommand::AbortAll);
        } else {
            tracing::info!("[WSE] Drain complete: all connections closed");
        }
        Ok(remaining)
    }

    /// Whether the server runs on the current thread: a callback, or code a
    /// callback runs.
    fn on_own_runtime(&self) -> bool {
        let own = self.runtime().map(|handle| handle.id());
        own.is_some() && tokio::runtime::Handle::try_current().ok().map(|h| h.id()) == own
    }

    pub(super) fn stop_impl(&self, py: Python<'_>) -> PyResult<()> {
        let mut server_thread = lock(py, &self.lifecycle);
        let Some(handle) = server_thread.take() else {
            return Ok(());
        };
        let on_own_runtime = self.on_own_runtime();
        // The run leaves the cluster as it ends, after every publish its
        // command task took.
        if let Ok(tx) = self.command_tx() {
            let _ = tx.command(ServerCommand::Shutdown);
        }
        self.running.store(false, Ordering::SeqCst);
        // From a callback the server thread is not joined: it waits for its
        // callbacks, this one included. It stops by itself, cleaning up as
        // it ends, and the next start() (or stop() from another thread)
        // waits for it.
        let panicked = if on_own_runtime {
            *self.stopping.lock() = self.runtime().map(|h| h.id());
            *server_thread = Some(handle);
            false
        } else {
            // Detached while the server closes its connections (up to three
            // seconds) and leaves the cluster: other Python threads keep
            // running, and a start() from another thread waits for the
            // `lifecycle` lock without holding up the interpreter.
            let panicked = py.detach(move || handle.join().is_err());
            run::stopped(&self.shared);
            panicked
        };
        self.cmd_tx.store(None);
        *self.rt_handle.write() = None;
        drop(server_thread);
        if panicked {
            return Err(PyRuntimeError::new_err("Server thread panicked"));
        }
        Ok(())
    }
}
