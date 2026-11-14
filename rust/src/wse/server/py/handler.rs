//! The server's handler for Python: drain mode (the application takes the
//! events in batches) or callback mode (its callbacks, called one after
//! another for each connection).

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use super::callbacks::{fire_on_connect, fire_on_disconnect};
use super::convert::json_to_pyobj;
use crate::wse::server::handler::{Connect, Handler, Inbound};
use crate::wse::server::state::{InboundEvent, SharedState, inbound_cost};

/// The callbacks of callback mode, set by `set_callbacks()`.
#[derive(Default)]
pub(crate) struct PyCallbacks {
    pub(crate) on_connect: std::sync::RwLock<Option<Arc<Py<PyAny>>>>,
    pub(crate) on_message: std::sync::RwLock<Option<Arc<Py<PyAny>>>>,
    pub(crate) on_disconnect: std::sync::RwLock<Option<Arc<Py<PyAny>>>>,
    pub(crate) on_signed_message: std::sync::RwLock<Option<Arc<Py<PyAny>>>>,
}

/// Max concurrent on_message callbacks (blocking tasks) per connection in
/// callback mode. Bounds inbound flooding from exhausting the blocking pool.
const MAX_INFLIGHT_CALLBACKS: usize = 256;

/// A callback job of callback mode; it attaches to Python itself.
type CallbackJob = Box<dyn FnOnce() + Send>;

/// The callbacks of one connection, run one after another on the blocking
/// pool in the order of its events: on_connect first, its messages in order,
/// on_disconnect last. A drainer runs only while jobs wait, so an idle
/// connection holds no thread.
pub(crate) struct CallbackLane {
    queue: parking_lot::Mutex<LaneQueue>,
}

#[derive(Default)]
struct LaneQueue {
    jobs: VecDeque<CallbackJob>,
    draining: bool,
}

impl CallbackLane {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            queue: parking_lot::Mutex::new(LaneQueue::default()),
        })
    }

    /// Queue `job`; false (and nothing queued) when `limit` jobs wait already.
    fn push(self: &Arc<Self>, job: CallbackJob, limit: Option<usize>) -> bool {
        let mut queue = self.queue.lock();
        if limit.is_some_and(|limit| queue.jobs.len() >= limit) {
            return false;
        }
        queue.jobs.push_back(job);
        if !queue.draining {
            queue.draining = true;
            let lane = Arc::clone(self);
            tokio::task::spawn_blocking(move || lane.drain());
        }
        true
    }

    fn drain(&self) {
        loop {
            let job = {
                let mut queue = self.queue.lock();
                match queue.jobs.pop_front() {
                    Some(job) => job,
                    None => {
                        queue.draining = false;
                        return;
                    }
                }
            };
            job();
        }
    }
}

/// The handler a Python server runs its connections with.
pub(crate) struct PyHandler {
    pub(crate) state: Arc<SharedState>,
    pub(crate) callbacks: Arc<PyCallbacks>,
}

/// A connection as the Python handler keeps it: in the mode the server was
/// in when the connection came.
pub(crate) enum PyConnection {
    Drain {
        conn_id: String,
    },
    Callbacks {
        conn_id: Arc<String>,
        lane: Arc<CallbackLane>,
        on_message: Option<Arc<Py<PyAny>>>,
        on_signed_message: Option<Arc<Py<PyAny>>>,
    },
}

impl PyHandler {
    /// Queue an on_message callback. It needs a place among the connection's
    /// `MAX_INFLIGHT_CALLBACKS` waiting callbacks (a client flooding messages
    /// is shed, counted in `rate_limited_total`) and room for its `bytes` in
    /// the server's budget of messages waiting for the application, the same
    /// one drain mode uses (counted in `inbound_dropped` when there is none).
    /// False when it was shed.
    fn spawn_bounded<F>(&self, lane: &Arc<CallbackLane>, bytes: usize, work: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        let Some(room) = SharedState::callback_room(&self.state, bytes) else {
            return false;
        };
        let job: CallbackJob = Box::new(move || {
            let _room = room;
            work();
        });
        if lane.push(job, Some(MAX_INFLIGHT_CALLBACKS)) {
            return true;
        }
        self.state
            .rate_limited_total
            .fetch_add(1, Ordering::Relaxed);
        false
    }
}

impl Handler for PyHandler {
    type Connection = PyConnection;

    /// Drain mode: a connect event, refused when a stalled drain already
    /// holds too many connect and disconnect events. Callback mode: the
    /// connection's callbacks start with on_connect.
    fn connect(&self, connect: &Connect<'_>) -> Option<PyConnection> {
        if self.state.drain_mode.load(Ordering::Relaxed) {
            let conn_id = (**connect.conn_id).clone();
            let event = match connect.user_id {
                Some(user_id) => InboundEvent::AuthConnect {
                    conn_id: conn_id.clone(),
                    user_id: user_id.to_owned(),
                },
                None => InboundEvent::Connect {
                    conn_id: conn_id.clone(),
                    cookies: connect.cookies.to_owned(),
                },
            };
            return self
                .state
                .push_connect(event)
                .then_some(PyConnection::Drain { conn_id });
        }
        let lane = CallbackLane::new();
        if let Some(callback) = self.callbacks.on_connect.read().unwrap().clone() {
            let conn_id = (**connect.conn_id).clone();
            let who = connect
                .user_id
                .map_or_else(|| connect.cookies.to_owned(), str::to_owned);
            lane.push(
                Box::new(move || fire_on_connect(&callback, &conn_id, &who)),
                None,
            );
        }
        Some(PyConnection::Callbacks {
            conn_id: Arc::clone(connect.conn_id),
            lane,
            on_message: self.callbacks.on_message.read().unwrap().clone(),
            on_signed_message: self.callbacks.on_signed_message.read().unwrap().clone(),
        })
    }

    fn message(&self, conn: &mut PyConnection, message: Inbound, size: usize) -> bool {
        match conn {
            PyConnection::Drain { conn_id } => {
                let conn_id = conn_id.clone();
                let event = match message {
                    Inbound::Json(value) => InboundEvent::Message { conn_id, value },
                    Inbound::Text(text) => InboundEvent::RawText { conn_id, text },
                    Inbound::Binary(data) => InboundEvent::Binary { conn_id, data },
                };
                self.state.push_message(event, size)
            }
            PyConnection::Callbacks {
                conn_id,
                lane,
                on_message,
                ..
            } => {
                let Some(callback) = on_message else {
                    return true;
                };
                let callback = Arc::clone(callback);
                let conn_id = Arc::clone(conn_id);
                let bytes = match &message {
                    Inbound::Json(value) => inbound_cost(size, value),
                    Inbound::Text(_) | Inbound::Binary(_) => size,
                };
                self.spawn_bounded(lane, bytes, move || {
                    Python::try_attach(|py| {
                        let argument = match &message {
                            Inbound::Json(value) => json_to_pyobj(py, value),
                            Inbound::Text(text) => text
                                .as_str()
                                .into_pyobject(py)
                                .map_or_else(|_| py.None(), |text| text.into_any().unbind()),
                            Inbound::Binary(data) => PyBytes::new(py, data).into_any().unbind(),
                        };
                        if let Err(e) = callback.call1(py, (&**conn_id, argument)) {
                            tracing::warn!("[WSE] on_message error: {e}");
                        }
                    });
                })
            }
        }
    }

    /// Drain mode: a `signed` event. Callback mode: on_signed_message, or
    /// on_message with the body alone when only that one is set.
    fn signed(&self, conn: &mut PyConnection, mut message: serde_json::Value, size: usize) -> bool {
        match conn {
            PyConnection::Drain { conn_id } => self.state.push_message(
                InboundEvent::Signed {
                    conn_id: conn_id.clone(),
                    value: message,
                },
                size,
            ),
            PyConnection::Callbacks {
                conn_id,
                lane,
                on_message,
                on_signed_message,
            } => {
                let callback = match (on_signed_message, on_message) {
                    (Some(callback), _) => Arc::clone(callback),
                    (None, Some(callback)) => {
                        if let serde_json::Value::Object(fields) = &mut message {
                            fields.remove("signature");
                        }
                        Arc::clone(callback)
                    }
                    (None, None) => return false,
                };
                let conn_id = Arc::clone(conn_id);
                let bytes = inbound_cost(size, &message);
                self.spawn_bounded(lane, bytes, move || {
                    Python::try_attach(|py| {
                        let message = json_to_pyobj(py, &message);
                        if let Err(e) = callback.call1(py, (&**conn_id, message)) {
                            tracing::warn!("[WSE] signed message callback error: {e}");
                        }
                    });
                })
            }
        }
    }

    fn disconnect(&self, conn: PyConnection) {
        match conn {
            PyConnection::Drain { conn_id } => {
                self.state
                    .push_inbound(InboundEvent::Disconnect { conn_id });
            }
            PyConnection::Callbacks { conn_id, lane, .. } => {
                if let Some(callback) = self.callbacks.on_disconnect.read().unwrap().clone() {
                    let conn_id = (*conn_id).clone();
                    lane.push(
                        Box::new(move || fire_on_disconnect(&callback, &conn_id)),
                        None,
                    );
                }
            }
        }
    }
}
