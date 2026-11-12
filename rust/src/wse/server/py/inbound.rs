//! Drain mode: hands the queued inbound events to Python in batches.

use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};

use super::RustWSEServer;
use super::convert::json_to_pyobj;
use crate::wse::server::state::InboundEvent;

impl RustWSEServer {
    pub(super) fn drain_inbound_impl(
        &self,
        py: Python<'_>,
        max_count: usize,
        timeout_ms: u64,
    ) -> PyResult<Py<PyList>> {
        // Collect events with GIL released (channel recv happens without GIL)
        let events: Vec<InboundEvent> = py.detach(|| {
            let mut events = Vec::with_capacity(max_count.min(4096));

            // First event: block with timeout (efficient — no busy wait)
            match self
                .shared
                .inbound_rx
                .recv_timeout(Duration::from_millis(timeout_ms))
            {
                Ok(event) => events.push(event),
                Err(_) => return events, // Timeout or disconnected
            }

            // Drain remaining without blocking (batch read)
            while events.len() < max_count {
                match self.shared.inbound_rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(_) => break,
                }
            }

            events
        });

        // Convert all events to Python in one GIL hold
        let list = PyList::empty(py);
        for event in &events {
            match event {
                InboundEvent::Connect { conn_id, cookies } => {
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "connect".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            cookies.as_str().into_pyobject(py).unwrap().into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::AuthConnect { conn_id, user_id } => {
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "auth_connect".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            user_id.as_str().into_pyobject(py).unwrap().into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::Message { conn_id, value } => {
                    let py_data = json_to_pyobj(py, value);
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "msg".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            py_data.clone_ref(py).into_bound(py).into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::RawText { conn_id, text } => {
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "raw".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            text.as_str().into_pyobject(py).unwrap().into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::Binary { conn_id, data } => {
                    let py_bytes = PyBytes::new(py, data);
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "bin".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            py_bytes.into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::Disconnect { conn_id } => {
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "disconnect".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            py.None().into_bound(py).into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::PresenceJoin {
                    topic,
                    user_id,
                    data,
                } => {
                    let payload = PyDict::new(py);
                    payload.set_item("topic", topic.as_str())?;
                    payload.set_item("user_id", user_id.as_str())?;
                    payload.set_item("data", json_to_pyobj(py, data))?;
                    let tuple = PyTuple::new(
                        py,
                        &[
                            PyString::new(py, "presence_join").into_any(),
                            py.None().into_bound(py).into_any(),
                            payload.into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::PresenceLeave {
                    topic,
                    user_id,
                    data,
                } => {
                    let payload = PyDict::new(py);
                    payload.set_item("topic", topic.as_str())?;
                    payload.set_item("user_id", user_id.as_str())?;
                    payload.set_item("data", json_to_pyobj(py, data))?;
                    let tuple = PyTuple::new(
                        py,
                        &[
                            PyString::new(py, "presence_leave").into_any(),
                            py.None().into_bound(py).into_any(),
                            payload.into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
            }
        }
        Ok(list.unbind())
    }
}
