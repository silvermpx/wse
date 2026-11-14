//! Sending to one connection and broadcasting to all, to a topic or to the
//! cluster: the engine's `Publisher`, with Python's arguments and errors.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::RustWSEServer;
use super::convert::pyobj_to_json;
use crate::server::{Error, Publisher};

/// An engine error as Python sees it.
pub(super) fn py_error(error: Error) -> PyErr {
    match error {
        Error::Stopped => PyRuntimeError::new_err("Channel closed"),
        Error::NoSigningKey => PyRuntimeError::new_err("sign() needs the server's signing_key"),
        Error::BadKey => PyValueError::new_err(error.to_string()),
        error => PyRuntimeError::new_err(error.to_string()),
    }
}

/// Wait without the GIL while the server's command queue is full: a
/// publisher faster than the server is held back here.
fn wait_for_room(py: Python<'_>, publisher: &Publisher) {
    if publisher.is_full() {
        py.detach(|| publisher.wait_ready());
    }
}

impl RustWSEServer {
    pub(super) fn accept_connection_impl(&self, conn_id: &str, user_id: &str) -> PyResult<bool> {
        if self.shared.jwt_config.is_some() {
            return Err(py_error(Error::AcceptsByToken));
        }
        self.command_tx()?
            .accept(conn_id, user_id)
            .map_err(py_error)
    }

    pub(super) fn sign_impl(
        &self,
        event_type: &str,
        payload: Option<&Bound<'_, PyAny>>,
        topic: Option<&str>,
    ) -> PyResult<String> {
        if self.shared.signer.is_none() {
            return Err(py_error(Error::NoSigningKey));
        }
        let payload = match payload {
            Some(p) if !p.is_none() => pyobj_to_json(p)?,
            _ => serde_json::Value::Null,
        };
        self.shared
            .sign(event_type, payload, topic)
            .map_err(py_error)
    }

    pub(super) fn set_client_key_impl(
        &self,
        conn_id: &str,
        public_key: Option<&str>,
    ) -> PyResult<bool> {
        self.shared
            .set_client_key(conn_id, public_key)
            .map_err(py_error)
    }

    pub(super) fn send_impl(&self, py: Python<'_>, conn_id: &str, data: &str) -> PyResult<()> {
        let publisher = self.command_tx()?;
        wait_for_room(py, &publisher);
        publisher.send(conn_id, data).map_err(py_error)
    }

    pub(super) fn send_bytes_impl(
        &self,
        py: Python<'_>,
        conn_id: &str,
        data: &[u8],
    ) -> PyResult<()> {
        let publisher = self.command_tx()?;
        wait_for_room(py, &publisher);
        publisher.send_bytes(conn_id, data).map_err(py_error)
    }

    pub(super) fn send_event_impl(
        &self,
        py: Python<'_>,
        conn_id: &str,
        event: &Bound<'_, PyDict>,
        compression_threshold: usize,
    ) -> PyResult<usize> {
        let publisher = self.command_tx()?;
        // A connection that is gone (or never was) gets nothing.
        if !self.shared.conn_last_activity.contains_key(conn_id) {
            return Ok(0);
        }
        let mut fields = serde_json::Map::new();
        for (key, value) in event.iter() {
            let Ok(key) = key.extract::<String>() else {
                continue;
            };
            // The envelope's own: `c` and `v` are the server's, `t` counts
            // only as text.
            match key.as_str() {
                "c" | "v" => {}
                "t" => {
                    if let Ok(t) = value.extract::<String>() {
                        fields.insert(key, serde_json::Value::String(t));
                    }
                }
                _ => {
                    fields.insert(key, pyobj_to_json(&value)?);
                }
            }
        }
        wait_for_room(py, &publisher);
        publisher
            .send_event(conn_id, fields, compression_threshold)
            .map_err(py_error)
    }

    pub(super) fn broadcast_all_impl(&self, py: Python<'_>, data: &str) -> PyResult<()> {
        let publisher = self.command_tx()?;
        wait_for_room(py, &publisher);
        publisher.broadcast(data).map_err(py_error)
    }

    pub(super) fn broadcast_all_bytes_impl(&self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        let publisher = self.command_tx()?;
        wait_for_room(py, &publisher);
        publisher.broadcast_bytes(data).map_err(py_error)
    }

    pub(super) fn broadcast_local_impl(
        &self,
        py: Python<'_>,
        topic: &str,
        data: &str,
    ) -> PyResult<()> {
        let publisher = self.command_tx()?;
        wait_for_room(py, &publisher);
        publisher.publish_local(topic, data).map_err(py_error)
    }

    pub(super) fn broadcast_impl(&self, py: Python<'_>, topic: &str, data: &str) -> PyResult<()> {
        let publisher = self.command_tx()?;
        wait_for_room(py, &publisher);
        publisher
            .publish(topic, data)
            .map_err(|_| PyRuntimeError::new_err("Server command channel closed"))
    }
}
