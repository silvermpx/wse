//! The Python callbacks of callback mode (the alternative to drain mode).

use pyo3::prelude::*;

pub(in crate::wse::server) fn fire_on_connect(callback: &Py<PyAny>, conn_id: &str, cookies: &str) {
    Python::try_attach(|py| {
        if let Err(e) = callback.call1(py, (conn_id, cookies)) {
            tracing::warn!("[WSE] on_connect error: {e}");
        }
    });
}

pub(in crate::wse::server) fn fire_on_disconnect(callback: &Py<PyAny>, conn_id: &str) {
    Python::try_attach(|py| {
        if let Err(e) = callback.call1(py, (conn_id,)) {
            tracing::warn!("[WSE] on_disconnect error: {e}");
        }
    });
}
