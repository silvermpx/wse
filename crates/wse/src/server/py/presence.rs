//! Presence queries and presence data updates.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::RustWSEServer;
use super::convert::{json_to_pyobj, pyobj_to_json};

impl RustWSEServer {
    pub(super) fn presence_impl(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);
        for member in self.engine().presence(topic) {
            let user_dict = PyDict::new(py);
            user_dict.set_item("data", json_to_pyobj(py, &member.data))?;
            user_dict.set_item("connections", member.connections)?;
            result.set_item(member.user_id.as_str(), user_dict)?;
        }
        Ok(result.into())
    }

    pub(super) fn presence_stats_impl(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);
        let (users, conns) = self.engine().presence_stats(topic);
        result.set_item("num_users", users)?;
        result.set_item("num_connections", conns)?;
        Ok(result.into())
    }

    pub(super) fn update_presence_impl(
        &self,
        conn_id: &str,
        data: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        if self.shared.presence.is_some()
            && !self
                .engine()
                .update_presence(conn_id, &pyobj_to_json(data.as_any())?)
        {
            return Err(PyRuntimeError::new_err(
                "Failed to update presence data (size limit or unknown connection)",
            ));
        }
        Ok(())
    }
}
