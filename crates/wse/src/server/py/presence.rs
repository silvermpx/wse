//! Presence queries and presence data updates.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::RustWSEServer;
use super::convert::{json_to_pyobj, pyobj_to_json};

impl RustWSEServer {
    pub(super) fn presence_impl(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);

        if let Some(ref pm) = self.shared.presence {
            for (user_id, data, conn_count) in pm.query(topic) {
                let user_dict = PyDict::new(py);
                user_dict.set_item("data", json_to_pyobj(py, &data))?;
                user_dict.set_item("connections", conn_count)?;
                result.set_item(user_id.as_str(), user_dict)?;
            }
        }

        Ok(result.into())
    }

    pub(super) fn presence_stats_impl(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);
        if let Some(ref pm) = self.shared.presence {
            let (users, conns) = pm.stats(topic);
            result.set_item("num_users", users)?;
            result.set_item("num_connections", conns)?;
        } else {
            result.set_item("num_users", 0)?;
            result.set_item("num_connections", 0)?;
        }
        Ok(result.into())
    }

    pub(super) fn update_presence_impl(
        &self,
        conn_id: &str,
        data: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        if self.shared.presence.is_some() {
            let new_data = pyobj_to_json(data.as_any())?;
            if !self.shared.update_presence(conn_id, &new_data) {
                return Err(PyRuntimeError::new_err(
                    "Failed to update presence data (size limit or unknown connection)",
                ));
            }
        }
        Ok(())
    }
}
