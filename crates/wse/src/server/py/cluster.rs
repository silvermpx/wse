//! Joining the cluster mesh, cluster TLS reload and the peer list.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use super::RustWSEServer;
use super::messaging::py_error;
use crate::server::cluster_link;
use crate::server::{ClusterSettings, ClusterTls};

impl RustWSEServer {
    pub(super) fn connect_cluster_inner(
        &self,
        peers: Vec<String>,
        tls: (Option<String>, Option<String>, Option<String>),
        listen: (Option<u16>, Option<String>, bool),
        seeds: Option<Vec<String>>,
        cluster_addr: Option<String>,
    ) -> PyResult<()> {
        let (tls_cert, tls_key, tls_ca) = tls;
        let (cluster_port, cluster_host, single_port) = listen;

        // The TLS files come from the call, or from the environment.
        let tls = match (tls_cert, tls_key, tls_ca) {
            (Some(cert), Some(key), Some(ca)) => Some(ClusterTls::new(cert, key, ca)),
            (None, None, None) => match (
                std::env::var("WSE_CLUSTER_TLS_CERT").ok(),
                std::env::var("WSE_CLUSTER_TLS_KEY").ok(),
                std::env::var("WSE_CLUSTER_TLS_CA").ok(),
            ) {
                (Some(cert), Some(key), Some(ca)) => Some(ClusterTls::new(cert, key, ca)),
                (None, None, None) => None,
                _ => {
                    return Err(PyRuntimeError::new_err(
                        "Partial TLS env vars: set all of WSE_CLUSTER_TLS_CERT, \
                         WSE_CLUSTER_TLS_KEY, WSE_CLUSTER_TLS_CA or none",
                    ));
                }
            },
            _ => {
                return Err(PyRuntimeError::new_err(
                    "Partial TLS config: provide all of tls_cert, tls_key, tls_ca or none",
                ));
            }
        };
        let settings = ClusterSettings {
            peers,
            seeds,
            cluster_addr,
            port: cluster_port,
            host: cluster_host,
            single_port,
            tls,
        };
        // On the server's runtime; a stopped server is refused by the engine.
        let runtime = self.runtime();
        let _runtime = runtime.as_ref().map(tokio::runtime::Handle::enter);
        cluster_link::connect(&self.shared, settings).map_err(py_error)
    }

    pub(super) fn reload_cluster_tls_impl(
        &self,
        cert_path: String,
        key_path: String,
        ca_path: String,
    ) -> PyResult<()> {
        self.engine()
            .reload_cluster_tls(&ClusterTls::new(cert_path, key_path, ca_path))
            .map_err(py_error)
    }

    pub(super) fn cluster_info_impl(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        // Copied out first: no map shard lock is held while Python allocates.
        let peers = self.engine().cluster_peers();
        let list = PyList::empty(py);
        for peer in &peers {
            let d = PyDict::new(py);
            d.set_item("address", &peer.address)?;
            d.set_item("instance_id", &peer.instance_id)?;
            d.set_item("capabilities", peer.capabilities)?;
            d.set_item("connected_at", peer.connected_at)?;
            list.append(d)?;
        }
        Ok(list.unbind())
    }
}
