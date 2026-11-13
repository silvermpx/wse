//! Sending to one connection and broadcasting to all, to a topic or to the cluster.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use flate2::Compression;
use flate2::write::ZlibEncoder;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio_tungstenite::tungstenite::protocol::Message;
use uuid::Uuid;

use super::RustWSEServer;
use super::convert::pyobj_to_json;
use crate::wse::msgpack::serde_json_to_rmpv;
use crate::wse::server::ServerCommand;
use crate::wse::server::messages::{build_server_ready, inject_category, message_category};

impl RustWSEServer {
    pub(super) fn accept_connection_impl(&self, conn_id: &str, user_id: &str) -> PyResult<bool> {
        if self.shared.jwt_config.is_some() {
            return Err(PyRuntimeError::new_err(
                "this server validates tokens itself (jwt_secret) and accepts connections on \
                 its own; accept_connection() is for applications that authenticate them",
            ));
        }
        let tx = self.command_tx()?;
        // Its activity entry lives from registration to cleanup.
        if !self.shared.conn_last_activity.contains_key(conn_id) {
            return Ok(false);
        }
        if let Some(ref pm) = self.shared.presence {
            pm.register_connection(conn_id, user_id);
            // Gone meanwhile: its cleanup may have run before the register.
            if !self.shared.conn_last_activity.contains_key(conn_id) {
                pm.remove_connection(conn_id);
                return Ok(false);
            }
        }
        let ready = build_server_ready(conn_id, user_id, self.shared.recovery.is_some());
        tx.send(ServerCommand::SendText {
            conn_id: conn_id.to_owned(),
            data: ready,
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(true)
    }

    pub(super) fn send_impl(&self, py: Python<'_>, conn_id: &str, data: &str) -> PyResult<()> {
        let tx = self.command_tx()?;
        tx.send_waiting(
            py,
            ServerCommand::SendText {
                conn_id: conn_id.to_owned(),
                data: data.to_owned(),
            },
        )
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn send_bytes_impl(
        &self,
        py: Python<'_>,
        conn_id: &str,
        data: &[u8],
    ) -> PyResult<()> {
        let tx = self.command_tx()?;
        tx.send_waiting(
            py,
            ServerCommand::SendBytes {
                conn_id: conn_id.to_owned(),
                data: data.to_vec(),
            },
        )
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn send_event_impl(
        &self,
        py: Python<'_>,
        conn_id: &str,
        event: &Bound<'_, PyDict>,
        compression_threshold: usize,
    ) -> PyResult<usize> {
        let cmd_tx = self.command_tx()?;
        // A connection that is gone (or never was) gets nothing.
        if !self.shared.conn_last_activity.contains_key(conn_id) {
            return Ok(0);
        }

        // Dedup: skip if this event ID was already sent
        if let Ok(Some(id_val)) = event.get_item("id")
            && let Ok(id_str) = id_val.extract::<String>()
        {
            // Dedup is per (connection, id): the SAME event id delivered to a
            // DIFFERENT connection is a legitimate distinct send and must not be
            // dropped (the set was previously keyed by id alone, so send_event to
            // conn B silently returned 0 if conn A had already received that id).
            let key = format!("{conn_id}\0{id_str}");
            let mut dedup = self.dedup.lock().unwrap();
            if !dedup.seen.insert(key.clone()) {
                return Ok(0); // duplicate to THIS connection, skip serialize+send
            }
            dedup.queue.push_back(key);
            while dedup.queue.len() > dedup.max_entries {
                if let Some(old) = dedup.queue.pop_front() {
                    dedup.seen.remove(&old);
                }
            }
        }

        // Build serde_json map from PyDict: c first, t second, v last
        let mut map = serde_json::Map::new();
        let mut msg_type = String::new();

        // Extract 't' value (need it to determine category)
        if let Ok(Some(t_val)) = event.get_item("t")
            && let Ok(s) = t_val.extract::<String>()
        {
            msg_type = s;
        }

        // c first (category)
        let cat = message_category(&msg_type);
        map.insert("c".to_string(), serde_json::Value::String(cat.to_string()));

        // t second
        if !msg_type.is_empty() {
            map.insert("t".to_string(), serde_json::Value::String(msg_type.clone()));
        }

        // All other keys (skip t, v, c)
        for (k, v) in event.iter() {
            let key = match k.extract::<String>() {
                Ok(s) => s,
                Err(_) => continue,
            };
            if key == "t" || key == "v" || key == "c" {
                continue;
            }
            map.insert(key, pyobj_to_json(&v)?);
        }

        // Envelope: id, seq, ts
        if !map.contains_key("id") {
            map.insert(
                "id".to_string(),
                serde_json::Value::String(Uuid::now_v7().to_string()),
            );
        }

        static GLOBAL_SEQ: AtomicU64 = AtomicU64::new(0);
        if !map.contains_key("seq") {
            map.insert(
                "seq".to_string(),
                serde_json::Value::Number(GLOBAL_SEQ.fetch_add(1, Ordering::Relaxed).into()),
            );
        }

        if !map.contains_key("ts") {
            let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            map.insert("ts".to_string(), serde_json::Value::String(ts));
        }

        // v goes last
        map.insert("v".to_string(), serde_json::Value::Number(1.into()));

        // Check if this connection wants msgpack (lock-free DashMap read)
        let use_msgpack = self
            .shared
            .conn_formats
            .get(conn_id)
            .map(|v| *v)
            .unwrap_or(false);

        // Check if this connection has E2E encryption
        // Encrypted connections are sealed by the server on the way out, and
        // never compressed: compression would leak through the length.
        let e2e = self.shared.conn_encryption.contains_key(conn_id)
            || self.shared.conn_sealed.contains(conn_id);
        // The client's own compression wish, unless the call turns it off.
        let client_threshold = self.shared.conn_compression.get(conn_id).map(|t| *t);

        let body: Vec<u8> = if use_msgpack {
            // Msgpack: M: prefix + rmpv serialize (binary frame)
            let rmpv_val = serde_json_to_rmpv(&serde_json::Value::Object(map));
            let mut buf = Vec::with_capacity(256);
            buf.extend_from_slice(b"M:");
            rmpv::encode::write_value(&mut buf, &rmpv_val)
                .map_err(|e| PyRuntimeError::new_err(format!("Msgpack error: {e}")))?;
            buf
        } else {
            let json = serde_json::to_vec(&serde_json::Value::Object(map))
                .map_err(|e| PyRuntimeError::new_err(format!("JSON error: {e}")))?;
            let threshold = client_threshold.unwrap_or(compression_threshold);
            if !e2e && compression_threshold > 0 && threshold > 0 && json.len() > threshold {
                // Compressed: C: prefix + zlib (binary frame)
                let mut encoder = ZlibEncoder::new(Vec::from(&b"C:"[..]), Compression::fast());
                encoder
                    .write_all(&json)
                    .map_err(|e| PyRuntimeError::new_err(format!("Compress error: {e}")))?;
                encoder
                    .finish()
                    .map_err(|e| PyRuntimeError::new_err(format!("Compress finish: {e}")))?
            } else {
                // JSON as a binary frame (skips UTF-8 validation overhead)
                json
            }
        };
        let byte_count = body.len();
        cmd_tx
            .send_waiting(
                py,
                ServerCommand::SendPrebuilt {
                    conn_id: conn_id.to_owned(),
                    message: Message::Binary(body.into()),
                },
            )
            .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(byte_count)
    }

    pub(super) fn broadcast_all_impl(&self, py: Python<'_>, data: &str) -> PyResult<()> {
        let tx = self.command_tx()?;
        let data = inject_category(data);
        tx.send_waiting(py, ServerCommand::BroadcastText { data })
            .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn broadcast_all_bytes_impl(&self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        let tx = self.command_tx()?;
        tx.send_waiting(
            py,
            ServerCommand::BroadcastBytes {
                data: data.to_vec(),
            },
        )
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn broadcast_local_impl(
        &self,
        py: Python<'_>,
        topic: &str,
        data: &str,
    ) -> PyResult<()> {
        let tx = self.command_tx()?;
        let data = inject_category(data);
        tx.send_waiting(
            py,
            ServerCommand::BroadcastLocal {
                topic: topic.to_owned(),
                data,
                skip_recovery: false,
                publish: false,
            },
        )
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn broadcast_impl(&self, py: Python<'_>, topic: &str, data: &str) -> PyResult<()> {
        let data = inject_category(data);

        // The command task stamps the message (recovery position) and fans it
        // out locally, then publishes the same stamped bytes to cluster peers:
        // every node delivers one wire form per message, with one position.
        self.command_tx()?
            .send_waiting(
                py,
                ServerCommand::BroadcastLocal {
                    topic: topic.to_owned(),
                    data,
                    skip_recovery: false,
                    publish: true,
                },
            )
            .map_err(|_| PyRuntimeError::new_err("Server command channel closed"))?;
        Ok(())
    }
}
