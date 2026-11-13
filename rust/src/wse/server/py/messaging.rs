//! Sending to one connection and broadcasting to all, to a topic or to the cluster.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use flate2::Compression;
use flate2::write::ZlibEncoder;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio_tungstenite::tungstenite::protocol::Message;
use uuid::Uuid;

use super::RustWSEServer;
use super::convert::pyobj_to_json;
use crate::wse::cluster::ClusterCommand;
use crate::wse::compression::serde_json_to_rmpv;
use crate::wse::server::ServerCommand;
use crate::wse::server::e2e::encrypt_outbound;
use crate::wse::server::messages::{inject_category, message_category};
use crate::wse::server::state::PerConnRate;

impl RustWSEServer {
    pub(super) fn send_impl(&self, conn_id: &str, data: &str) -> PyResult<()> {
        let tx = self.command_tx()?;
        tx.send(ServerCommand::SendText {
            conn_id: conn_id.to_owned(),
            data: data.to_owned(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn send_bytes_impl(&self, conn_id: &str, data: &[u8]) -> PyResult<()> {
        let tx = self.command_tx()?;
        tx.send(ServerCommand::SendBytes {
            conn_id: conn_id.to_owned(),
            data: data.to_vec(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn send_event_impl(
        &self,
        _py: Python<'_>,
        conn_id: &str,
        event: &Bound<'_, PyDict>,
        compression_threshold: usize,
    ) -> PyResult<usize> {
        let cmd_tx = self.command_tx()?;

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

        // Per-connection rate limiter (token bucket) with client feedback
        {
            let mut entry = self
                .shared
                .conn_rates
                .entry(conn_id.to_owned())
                .or_insert_with(|| PerConnRate {
                    tokens: self.shared.rate_capacity,
                    last_refill: std::time::Instant::now(),
                    last_rate_limited: None,
                    last_warning: None,
                });
            let rate = entry.value_mut();
            let elapsed = rate.last_refill.elapsed().as_secs_f64();
            rate.tokens =
                (rate.tokens + elapsed * self.shared.rate_refill).min(self.shared.rate_capacity);
            rate.last_refill = std::time::Instant::now();

            if rate.tokens < 1.0 {
                self.shared
                    .rate_limited_total
                    .fetch_add(1, Ordering::Relaxed);
                // Send rate_limited error to client (throttled: max once per second)
                let should_notify = rate
                    .last_rate_limited
                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
                if should_notify {
                    let err = "{\"c\":\"WSE\",\"t\":\"error\",\"p\":{\"code\":\"RATE_LIMITED\",\"message\":\"Rate limit exceeded\",\"retry_after\":1.0},\"v\":1}".to_string();
                    cmd_tx
                        .send(ServerCommand::SendText {
                            conn_id: conn_id.to_owned(),
                            data: err,
                        })
                        .ok();
                    rate.last_rate_limited = Some(std::time::Instant::now());
                }
                return Ok(0);
            }

            // Warning at 20% remaining capacity (throttled: max once per second)
            let warning_threshold = self.shared.rate_capacity * 0.2;
            if rate.tokens < warning_threshold {
                let should_warn = rate
                    .last_warning
                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
                if should_warn {
                    let warning = format!(
                        "{{\"c\":\"WSE\",\"t\":\"rate_limit_warning\",\"p\":{{\"current_rate\":{},\"limit\":{},\"remaining\":{},\"retry_after\":1.0}},\"v\":1}}",
                        (self.shared.rate_capacity - rate.tokens) as u64,
                        self.shared.rate_capacity as u64,
                        rate.tokens as u64,
                    );
                    cmd_tx
                        .send(ServerCommand::SendText {
                            conn_id: conn_id.to_owned(),
                            data: warning,
                        })
                        .ok();
                    rate.last_warning = Some(std::time::Instant::now());
                }
            }

            rate.tokens -= 1.0;
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
            map.insert(key, pyobj_to_json(&v));
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
        let conn_cipher = self.shared.conn_encryption.get(conn_id);

        let byte_count;

        if use_msgpack {
            // Msgpack path: M: prefix + rmpv serialize (binary frame)
            let rmpv_val = serde_json_to_rmpv(&serde_json::Value::Object(map));
            let mut buf = Vec::new();
            rmpv::encode::write_value(&mut buf, &rmpv_val)
                .map_err(|e| PyRuntimeError::new_err(format!("Msgpack error: {e}")))?;
            let mut final_bytes = Vec::with_capacity(buf.len() + 2);
            final_bytes.extend_from_slice(b"M:");
            final_bytes.extend_from_slice(&buf);

            // Encrypt if E2E enabled for this connection
            let final_bytes = if let Some(ref cipher) = conn_cipher {
                let encrypted = encrypt_outbound(cipher, &final_bytes)
                    .map_err(|e| PyRuntimeError::new_err(format!("Encrypt error: {e}")))?;
                let mut enc_buf = Vec::with_capacity(2 + encrypted.len());
                enc_buf.extend_from_slice(b"E:");
                enc_buf.extend_from_slice(&encrypted);
                enc_buf
            } else {
                final_bytes
            };

            byte_count = final_bytes.len();
            cmd_tx
                .send(ServerCommand::SendPrebuilt {
                    conn_id: conn_id.to_owned(),
                    message: Message::Binary(final_bytes.into()),
                })
                .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        } else {
            // JSON path: serialize (no wire prefix, c is inside JSON)
            let json_str = serde_json::to_string(&serde_json::Value::Object(map))
                .map_err(|e| PyRuntimeError::new_err(format!("JSON error: {e}")))?;

            let payload_bytes = json_str.as_bytes();

            if let Some(ref cipher) = conn_cipher {
                // E2E encrypted: encrypt plaintext payload, send as E: prefix binary
                let encrypted = encrypt_outbound(cipher, payload_bytes)
                    .map_err(|e| PyRuntimeError::new_err(format!("Encrypt error: {e}")))?;
                let mut final_bytes = Vec::with_capacity(2 + encrypted.len());
                final_bytes.extend_from_slice(b"E:");
                final_bytes.extend_from_slice(&encrypted);
                byte_count = final_bytes.len();
                cmd_tx
                    .send(ServerCommand::SendPrebuilt {
                        conn_id: conn_id.to_owned(),
                        message: Message::Binary(final_bytes.into()),
                    })
                    .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
            } else if compression_threshold > 0 && payload_bytes.len() > compression_threshold {
                // Compressed: C: prefix + zlib (binary frame)
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
                encoder
                    .write_all(payload_bytes)
                    .map_err(|e| PyRuntimeError::new_err(format!("Compress error: {e}")))?;
                let compressed = encoder
                    .finish()
                    .map_err(|e| PyRuntimeError::new_err(format!("Compress finish: {e}")))?;
                let mut final_bytes = Vec::with_capacity(compressed.len() + 2);
                final_bytes.extend_from_slice(b"C:");
                final_bytes.extend_from_slice(&compressed);
                byte_count = final_bytes.len();
                cmd_tx
                    .send(ServerCommand::SendPrebuilt {
                        conn_id: conn_id.to_owned(),
                        message: Message::Binary(final_bytes.into()),
                    })
                    .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
            } else {
                // Uncompressed JSON: binary frame (skips UTF-8 validation overhead)
                byte_count = json_str.len();
                cmd_tx
                    .send(ServerCommand::SendPrebuilt {
                        conn_id: conn_id.to_owned(),
                        message: Message::Binary(json_str.into_bytes().into()),
                    })
                    .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
            }
        }

        self.shared
            .messages_sent_total
            .fetch_add(1, Ordering::Relaxed);
        self.shared
            .bytes_sent_total
            .fetch_add(byte_count as u64, Ordering::Relaxed);
        Ok(byte_count)
    }

    pub(super) fn broadcast_all_impl(&self, data: &str) -> PyResult<()> {
        let tx = self.command_tx()?;
        let data = inject_category(data);
        tx.send(ServerCommand::BroadcastText { data })
            .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn broadcast_all_bytes_impl(&self, data: &[u8]) -> PyResult<()> {
        let tx = self.command_tx()?;
        tx.send(ServerCommand::BroadcastBytes {
            data: data.to_vec(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn broadcast_local_impl(&self, topic: &str, data: &str) -> PyResult<()> {
        let tx = self.command_tx()?;
        let data = inject_category(data);
        tx.send(ServerCommand::BroadcastLocal {
            topic: topic.to_owned(),
            data,
            skip_recovery: false,
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn broadcast_impl(&self, topic: &str, data: &str) -> PyResult<()> {
        let data = inject_category(data);

        // Local dispatch (always)
        if let Ok(tx) = self.command_tx() {
            let _ = tx.send(ServerCommand::BroadcastLocal {
                topic: topic.to_owned(),
                data: data.clone(),
                skip_recovery: false,
            });
        }

        // Cluster publish (if connected)
        let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();
        if let Some(tx) = cluster_tx {
            tx.send(ClusterCommand::Publish {
                topic: topic.to_owned(),
                payload: data,
            })
            .map_err(|_| PyRuntimeError::new_err("Cluster command channel closed"))?;
        }
        Ok(())
    }
}
