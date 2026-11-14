//! Protocol messages the server builds itself, and the message category helpers.

use uuid::Uuid;

use super::SharedState;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format a presence event as a JSON message with category.
pub(crate) fn format_presence_msg(
    event_type: &str,
    user_id: &str,
    data: &serde_json::Value,
) -> String {
    let payload = serde_json::json!({
        "c": "WSE",
        "t": event_type,
        "p": { "user_id": user_id, "data": data }
    });
    payload.to_string()
}

/// Normalize JSON keys: "type"→"t", "payload"→"p" (done in Rust, no GIL needed).
pub(super) fn normalize_json_keys(mut val: serde_json::Value) -> serde_json::Value {
    if let serde_json::Value::Object(ref mut map) = val {
        if !map.contains_key("t")
            && let Some(v) = map.remove("type")
        {
            map.insert("t".to_string(), v);
        }
        if !map.contains_key("p")
            && let Some(v) = map.remove("payload")
        {
            map.insert("p".to_string(), v);
        }
    }
    val
}

pub(super) fn message_category(msg_type: &str) -> &'static str {
    if msg_type.contains("_snapshot") || msg_type.starts_with("snapshot_") {
        return "S";
    }
    match msg_type {
        "server_ready"
        | "server_hello"
        | "client_hello_ack"
        | "connection_state_change"
        | "subscription_update"
        | "snapshot_complete"
        | "error"
        | "pong"
        | "PONG" => "WSE",
        _ => "U",
    }
}

/// Inject `"c"` field at the start of a JSON object from Python publishers.
/// Depth-aware scan: only matches `"t"` and `"c"` keys at the top-level object (depth 1),
/// ignoring occurrences inside nested objects or string values.
pub(super) fn inject_category(data: &str) -> String {
    let bytes = data.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut t_val_range: Option<(usize, usize)> = None;
    let mut first_brace: Option<usize> = None;
    let mut i = 0;

    while i < bytes.len() {
        if in_string {
            if bytes[i] == b'\\' {
                i += 1; // skip the backslash
                if i < bytes.len() && bytes[i] == b'u' {
                    i += 5; // skip uXXXX (4 hex digits + the 'u')
                } else {
                    i += 1; // skip single escaped char
                }
                continue;
            }
            if bytes[i] == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match bytes[i] {
            b'{' => {
                depth += 1;
                if first_brace.is_none() {
                    first_brace = Some(i);
                }
                i += 1;
            }
            b'}' => {
                depth -= 1;
                i += 1;
            }
            // Top-level "c": key -- category already present
            b'"' if depth == 1 && bytes[i..].starts_with(b"\"c\":") => {
                return data.to_string();
            }
            // Top-level "t":"value" -- extract event type
            b'"' if depth == 1 && t_val_range.is_none() && bytes[i..].starts_with(b"\"t\":\"") => {
                let val_start = i + 5;
                let mut j = val_start;
                loop {
                    if j >= bytes.len() {
                        break;
                    }
                    if bytes[j] == b'\\' {
                        j += 1; // skip backslash
                        if j < bytes.len() && bytes[j] == b'u' {
                            j += 5; // skip uXXXX
                        } else {
                            j += 1; // skip single escaped char
                        }
                        continue;
                    }
                    if bytes[j] == b'"' {
                        t_val_range = Some((val_start, j));
                        i = j + 1;
                        break;
                    }
                    j += 1;
                }
                if t_val_range.is_none() {
                    i += 1;
                }
            }
            // Top-level "t": with non-string value (number, null, bool) -- use "U" (unknown) category.
            // Synthesize a range pointing to "U" so inject_category still adds the "c" field.
            b'"' if depth == 1
                && t_val_range.is_none()
                && bytes[i..].starts_with(b"\"t\":")
                && !bytes[i..].starts_with(b"\"t\":\"") =>
            {
                // Non-string t value -- set a synthetic range that will map to "U" category
                // Use 0..0 as a sentinel: message_category("") returns "U"
                t_val_range = Some((0, 0));
                i += 4; // skip past "t":
            }
            b'"' => {
                in_string = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    if let (Some((vs, ve)), Some(brace)) = (t_val_range, first_brace) {
        let event_type = &data[vs..ve];
        let cat = message_category(event_type);
        let mut result = String::with_capacity(data.len() + cat.len() + 8);
        result.push_str(&data[..=brace]);
        result.push_str("\"c\":\"");
        result.push_str(cat);
        result.push_str("\",");
        result.push_str(&data[brace + 1..]);
        return result;
    }

    data.to_string()
}

/// Build a JSON error message for auth failures.
pub(super) fn build_error_json(code: &str, message: &str) -> String {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let payload = serde_json::json!({
        "c": "WSE",
        "t": "error",
        "p": {
            "code": code,
            "message": message,
            "timestamp": ts,
        },
        "v": 1
    });
    payload.to_string()
}

/// Build server_hello JSON response to client_hello (protocol negotiation).
pub(super) fn build_server_hello(
    conn_id: &str,
    client_proto: u32,
    state: &SharedState,
    encryption_pubkey: Option<&str>,
    e2e: Option<serde_json::Value>,
) -> String {
    let proto = client_proto.min(2);
    let recovery_enabled = state.recovery.is_some();
    let cluster_enabled = state.cluster_cmd_tx.load().is_some();
    let encryption_enabled = encryption_pubkey.is_some() || e2e.is_some();
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut val = serde_json::json!({
        "c": "WSE",
        "t": "server_hello",
        "p": {
            "server_version": env!("CARGO_PKG_VERSION"),
            "protocol_version": proto,
            "features": {
                "compression": true,
                "recovery": recovery_enabled,
                "cluster": cluster_enabled,
                "batching": true,
                "priority_queue": true,
                "msgpack": true,
                "encryption": encryption_enabled,
                "signing": state.signer.is_some()
            },
            "limits": {
                "max_message_size": state.max_message_size,
                "rate_limit_capacity": state.rate_capacity as u64,
                "rate_limit_refill": state.rate_refill as u64
            },
            "ping_interval": state.ping_interval_secs * 1000,
            "connection_id": conn_id,
            "server_time": ts
        },
        "v": 1
    });
    if let Some(pubkey) = encryption_pubkey {
        val["p"]["encryption_public_key"] = serde_json::Value::String(pubkey.to_string());
    }
    if let Some(e2e) = e2e {
        val["p"]["e2e"] = e2e;
    }
    // The server's signing key: its id is the audience of the client's
    // signed messages. Clients check the server's signatures against the
    // keys they pin, not against this.
    if let Some(signer) = &state.signer {
        val["p"]["signing"] = serde_json::json!({
            "kid": signer.kid,
            "public_key": signer.public_key,
        });
    }
    val.to_string()
}

/// Build server_ready JSON message (sent immediately from Rust after JWT validation).
pub(super) fn build_server_ready(conn_id: &str, user_id: &str, state: &SharedState) -> String {
    let recovery_enabled = state.recovery.is_some();
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let msg_id = Uuid::now_v7().to_string();
    let mut val = serde_json::json!({
        "c": "WSE",
        "t": "server_ready",
        "id": msg_id,
        "ts": ts,
        "seq": 1,
        "p": {
            "message": "Connection established (Rust transport)",
            "details": {
                "version": 1,
                "features": {
                    "compression": true,
                    "encryption": false,
                    "batching": true,
                    "priority_queue": true,
                    "circuit_breaker": true,
                    "rust_transport": true,
                    "rust_jwt": true,
                    "recovery": recovery_enabled
                },
                "connection_id": conn_id,
                "server_time": ts,
                "user_id": user_id
            }
        },
        "v": 1
    });
    // The server's signing key, as in server_hello: a client may sign as
    // soon as it is ready.
    if let Some(signer) = &state.signer {
        val["p"]["details"]["signing"] = serde_json::json!({
            "kid": signer.kid,
            "public_key": signer.public_key,
        });
    }
    val.to_string()
}
