use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyNone, PyString, PyTuple};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[inline]
fn is_clean_primitive(obj: &Bound<'_, PyAny>) -> bool {
    obj.is_instance_of::<PyString>()
        || obj.is_instance_of::<PyBool>()
        || obj.is_instance_of::<PyInt>()
        || obj.is_instance_of::<PyFloat>()
        || obj.is_instance_of::<PyNone>()
}

#[inline]
fn is_clean_shallow(obj: &Bound<'_, PyAny>) -> bool {
    is_clean_primitive(obj) || obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyDict>()
}

fn dict_needs_conversion(dict: &Bound<'_, PyDict>) -> bool {
    for (_, v) in dict.iter() {
        if !is_clean_shallow(&v) {
            return true;
        }
        if let Ok(inner_list) = v.cast::<PyList>() {
            for item in inner_list.iter() {
                if !is_clean_shallow(&item) {
                    return true;
                }
            }
        }
        if let Ok(inner_dict) = v.cast::<PyDict>() {
            for (_, inner_v) in inner_dict.iter() {
                if !is_clean_shallow(&inner_v) {
                    return true;
                }
            }
        }
    }
    false
}

fn convert_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    if is_clean_primitive(obj) {
        return Ok(obj.clone().unbind());
    }

    if let Ok(dict) = obj.cast::<PyDict>() {
        if !dict_needs_conversion(dict) {
            return Ok(dict.clone().into_any().unbind());
        }
        let new_dict = PyDict::new(py);
        for (k, v) in dict.iter() {
            let converted = convert_value(py, &v)?;
            new_dict.set_item(k, converted)?;
        }
        return Ok(new_dict.into_any().unbind());
    }

    if let Ok(pybytes) = obj.cast::<PyBytes>() {
        let raw: &[u8] = pybytes.as_bytes();
        if let Ok(s) = std::str::from_utf8(raw) {
            return Ok(PyString::new(py, s).into_any().unbind());
        }
        let s: String = raw.iter().map(|&b| b as char).collect();
        return Ok(PyString::new(py, &s).into_any().unbind());
    }

    // UUID (has `hex` attr that is a 32-char string)
    if obj.hasattr("hex")?
        && let Ok(hex_val) = obj.getattr("hex")
        && let Ok(hex_str) = hex_val.extract::<String>()
        && hex_str.len() == 32
    {
        let s = obj.str()?;
        return Ok(s.into_any().unbind());
    }

    // datetime/date (has isoformat + year)
    if obj.hasattr("isoformat")? && obj.hasattr("year")? {
        let iso = obj.call_method0("isoformat")?;
        return Ok(iso.unbind());
    }

    // Enum (has value + name)
    if obj.hasattr("value")? && obj.hasattr("name")? {
        let val = obj.getattr("value")?;
        return convert_value(py, &val);
    }

    if let Ok(list) = obj.cast::<PyList>() {
        let new_list = PyList::empty(py);
        for item in list.iter() {
            let converted = convert_value(py, &item)?;
            new_list.append(converted)?;
        }
        return Ok(new_list.into_any().unbind());
    }

    if let Ok(tuple) = obj.cast::<PyTuple>() {
        let new_list = PyList::empty(py);
        for item in tuple.iter() {
            let converted = convert_value(py, &item)?;
            new_list.append(converted)?;
        }
        return Ok(new_list.into_any().unbind());
    }

    if obj.hasattr("__dict__")? {
        let dunder_dict = obj.getattr("__dict__")?;
        if let Ok(dict) = dunder_dict.cast::<PyDict>() {
            return convert_value(py, dict.as_any());
        }
    }

    Ok(obj.clone().unbind())
}

fn ensure_str(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    if let Ok(pybytes) = obj.cast::<PyBytes>() {
        let raw = pybytes.as_bytes();
        if let Ok(s) = std::str::from_utf8(raw) {
            return Ok(PyString::new(py, s).into_any().unbind());
        }
        let s: String = raw.iter().map(|&b| b as char).collect();
        return Ok(PyString::new(py, &s).into_any().unbind());
    }

    if obj.hasattr("hex")?
        && let Ok(hex_val) = obj.getattr("hex")
        && let Ok(hex_str) = hex_val.extract::<String>()
        && hex_str.len() == 32
    {
        let s = obj.str()?;
        return Ok(s.into_any().unbind());
    }

    if obj.hasattr("isoformat")? && obj.hasattr("year")? {
        let iso = obj.call_method0("isoformat")?;
        return Ok(iso.unbind());
    }

    if obj.hasattr("value")? && obj.hasattr("name")? {
        let val = obj.getattr("value")?;
        return Ok(val.unbind());
    }

    Ok(obj.clone().unbind())
}

#[inline]
fn dict_get_str(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<String>> {
    match dict.get_item(key)? {
        Some(val) if !val.is_none() => Ok(Some(val.str()?.to_string())),
        _ => Ok(None),
    }
}

#[inline]
fn dict_get<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    match dict.get_item(key)? {
        Some(val) if !val.is_none() => Ok(Some(val)),
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Latency calculation
// ---------------------------------------------------------------------------

/// Parse an ISO 8601 timestamp string and compute milliseconds from then to now.
/// Returns None if parsing fails or the timestamp is not available.
fn compute_latency_ms(timestamp_str: &str) -> Option<i64> {
    // Handle 'Z' suffix -> '+00:00'
    let normalized = if let Some(stripped) = timestamp_str.strip_suffix('Z') {
        format!("{}+00:00", stripped)
    } else {
        timestamp_str.to_string()
    };

    // Try parsing with chrono
    if let Ok(dt) = DateTime::parse_from_rfc3339(&normalized) {
        let now = Utc::now();
        let delta = now.signed_duration_since(dt.with_timezone(&Utc));
        return Some(delta.num_milliseconds());
    }

    // Fallback: try parsing without timezone (assume UTC)
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.f") {
        let utc_dt = dt.and_utc();
        let now = Utc::now();
        let delta = now.signed_duration_since(utc_dt);
        return Some(delta.num_milliseconds());
    }

    None
}

// ---------------------------------------------------------------------------
// Generic payload extraction
// ---------------------------------------------------------------------------

/// Generic payload extraction: convert all values to JSON-safe types.
/// If event has a `payload` key, use it; otherwise use the event itself.
/// Strips internal keys like `_metadata`.
fn extract_payload<'py>(
    py: Python<'py>,
    event: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyDict>> {
    // Check if there is a 'payload' key
    let source = match dict_get(event, "payload")? {
        Some(val) => {
            if let Ok(d) = val.cast::<PyDict>() {
                d.clone()
            } else {
                event.clone()
            }
        }
        None => event.clone(),
    };

    // Remove _metadata if present
    let result = PyDict::new(py);
    for (k, v) in source.iter() {
        let key_str = k.str()?.to_string();
        if key_str == "_metadata" {
            continue;
        }
        let converted = convert_value(py, &v)?;
        result.set_item(k, converted)?;
    }

    // Ensure event_type is set if type is provided
    let has_event_type = result.contains("event_type")?;
    if !has_event_type && let Some(type_val) = dict_get(&result, "type")? {
        result.set_item("event_type", type_val)?;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Main entry point: transform raw event dict -> wire envelope
// ---------------------------------------------------------------------------

/// Transform a raw event dictionary into a WSE wire envelope.
///
/// Input: arbitrary Python dict with event data, optional `_metadata`, `event_type`/`type`.
/// Output: `{t, id, ts, seq, p, v}` wire-format dict ready for JSON serialization.
///
/// This is the generic version: payload is extracted and converted without
/// domain-specific transforms. All Python types (UUID, datetime, Enum, bytes)
/// are converted to JSON-safe primitives.
#[pyfunction]
pub fn rust_transform_event<'py>(
    py: Python<'py>,
    event: &Bound<'py, PyDict>,
    sequence: i64,
    event_type_map: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyDict>> {
    // 0. Already transformed?
    let has_v = event.contains("v")?;
    let has_t = event.contains("t")?;
    let has_p = event.contains("p")?;
    let has_id = event.contains("id")?;
    let has_ts = event.contains("ts")?;

    if has_v && has_t && has_p && has_id && has_ts {
        if !event.contains("seq")? {
            event.set_item("seq", sequence)?;
        }
        return Ok(event.clone());
    }

    // 1. Resolve event_type
    let raw_event_type: String = {
        let val = dict_get(event, "event_type")?.or(dict_get(event, "type")?);
        match val {
            Some(v) => {
                let converted: Py<PyAny> = ensure_str(py, &v)?;
                converted.bind(py).str()?.to_string()
            }
            None => "unknown".to_string(),
        }
    };

    let ws_event_type: String = match event_type_map.get_item(&raw_event_type)? {
        Some(mapped) => mapped.str()?.to_string(),
        None => raw_event_type.to_lowercase(),
    };

    // 2. Extract metadata
    let metadata: Option<Bound<'py, PyDict>> = match dict_get(event, "_metadata")? {
        Some(val) => val.cast::<PyDict>().ok().cloned(),
        None => None,
    };

    // 3. Compute latency
    let event_timestamp_str: Option<String> = {
        let from_meta = metadata
            .as_ref()
            .and_then(|m| dict_get_str(m, "timestamp").ok().flatten());
        let from_event = dict_get_str(event, "timestamp").ok().flatten();
        from_meta.or(from_event)
    };

    let event_latency_ms: Option<i64> = event_timestamp_str.as_deref().and_then(compute_latency_ms);

    // 4. Resolve event ID
    let event_id: String = {
        let from_meta = metadata
            .as_ref()
            .and_then(|m| dict_get(m, "event_id").ok().flatten());
        let from_event = dict_get(event, "event_id")?.filter(|v| !v.is_none());
        match from_meta.or(from_event) {
            Some(val) => {
                let converted: Py<PyAny> = ensure_str(py, &val)?;
                converted.bind(py).str()?.to_string()
            }
            None => Uuid::now_v7().to_string(),
        }
    };

    // 5. Resolve timestamp
    let timestamp: String = {
        let from_meta = metadata
            .as_ref()
            .and_then(|m| dict_get(m, "timestamp").ok().flatten());
        let from_event = dict_get(event, "timestamp")?.filter(|v| !v.is_none());
        match from_meta.or(from_event) {
            Some(val) => {
                let converted: Py<PyAny> = ensure_str(py, &val)?;
                converted.bind(py).str()?.to_string()
            }
            None => Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        }
    };

    // 6. Generic payload extraction (convert all values to JSON-safe types)
    let payload = extract_payload(py, event)?;
    let payload_converted = convert_value(py, payload.as_any())?;

    // 7. Build wire envelope: c first, t second, v last
    let envelope = PyDict::new(py);

    // c first (category)
    if let Some(msg_cat) = dict_get(event, "c")? {
        envelope.set_item("c", msg_cat)?;
    } else if ws_event_type.contains("_snapshot") || ws_event_type.contains("snapshot_") {
        envelope.set_item("c", "S")?;
    } else {
        const WSE_TYPES: &[&str] = &[
            "server_ready",
            "snapshot_complete",
            "client_hello_ack",
            "connection_state_change",
            "error",
            "pong",
        ];
        if WSE_TYPES.contains(&ws_event_type.as_str()) {
            envelope.set_item("c", "WSE")?;
        } else {
            envelope.set_item("c", "U")?;
        }
    }

    // t second
    envelope.set_item("t", PyString::new(py, &ws_event_type))?;
    envelope.set_item("id", PyString::new(py, &event_id))?;
    envelope.set_item("ts", PyString::new(py, &timestamp))?;
    envelope.set_item("seq", sequence)?;
    envelope.set_item("p", payload_converted)?;
    envelope.set_item("v", 1)?;

    // Preserve original event type
    if raw_event_type != ws_event_type {
        envelope.set_item("original_event_type", PyString::new(py, &raw_event_type))?;
    }

    // 10. Latency
    if let Some(ms) = event_latency_ms {
        envelope.set_item("latency_ms", ms)?;
    }

    // 11. Optional fields
    let cid_from_meta = metadata
        .as_ref()
        .and_then(|m| dict_get_str(m, "correlation_id").ok().flatten());
    let cid_from_event = dict_get_str(event, "correlation_id")?.filter(|s| !s.is_empty());
    if let Some(cid) = cid_from_meta.or(cid_from_event) {
        envelope.set_item("cid", PyString::new(py, &cid))?;
    }

    let pri_from_meta = metadata
        .as_ref()
        .and_then(|m| dict_get(m, "priority").ok().flatten());
    let pri_from_event = dict_get(event, "pri")?.filter(|v| !v.is_none());
    if let Some(pri) = pri_from_meta.or(pri_from_event) {
        envelope.set_item("pri", pri)?;
    }

    {
        let ver_from_event = dict_get(event, "version")?;
        let ver_from_meta = metadata
            .as_ref()
            .and_then(|m| dict_get(m, "version").ok().flatten());
        match ver_from_event.or(ver_from_meta) {
            Some(v) => {
                if let Ok(n) = v.extract::<i64>() {
                    envelope.set_item("event_version", n)?;
                } else {
                    envelope.set_item("event_version", 1)?;
                }
            }
            None => {
                envelope.set_item("event_version", 1)?;
            }
        }
    }

    {
        let trace_from_meta = metadata
            .as_ref()
            .and_then(|m| dict_get_str(m, "trace_id").ok().flatten());
        let trace_from_event = dict_get_str(event, "trace_id")?.filter(|s| !s.is_empty());
        if let Some(trace_id) = trace_from_meta.or(trace_from_event) {
            envelope.set_item("trace_id", PyString::new(py, &trace_id))?;
        }
    }

    Ok(envelope)
}
