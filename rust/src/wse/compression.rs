use flate2::Compression;
use flate2::write::{ZlibDecoder, ZlibEncoder};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyNone, PyString, PyTuple};
use std::io::Write;

// ---------------------------------------------------------------------------
// Standalone functions (backward compatibility)
// ---------------------------------------------------------------------------

/// Compress data using flate2 zlib.
///
/// # Arguments
/// * `data`  - Raw bytes to compress.
/// * `level` - Compression level 1 (fastest) through 9 (smallest).
#[pyfunction]
pub fn rust_compress(py: Python, data: &[u8], level: i32) -> PyResult<Py<PyBytes>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(level as u32));
    encoder.write_all(data).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("compression write error: {e}"))
    })?;
    let compressed = encoder.finish().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("compression finish error: {e}"))
    })?;
    Ok(PyBytes::new(py, &compressed).unbind())
}

/// Decompress zlib-compressed data.
#[pyfunction]
pub fn rust_decompress(py: Python, data: &[u8]) -> PyResult<Py<PyBytes>> {
    let mut decoder = ZlibDecoder::new(Vec::new());
    decoder.write_all(data).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("decompression write error: {e}"))
    })?;
    let decompressed = decoder.finish().map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("decompression finish error: {e}"))
    })?;
    Ok(PyBytes::new(py, &decompressed).unbind())
}

/// Return true if the data length exceeds the given threshold.
#[pyfunction]
pub fn rust_should_compress(data: &[u8], threshold: usize) -> bool {
    data.len() > threshold
}

// ---------------------------------------------------------------------------
// Helpers: Python value -> rmp_serde::Value conversion
// ---------------------------------------------------------------------------

/// Convert a Python object to an rmpv::Value suitable for msgpack serialization.
/// Handles UUID, datetime, date, Enum, dict, list, tuple, bytes, and primitives.
fn py_to_rmpv(obj: &Bound<'_, PyAny>) -> PyResult<rmpv::Value> {
    // None
    if obj.is_instance_of::<PyNone>() {
        return Ok(rmpv::Value::Nil);
    }

    // Bool (must check before int, since bool is subclass of int in Python)
    if obj.is_instance_of::<PyBool>() {
        let v: bool = obj.extract()?;
        return Ok(rmpv::Value::Boolean(v));
    }

    // Int
    if obj.is_instance_of::<PyInt>() {
        // Try i64 first, then fall back to string for very large ints
        if let Ok(v) = obj.extract::<i64>() {
            return Ok(rmpv::Value::Integer(rmpv::Integer::from(v)));
        }
        if let Ok(v) = obj.extract::<u64>() {
            return Ok(rmpv::Value::Integer(rmpv::Integer::from(v)));
        }
        // Fallback: convert to string
        let s = obj.str()?.to_string();
        return Ok(rmpv::Value::String(s.into()));
    }

    // Float
    if obj.is_instance_of::<PyFloat>() {
        let v: f64 = obj.extract()?;
        return Ok(rmpv::Value::F64(v));
    }

    // String
    if obj.is_instance_of::<PyString>() {
        let s: String = obj.extract()?;
        return Ok(rmpv::Value::String(s.into()));
    }

    // Bytes
    if obj.is_instance_of::<PyBytes>() {
        let b: &[u8] = obj.cast::<PyBytes>()?.as_bytes();
        return Ok(rmpv::Value::Binary(b.to_vec()));
    }

    // Dict
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut pairs = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key = py_to_rmpv(&k)?;
            let val = py_to_rmpv(&v)?;
            pairs.push((key, val));
        }
        return Ok(rmpv::Value::Map(pairs));
    }

    // List
    if let Ok(list) = obj.cast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for item in list.iter() {
            items.push(py_to_rmpv(&item)?);
        }
        return Ok(rmpv::Value::Array(items));
    }

    // Tuple -> Array
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        let mut items = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            items.push(py_to_rmpv(&item)?);
        }
        return Ok(rmpv::Value::Array(items));
    }

    // UUID (has `hex` attr that is a 32-char string)
    if obj.hasattr("hex")?
        && let Ok(hex_val) = obj.getattr("hex")
        && let Ok(hex_str) = hex_val.extract::<String>()
        && hex_str.len() == 32
    {
        let s = obj.str()?.to_string();
        return Ok(rmpv::Value::String(s.into()));
    }

    // datetime/date (has isoformat + year)
    if obj.hasattr("isoformat")? && obj.hasattr("year")? {
        let iso = obj.call_method0("isoformat")?;
        let s: String = iso.extract()?;
        return Ok(rmpv::Value::String(s.into()));
    }

    // Enum (has value + name)
    if obj.hasattr("value")? && obj.hasattr("name")? {
        let val = obj.getattr("value")?;
        return py_to_rmpv(&val);
    }

    // Has __dict__ -> serialize as dict
    if obj.hasattr("__dict__")? {
        let dunder_dict = obj.getattr("__dict__")?;
        if let Ok(dict) = dunder_dict.cast::<PyDict>() {
            return py_to_rmpv(dict.as_any());
        }
    }

    // Fallback: str(obj)
    let s = obj.str()?.to_string();
    Ok(rmpv::Value::String(s.into()))
}

/// Convert an rmpv::Value back to a Python object.
fn rmpv_to_py<'py>(py: Python<'py>, val: &rmpv::Value) -> PyResult<Bound<'py, PyAny>> {
    match val {
        rmpv::Value::Nil => Ok(py.None().into_bound(py)),
        rmpv::Value::Boolean(b) => Ok(PyBool::new(py, *b).to_owned().into_any()),
        rmpv::Value::Integer(i) => {
            if let Some(v) = i.as_i64() {
                Ok(v.into_pyobject(py)?.into_any())
            } else if let Some(v) = i.as_u64() {
                Ok(v.into_pyobject(py)?.into_any())
            } else {
                Ok(0i64.into_pyobject(py)?.into_any())
            }
        }
        rmpv::Value::F32(f) => Ok((*f as f64).into_pyobject(py)?.into_any()),
        rmpv::Value::F64(f) => Ok(f.into_pyobject(py)?.into_any()),
        rmpv::Value::String(s) => {
            let st = s.as_str().unwrap_or("");
            Ok(PyString::new(py, st).into_any())
        }
        rmpv::Value::Binary(b) => Ok(PyBytes::new(py, b).into_any()),
        rmpv::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                let py_item = rmpv_to_py(py, item)?;
                list.append(py_item)?;
            }
            Ok(list.into_any())
        }
        rmpv::Value::Map(pairs) => {
            let dict = PyDict::new(py);
            for (k, v) in pairs {
                let py_k = rmpv_to_py(py, k)?;
                let py_v = rmpv_to_py(py, v)?;
                dict.set_item(py_k, py_v)?;
            }
            Ok(dict.into_any())
        }
        rmpv::Value::Ext(_, data) => Ok(PyBytes::new(py, data).into_any()),
    }
}

/// Serialize a Python dict using the custom serializer (for JSON fallback in pack_event).
/// Converts UUID/datetime/Enum to strings, similar to Python's `default=` callback.
fn py_dict_to_json_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let rmpv_val = py_to_rmpv(obj)?;
    let json_val = rmpv_to_serde_json(&rmpv_val);
    serde_json::to_vec(&json_val).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("JSON serialization failed: {e}"))
    })
}

/// Convert rmpv::Value -> serde_json::Value for JSON fallback serialization.
fn rmpv_to_serde_json(val: &rmpv::Value) -> serde_json::Value {
    match val {
        rmpv::Value::Nil => serde_json::Value::Null,
        rmpv::Value::Boolean(b) => serde_json::Value::Bool(*b),
        rmpv::Value::Integer(i) => {
            if let Some(v) = i.as_i64() {
                serde_json::Value::Number(serde_json::Number::from(v))
            } else if let Some(v) = i.as_u64() {
                serde_json::Value::Number(serde_json::Number::from(v))
            } else {
                serde_json::Value::Number(serde_json::Number::from(0))
            }
        }
        rmpv::Value::F32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        rmpv::Value::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        rmpv::Value::String(s) => serde_json::Value::String(s.as_str().unwrap_or("").to_string()),
        rmpv::Value::Binary(b) => {
            // Encode binary as base64 or hex in JSON context
            serde_json::Value::String(hex::encode(b))
        }
        rmpv::Value::Array(arr) => {
            let items: Vec<serde_json::Value> = arr.iter().map(rmpv_to_serde_json).collect();
            serde_json::Value::Array(items)
        }
        rmpv::Value::Map(pairs) => {
            let mut map = serde_json::Map::new();
            for (k, v) in pairs {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    _ => format!("{}", k),
                };
                map.insert(key, rmpv_to_serde_json(v));
            }
            serde_json::Value::Object(map)
        }
        rmpv::Value::Ext(_, data) => serde_json::Value::String(hex::encode(data)),
    }
}

/// Convert serde_json::Value -> Python object.
fn json_to_py<'py>(py: Python<'py>, val: &serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
    match val {
        serde_json::Value::Null => Ok(py.None().into_bound(py)),
        serde_json::Value::Bool(b) => Ok(PyBool::new(py, *b).to_owned().into_any()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any())
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_pyobject(py)?.into_any())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any())
            } else {
                Ok(py.None().into_bound(py))
            }
        }
        serde_json::Value::String(s) => Ok(PyString::new(py, s).into_any()),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.into_any())
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(PyString::new(py, k), json_to_py(py, v)?)?;
            }
            Ok(dict.into_any())
        }
    }
}

// ---------------------------------------------------------------------------
// RustCompressionManager pyclass
// ---------------------------------------------------------------------------

/// Full-featured compression manager with stats tracking, msgpack support,
/// and event packing/unpacking. Mirrors Python CompressionManager API.
#[pyclass]
pub struct RustCompressionManager {
    threshold: usize,
    compression_level: u32,
    // Stats
    total_compressed: u64,
    total_decompressed: u64,
    compression_failures: u64,
    decompression_failures: u64,
    total_bytes_saved: i64,
}

#[pymethods]
impl RustCompressionManager {
    /// Create a new compression manager.
    ///
    /// # Arguments
    /// * `threshold` - Minimum byte size before compression is applied (default: 1024).
    /// * `compression_level` - zlib compression level 1-9 (default: 6).
    #[new]
    #[pyo3(signature = (threshold=1024, compression_level=6))]
    fn new(threshold: usize, compression_level: u32) -> Self {
        Self {
            threshold,
            compression_level,
            total_compressed: 0,
            total_decompressed: 0,
            compression_failures: 0,
            decompression_failures: 0,
            total_bytes_saved: 0,
        }
    }

    /// Check if data should be compressed based on threshold.
    fn should_compress(&self, data: &[u8]) -> bool {
        data.len() > self.threshold
    }

    /// Compress data using zlib with validation and stats tracking.
    ///
    /// # Arguments
    /// * `data`  - Raw bytes to compress.
    /// * `level` - Optional override for compression level.
    #[pyo3(signature = (data, level=None))]
    fn compress(&mut self, py: Python, data: &[u8], level: Option<u32>) -> PyResult<Py<PyBytes>> {
        let lvl = level.unwrap_or(self.compression_level);
        let original_size = data.len();

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(lvl));
        if let Err(e) = encoder.write_all(data) {
            self.compression_failures += 1;
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "compression write error: {e}"
            )));
        }
        let compressed = match encoder.finish() {
            Ok(c) => c,
            Err(e) => {
                self.compression_failures += 1;
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "compression finish error: {e}"
                )));
            }
        };

        let compressed_size = compressed.len();
        let saved_bytes = original_size as i64 - compressed_size as i64;
        if saved_bytes > 0 {
            self.total_bytes_saved += saved_bytes;
        }
        self.total_compressed += 1;

        Ok(PyBytes::new(py, &compressed).unbind())
    }

    /// Decompress zlib-compressed data with stats tracking.
    fn decompress(&mut self, py: Python, data: &[u8]) -> PyResult<Py<PyBytes>> {
        let mut decoder = ZlibDecoder::new(Vec::new());
        if let Err(e) = decoder.write_all(data) {
            self.decompression_failures += 1;
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "decompression write error: {e}"
            )));
        }
        let decompressed = match decoder.finish() {
            Ok(d) => d,
            Err(e) => {
                self.decompression_failures += 1;
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "decompression finish error: {e}"
                )));
            }
        };

        self.total_decompressed += 1;
        Ok(PyBytes::new(py, &decompressed).unbind())
    }

    /// Pack a Python dict using msgpack with custom serialization for UUID/datetime/Enum.
    fn pack_msgpack<'py>(
        &self,
        py: Python<'py>,
        data: &Bound<'py, PyDict>,
    ) -> PyResult<Py<PyBytes>> {
        let rmpv_val = py_to_rmpv(data.as_any())?;
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &rmpv_val).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("msgpack packing failed: {e}"))
        })?;
        Ok(PyBytes::new(py, &buf).unbind())
    }

    /// Unpack msgpack bytes to a Python dict.
    fn unpack_msgpack<'py>(&self, py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        let val = rmpv::decode::read_value(&mut &data[..]).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("msgpack unpacking failed: {e}"))
        })?;
        rmpv_to_py(py, &val)
    }

    /// Pack event to bytes. If use_msgpack is true, try msgpack first with JSON fallback.
    /// Otherwise, use JSON directly. Handles UUID/datetime/Enum serialization.
    #[pyo3(signature = (event, use_msgpack=true))]
    fn pack_event<'py>(
        &self,
        py: Python<'py>,
        event: &Bound<'py, PyDict>,
        use_msgpack: bool,
    ) -> PyResult<Py<PyBytes>> {
        if use_msgpack {
            // Try msgpack first
            let rmpv_val = py_to_rmpv(event.as_any())?;
            let mut buf = Vec::new();
            match rmpv::encode::write_value(&mut buf, &rmpv_val) {
                Ok(()) => return Ok(PyBytes::new(py, &buf).unbind()),
                Err(_) => {
                    // Fall back to JSON
                    let json_bytes = py_dict_to_json_bytes(event.as_any())?;
                    return Ok(PyBytes::new(py, &json_bytes).unbind());
                }
            }
        }

        // JSON path
        let json_bytes = py_dict_to_json_bytes(event.as_any())?;
        Ok(PyBytes::new(py, &json_bytes).unbind())
    }

    /// Unpack event from bytes. If is_msgpack is true, try msgpack first then JSON fallback.
    /// Otherwise, try JSON with multiple encoding attempts.
    #[pyo3(signature = (data, is_msgpack=true))]
    fn unpack_event<'py>(
        &self,
        py: Python<'py>,
        data: &[u8],
        is_msgpack: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        if is_msgpack {
            // Try msgpack first
            if let Ok(val) = rmpv::decode::read_value(&mut &data[..])
                && let Ok(py_obj) = rmpv_to_py(py, &val)
            {
                return Ok(py_obj);
            }

            // Fall back to JSON with multiple encodings
            for encoding in &["utf-8", "latin-1", "cp1252"] {
                if let Some(result) = try_json_decode(py, data, encoding)? {
                    return Ok(result);
                }
            }

            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Failed to unpack event (tried msgpack + JSON)",
            ));
        }

        // JSON-only path with multiple encodings
        for encoding in &["utf-8", "latin-1", "cp1252"] {
            if let Some(result) = try_json_decode(py, data, encoding)? {
                return Ok(result);
            }
        }

        // Last resort: return raw hex as dict with error flag (matches Python behavior)
        let hex_str = hex::encode(data);
        let dict = PyDict::new(py);
        dict.set_item("_raw_hex", PyString::new(py, &hex_str))?;
        dict.set_item("_decode_error", true)?;
        Ok(dict.into_any())
    }

    /// Get compression statistics as a dict.
    fn get_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("total_compressed", self.total_compressed)?;
        dict.set_item("total_decompressed", self.total_decompressed)?;
        dict.set_item("compression_failures", self.compression_failures)?;
        dict.set_item("decompression_failures", self.decompression_failures)?;
        dict.set_item("total_bytes_saved", self.total_bytes_saved)?;

        // Derived stats
        let total_comp_attempts = self.total_compressed + self.compression_failures;
        let compression_success_rate = if total_comp_attempts > 0 {
            self.total_compressed as f64 / total_comp_attempts as f64
        } else {
            0.0
        };
        dict.set_item("compression_success_rate", compression_success_rate)?;

        let total_decomp_attempts = self.total_decompressed + self.decompression_failures;
        let decompression_success_rate = if total_decomp_attempts > 0 {
            self.total_decompressed as f64 / total_decomp_attempts as f64
        } else {
            0.0
        };
        dict.set_item("decompression_success_rate", decompression_success_rate)?;

        let average_bytes_saved = if self.total_compressed > 0 {
            self.total_bytes_saved as f64 / self.total_compressed as f64
        } else {
            0.0
        };
        dict.set_item("average_bytes_saved", average_bytes_saved)?;

        Ok(dict)
    }

    /// Reset all compression statistics to zero.
    fn reset_stats(&mut self) {
        self.total_compressed = 0;
        self.total_decompressed = 0;
        self.compression_failures = 0;
        self.decompression_failures = 0;
        self.total_bytes_saved = 0;
    }
}

// ---------------------------------------------------------------------------
// Public: serde_json::Value -> rmpv::Value conversion (used by server.rs)
// ---------------------------------------------------------------------------

/// Convert serde_json::Value to rmpv::Value for msgpack serialization.
/// Used by send_event() when a connection requests msgpack format.
pub fn serde_json_to_rmpv(val: &serde_json::Value) -> rmpv::Value {
    match val {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rmpv::Value::Integer(rmpv::Integer::from(i))
            } else if let Some(u) = n.as_u64() {
                rmpv::Value::Integer(rmpv::Integer::from(u))
            } else if let Some(f) = n.as_f64() {
                rmpv::Value::F64(f)
            } else {
                rmpv::Value::Nil
            }
        }
        serde_json::Value::String(s) => rmpv::Value::String(s.clone().into()),
        serde_json::Value::Array(arr) => {
            rmpv::Value::Array(arr.iter().map(serde_json_to_rmpv).collect())
        }
        serde_json::Value::Object(map) => {
            let pairs: Vec<(rmpv::Value, rmpv::Value)> = map
                .iter()
                .map(|(k, v)| {
                    (
                        rmpv::Value::String(k.clone().into()),
                        serde_json_to_rmpv(v),
                    )
                })
                .collect();
            rmpv::Value::Map(pairs)
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Try to decode bytes as JSON using a specific encoding.
/// Returns Ok(Some(..)) on success, Ok(None) if decoding/parsing fails,
/// Err only on PyO3 errors.
fn try_json_decode<'py>(
    py: Python<'py>,
    data: &[u8],
    encoding: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let decoded = match encoding {
        "utf-8" => match std::str::from_utf8(data) {
            Ok(s) => s.to_string(),
            Err(_) => return Ok(None),
        },
        "latin-1" => {
            // Latin-1 is a 1:1 byte-to-char mapping (0x00-0xFF)
            data.iter().map(|&b| b as char).collect::<String>()
        }
        "cp1252" => {
            // Windows-1252: same as latin-1 for most bytes, approximate
            data.iter().map(|&b| b as char).collect::<String>()
        }
        _ => return Ok(None),
    };

    match serde_json::from_str::<serde_json::Value>(&decoded) {
        Ok(json_val) => {
            let py_obj = json_to_py(py, &json_val)?;
            Ok(Some(py_obj))
        }
        Err(_) => Ok(None),
    }
}
