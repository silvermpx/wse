use crate::wse::msgpack::{decode_msgpack_guarded, float_value, rmpv_to_serde_json};
use flate2::Compression;
use flate2::write::ZlibEncoder;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyNone, PyString, PyTuple};
use std::io::Write;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Maximum decompressed output size (10 MB). Prevents decompression bombs
/// where a small compressed payload expands to gigabytes of RAM.
const MAX_DECOMPRESS_SIZE: usize = 10 * 1024 * 1024;

/// Inputs at least this large are compressed with the interpreter released,
/// so other Python threads keep running; for small ones the switch costs more.
const DETACH_COMPRESS_AT: usize = 64 * 1024;
/// Compressed inputs at least this large are inflated with the interpreter
/// released (the output can be up to MAX_DECOMPRESS_SIZE).
const DETACH_DECOMPRESS_AT: usize = 16 * 1024;

/// Run a pure-Rust codec step, detached from the interpreter when `detach`.
fn run_codec<T, F>(py: Python<'_>, detach: bool, f: F) -> T
where
    F: FnOnce() -> T + pyo3::marker::Ungil,
    T: pyo3::marker::Ungil,
{
    if detach { py.detach(f) } else { f() }
}

/// zlib levels accepted from Python. flate2's zlib-rs backend panics on
/// anything outside 0..=9, which would surface as an uncatchable-by-default
/// PanicException; 0 (store) is excluded to match `rust_compress`.
fn check_level(level: u32) -> PyResult<u32> {
    if (1..=9).contains(&level) {
        Ok(level)
    } else {
        Err(pyo3::exceptions::PyValueError::new_err(format!(
            "compression level must be between 1 and 9, got {level}"
        )))
    }
}

fn zlib_compress(data: &[u8], level: u32) -> Result<Vec<u8>, String> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(level));
    encoder
        .write_all(data)
        .map_err(|e| format!("compression write error: {e}"))?;
    encoder
        .finish()
        .map_err(|e| format!("compression finish error: {e}"))
}

// ---------------------------------------------------------------------------
// Standalone functions (backward compatibility)
// ---------------------------------------------------------------------------

/// Compress data using flate2 zlib.
///
/// # Arguments
/// * `data`  - Raw bytes to compress.
/// * `level` - Compression level 1 (fastest) through 9 (smallest).
#[pyfunction]
#[pyo3(signature = (data, level=6))]
pub fn rust_compress(py: Python, data: &[u8], level: i32) -> PyResult<Py<PyBytes>> {
    if !(1..=9).contains(&level) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "compression level must be between 1 and 9",
        ));
    }
    let compressed = run_codec(py, data.len() >= DETACH_COMPRESS_AT, || {
        zlib_compress(data, level as u32)
    })
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    Ok(PyBytes::new(py, &compressed).unbind())
}

/// Decompress zlib-compressed data (bounded to MAX_DECOMPRESS_SIZE).
#[pyfunction]
pub fn rust_decompress(py: Python, data: &[u8]) -> PyResult<Py<PyBytes>> {
    run_codec(py, data.len() >= DETACH_DECOMPRESS_AT, || {
        bounded_decompress(data)
    })
    .map(|decompressed| PyBytes::new(py, &decompressed).unbind())
    .map_err(pyo3::exceptions::PyRuntimeError::new_err)
}

/// Decompress with bounded output to prevent decompression bombs.
fn bounded_decompress(data: &[u8]) -> Result<Vec<u8>, String> {
    use flate2::read::ZlibDecoder as ZlibReader;
    use std::io::Read;
    let mut decoder = ZlibReader::new(data);
    let mut output = Vec::with_capacity(data.len().min(65_536));
    let mut buf = [0u8; 8192];
    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| format!("decompression error: {e}"))?;
        if n == 0 {
            break;
        }
        output.extend_from_slice(&buf[..n]);
        if output.len() > MAX_DECOMPRESS_SIZE {
            return Err(format!(
                "decompressed output exceeds {MAX_DECOMPRESS_SIZE} bytes limit"
            ));
        }
    }
    Ok(output)
}

/// Return true if the data length exceeds the given threshold.
#[pyfunction]
#[pyo3(signature = (data, threshold=1024))]
pub fn rust_should_compress(data: &[u8], threshold: usize) -> bool {
    data.len() > threshold
}

// ---------------------------------------------------------------------------
// Helpers: Python value -> rmp_serde::Value conversion
// ---------------------------------------------------------------------------

/// Containers nested deeper than this are rejected with `ValueError`; deeper
/// recursion could overflow the thread's stack, which aborts the process.
const MAX_ENCODE_DEPTH: usize = 128;

/// Record that the encoder enters the container `obj`. `path` holds the
/// containers being encoded around it: its length is the depth, and meeting
/// `obj` on it again means a reference cycle (`d["self"] = d`, or objects
/// reached through `__dict__` that point back up).
fn enter_container(obj: &Bound<'_, PyAny>, path: &mut Vec<usize>) -> PyResult<()> {
    let id = obj.as_ptr() as usize;
    if path.contains(&id) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "cannot encode a value that contains a reference cycle",
        ));
    }
    if path.len() >= MAX_ENCODE_DEPTH {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "cannot encode a value nested deeper than {MAX_ENCODE_DEPTH} levels"
        )));
    }
    path.push(id);
    Ok(())
}

/// Convert a Python object to an rmpv::Value suitable for msgpack serialization.
/// Handles UUID, datetime, date, Enum, dict, list, tuple, bytes, and primitives.
/// Raises `ValueError` for reference cycles and nesting deeper than
/// `MAX_ENCODE_DEPTH`.
fn py_to_rmpv(obj: &Bound<'_, PyAny>) -> PyResult<rmpv::Value> {
    py_to_rmpv_at(obj, &mut Vec::new())
}

fn py_to_rmpv_at(obj: &Bound<'_, PyAny>, path: &mut Vec<usize>) -> PyResult<rmpv::Value> {
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
        return Ok(float_value(v));
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
        enter_container(obj, path)?;
        let mut pairs = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key = py_to_rmpv_at(&k, path)?;
            let val = py_to_rmpv_at(&v, path)?;
            pairs.push((key, val));
        }
        path.pop();
        return Ok(rmpv::Value::Map(pairs));
    }

    // List
    if let Ok(list) = obj.cast::<PyList>() {
        enter_container(obj, path)?;
        let mut items = Vec::with_capacity(list.len());
        for item in list.iter() {
            items.push(py_to_rmpv_at(&item, path)?);
        }
        path.pop();
        return Ok(rmpv::Value::Array(items));
    }

    // Tuple -> Array
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        enter_container(obj, path)?;
        let mut items = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            items.push(py_to_rmpv_at(&item, path)?);
        }
        path.pop();
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
        enter_container(obj, path)?;
        let val = py_to_rmpv_at(&obj.getattr("value")?, path)?;
        path.pop();
        return Ok(val);
    }

    // Has __dict__ -> serialize as dict
    if obj.hasattr("__dict__")? {
        let dunder_dict = obj.getattr("__dict__")?;
        if let Ok(dict) = dunder_dict.cast::<PyDict>() {
            enter_container(obj, path)?;
            let val = py_to_rmpv_at(dict.as_any(), path)?;
            path.pop();
            return Ok(val);
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
///
/// Safe to share between threads, also on free-threaded Python: the settings
/// never change and the statistics are atomic counters, so threads compress
/// in parallel without waiting for each other.
#[pyclass(frozen)]
pub struct RustCompressionManager {
    threshold: usize,
    compression_level: u32,
    // Stats
    total_compressed: AtomicU64,
    total_decompressed: AtomicU64,
    compression_failures: AtomicU64,
    decompression_failures: AtomicU64,
    total_bytes_saved: AtomicI64,
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
    fn new(threshold: usize, compression_level: u32) -> PyResult<Self> {
        Ok(Self {
            threshold,
            compression_level: check_level(compression_level)?,
            total_compressed: AtomicU64::new(0),
            total_decompressed: AtomicU64::new(0),
            compression_failures: AtomicU64::new(0),
            decompression_failures: AtomicU64::new(0),
            total_bytes_saved: AtomicI64::new(0),
        })
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
    fn compress(&self, py: Python, data: &[u8], level: Option<u32>) -> PyResult<Py<PyBytes>> {
        let lvl = match level {
            Some(l) => check_level(l)?,
            None => self.compression_level,
        };
        let original_size = data.len();

        let compressed = match run_codec(py, original_size >= DETACH_COMPRESS_AT, || {
            zlib_compress(data, lvl)
        }) {
            Ok(c) => c,
            Err(e) => {
                self.compression_failures.fetch_add(1, Ordering::Relaxed);
                return Err(pyo3::exceptions::PyRuntimeError::new_err(e));
            }
        };

        let compressed_size = compressed.len();
        let saved_bytes = original_size as i64 - compressed_size as i64;
        if saved_bytes > 0 {
            self.total_bytes_saved
                .fetch_add(saved_bytes, Ordering::Relaxed);
        }
        self.total_compressed.fetch_add(1, Ordering::Relaxed);

        Ok(PyBytes::new(py, &compressed).unbind())
    }

    /// Decompress zlib-compressed data with stats tracking (bounded).
    fn decompress(&self, py: Python, data: &[u8]) -> PyResult<Py<PyBytes>> {
        match run_codec(py, data.len() >= DETACH_DECOMPRESS_AT, || {
            bounded_decompress(data)
        }) {
            Ok(decompressed) => {
                self.total_decompressed.fetch_add(1, Ordering::Relaxed);
                Ok(PyBytes::new(py, &decompressed).unbind())
            }
            Err(e) => {
                self.decompression_failures.fetch_add(1, Ordering::Relaxed);
                Err(pyo3::exceptions::PyRuntimeError::new_err(e))
            }
        }
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
        let val = decode_msgpack_guarded(data).map_err(|()| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "msgpack unpacking failed (invalid or exceeds max nesting depth)",
            )
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
            // Try msgpack first (depth-guarded against stack-overflow frames).
            // An event is a map; any other msgpack value means the bytes were
            // something else (JSON's '[' is the msgpack integer 91), so fall
            // back to JSON.
            if let Ok(val) = decode_msgpack_guarded(data)
                && matches!(val, rmpv::Value::Map(_))
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
        // Each counter is read once, so the derived rates below agree with the
        // raw numbers in this snapshot even while other threads keep counting.
        let total_compressed = self.total_compressed.load(Ordering::Relaxed);
        let total_decompressed = self.total_decompressed.load(Ordering::Relaxed);
        let compression_failures = self.compression_failures.load(Ordering::Relaxed);
        let decompression_failures = self.decompression_failures.load(Ordering::Relaxed);
        let total_bytes_saved = self.total_bytes_saved.load(Ordering::Relaxed);

        let dict = PyDict::new(py);
        dict.set_item("total_compressed", total_compressed)?;
        dict.set_item("total_decompressed", total_decompressed)?;
        dict.set_item("compression_failures", compression_failures)?;
        dict.set_item("decompression_failures", decompression_failures)?;
        dict.set_item("total_bytes_saved", total_bytes_saved)?;

        // Derived stats
        let total_comp_attempts = total_compressed + compression_failures;
        let compression_success_rate = if total_comp_attempts > 0 {
            total_compressed as f64 / total_comp_attempts as f64
        } else {
            0.0
        };
        dict.set_item("compression_success_rate", compression_success_rate)?;

        let total_decomp_attempts = total_decompressed + decompression_failures;
        let decompression_success_rate = if total_decomp_attempts > 0 {
            total_decompressed as f64 / total_decomp_attempts as f64
        } else {
            0.0
        };
        dict.set_item("decompression_success_rate", decompression_success_rate)?;

        let average_bytes_saved = if total_compressed > 0 {
            total_bytes_saved as f64 / total_compressed as f64
        } else {
            0.0
        };
        dict.set_item("average_bytes_saved", average_bytes_saved)?;

        Ok(dict)
    }

    /// Reset all compression statistics to zero.
    fn reset_stats(&self) {
        self.total_compressed.store(0, Ordering::Relaxed);
        self.total_decompressed.store(0, Ordering::Relaxed);
        self.compression_failures.store(0, Ordering::Relaxed);
        self.decompression_failures.store(0, Ordering::Relaxed);
        self.total_bytes_saved.store(0, Ordering::Relaxed);
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
            // Windows-1252: correct mapping for 0x80-0x9F range (smart quotes, etc.)
            static CP1252_MAP: [char; 32] = [
                '\u{20AC}', '\u{0081}', '\u{201A}', '\u{0192}', '\u{201E}', '\u{2026}', '\u{2020}',
                '\u{2021}', '\u{02C6}', '\u{2030}', '\u{0160}', '\u{2039}', '\u{0152}', '\u{008D}',
                '\u{017D}', '\u{008F}', '\u{0090}', '\u{2018}', '\u{2019}', '\u{201C}', '\u{201D}',
                '\u{2022}', '\u{2013}', '\u{2014}', '\u{02DC}', '\u{2122}', '\u{0161}', '\u{203A}',
                '\u{0153}', '\u{009D}', '\u{017E}', '\u{0178}',
            ];
            data.iter()
                .map(|&b| {
                    if (0x80..=0x9F).contains(&b) {
                        CP1252_MAP[(b - 0x80) as usize]
                    } else {
                        b as char
                    }
                })
                .collect::<String>()
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        #[test]
        fn arbitrary_bytes_never_panic(data in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let _ = bounded_decompress(&data);
        }

        #[test]
        fn zlib_round_trips(data in proptest::collection::vec(any::<u8>(), 0..8192), level in 1u32..=9) {
            let packed = zlib_compress(&data, level).unwrap();
            prop_assert_eq!(bounded_decompress(&packed).unwrap(), data);
        }
    }
}
