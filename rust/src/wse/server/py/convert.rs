//! Conversion between Python objects and `serde_json` values.

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};

// ---------------------------------------------------------------------------
// PyObject <-> serde_json::Value conversion
// ---------------------------------------------------------------------------

/// Deepest nesting of dicts, lists and tuples converted.
const MAX_DEPTH: usize = 128;

/// Convert a Python object to JSON. A container inside itself, or nesting
/// deeper than `MAX_DEPTH`, raises ValueError (as `json.dumps` does) instead
/// of recursing until the stack overflows.
pub(super) fn pyobj_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    let mut path = Vec::new();
    to_json(obj, &mut path)
}

/// `path`: the containers being converted, outermost first.
fn to_json(obj: &Bound<'_, PyAny>, path: &mut Vec<usize>) -> PyResult<serde_json::Value> {
    if obj.is_none() {
        return Ok(serde_json::Value::Null);
    }
    // Bool before int (bool is subclass of int in Python)
    if obj.is_instance_of::<PyBool>()
        && let Ok(b) = obj.extract::<bool>()
    {
        return Ok(serde_json::Value::Bool(b));
    }
    if obj.is_instance_of::<PyInt>()
        && let Ok(i) = obj.extract::<i64>()
    {
        return Ok(serde_json::Value::Number(i.into()));
    }
    if obj.is_instance_of::<PyFloat>()
        && let Ok(f) = obj.extract::<f64>()
    {
        return Ok(serde_json::Number::from_f64(f).map_or(serde_json::Value::Null, Into::into));
    }
    if obj.is_instance_of::<PyString>()
        && let Ok(s) = obj.extract::<String>()
    {
        return Ok(serde_json::Value::String(s));
    }
    let is_container = obj.is_instance_of::<PyDict>()
        || obj.is_instance_of::<PyList>()
        || obj.is_instance_of::<PyTuple>();
    if is_container {
        let id = obj.as_ptr() as usize;
        if path.contains(&id) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Circular reference detected",
            ));
        }
        if path.len() >= MAX_DEPTH {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "nested deeper than {MAX_DEPTH} levels"
            )));
        }
        path.push(id);
        let value = if let Ok(dict) = obj.cast::<PyDict>() {
            let mut map = serde_json::Map::new();
            for (k, v) in dict.iter() {
                let key = k.extract::<String>().unwrap_or_else(|_| format!("{}", k));
                map.insert(key, to_json(&v, path)?);
            }
            serde_json::Value::Object(map)
        } else if let Ok(list) = obj.cast::<PyList>() {
            serde_json::Value::Array(
                list.iter()
                    .map(|item| to_json(&item, path))
                    .collect::<PyResult<_>>()?,
            )
        } else {
            let tup = obj.cast::<PyTuple>()?;
            serde_json::Value::Array(
                tup.iter()
                    .map(|item| to_json(&item, path))
                    .collect::<PyResult<_>>()?,
            )
        };
        path.pop();
        return Ok(value);
    }
    if obj.is_instance_of::<PyBytes>()
        && let Ok(b) = obj.extract::<Vec<u8>>()
    {
        return Ok(serde_json::Value::String(hex::encode(b)));
    }
    // Try isoformat() for datetime objects
    if let Ok(iso) = obj.call_method0("isoformat")
        && let Ok(s) = iso.extract::<String>()
    {
        return Ok(serde_json::Value::String(s));
    }
    // Fallback: str(obj)
    if let Ok(s) = obj.str()
        && let Ok(s) = s.extract::<String>()
    {
        return Ok(serde_json::Value::String(s));
    }
    Ok(serde_json::Value::Null)
}

/// Convert serde_json::Value to a Python object.
pub(in crate::wse::server) fn json_to_pyobj(py: Python<'_>, val: &serde_json::Value) -> Py<PyAny> {
    match val {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(b) => (*b)
            .into_pyobject(py)
            .unwrap()
            .to_owned()
            .into_any()
            .unbind(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py).unwrap().into_any().unbind()
            } else if let Some(f) = n.as_f64() {
                f.into_pyobject(py).unwrap().into_any().unbind()
            } else {
                py.None()
            }
        }
        serde_json::Value::String(s) => s.into_pyobject(py).unwrap().into_any().unbind(),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                let _ = list.append(json_to_pyobj(py, item));
            }
            list.into_any().unbind()
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                let _ = dict.set_item(k, json_to_pyobj(py, v));
            }
            dict.into_any().unbind()
        }
    }
}
