//! The JWT functions of the Python module: `rust_jwt_decode` and
//! `rust_jwt_encode`, for an application's HTTP routes.

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::jwt::{JwtAlgorithm, jwt_decode_with_rotation, jwt_encode};

// ---------------------------------------------------------------------------
// PyO3 wrappers (for Python consumers — HTTP routes, etc.)
// ---------------------------------------------------------------------------

/// Decode a JWT token. Returns a dict of claims on success, None on failure.
#[pyfunction]
#[pyo3(signature = (token, secret, algorithm="HS256", issuer=None, audience=None, previous_secret=None, key_id=None))]
#[expect(
    clippy::too_many_arguments,
    reason = "PyO3 boundary: py + 7 Python kwargs"
)]
pub fn rust_jwt_decode<'py>(
    py: Python<'py>,
    token: &str,
    secret: &[u8],
    algorithm: &str,
    issuer: Option<&str>,
    audience: Option<&str>,
    previous_secret: Option<&[u8]>,
    key_id: Option<&str>,
) -> PyResult<Option<Py<PyDict>>> {
    match jwt_decode_with_rotation(
        token,
        secret,
        algorithm,
        issuer,
        audience,
        previous_secret,
        key_id,
    ) {
        Ok(payload) => {
            let dict = PyDict::new(py);
            if let serde_json::Value::Object(map) = payload {
                for (k, v) in map {
                    let py_val = json_value_to_py(py, &v);
                    dict.set_item(&k, py_val)?;
                }
            }
            Ok(Some(dict.unbind()))
        }
        Err(_) => Ok(None),
    }
}

/// Encode claims into a JWT token string.
///
/// claims: dict with all JWT claims (sub, exp, iat, iss, aud, etc.)
/// secret: For HS256 -- signing key as bytes. For RS256/ES256 -- PEM private key as bytes.
#[pyfunction]
#[pyo3(signature = (claims, secret, algorithm="HS256", key_id=None))]
pub fn rust_jwt_encode(
    _py: Python<'_>,
    claims: &Bound<'_, PyDict>,
    secret: &[u8],
    algorithm: &str,
    key_id: Option<&str>,
) -> PyResult<String> {
    let alg = JwtAlgorithm::from_str(algorithm).map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Unsupported algorithm: {algorithm}. Supported: {}",
            JwtAlgorithm::supported()
        ))
    })?;

    let json_val = pydict_to_json(claims)?;

    jwt_encode(&json_val, secret, alg, key_id)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("JWT encode error: {e}")))
}

// ---------------------------------------------------------------------------
// PyDict <-> serde_json conversion helpers
// ---------------------------------------------------------------------------

use pyo3::types::{PyBool, PyFloat, PyFrozenSet, PyInt, PyList, PySet, PyString, PyTuple};

pub(crate) fn pydict_to_json(dict: &Bound<'_, PyDict>) -> PyResult<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (k, v) in dict.iter() {
        let key = k.extract::<String>().unwrap_or_else(|_| format!("{}", k));
        map.insert(key, pyany_to_json(&v)?);
    }
    Ok(serde_json::Value::Object(map))
}

/// Convert one claim value. datetime becomes an integer UTC timestamp (as
/// PyJWT does for exp/iat/nbf), tuples, lists, sets and frozensets become
/// arrays. Anything JSON cannot represent raises TypeError rather than turning
/// into a string the token's consumers would misread.
fn pyany_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    let unsupported = || {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "JWT claim value of type {} is not JSON serializable",
            obj.get_type()
                .name()
                .map(|n| n.to_string())
                .unwrap_or_default()
        ))
    };
    if obj.is_none() {
        return Ok(serde_json::Value::Null);
    }
    // Bool before int (bool is subclass of int in Python)
    if obj.is_instance_of::<PyBool>() {
        return Ok(serde_json::Value::Bool(obj.extract::<bool>()?));
    }
    if obj.is_instance_of::<PyInt>() {
        if let Ok(i) = obj.extract::<i64>() {
            return Ok(serde_json::Value::Number(i.into()));
        }
        if let Ok(u) = obj.extract::<u64>() {
            return Ok(serde_json::Value::Number(u.into()));
        }
        return Err(unsupported());
    }
    if obj.is_instance_of::<PyFloat>() {
        return serde_json::Number::from_f64(obj.extract::<f64>()?)
            .map(serde_json::Value::Number)
            .ok_or_else(unsupported);
    }
    if obj.is_instance_of::<PyString>() {
        return Ok(serde_json::Value::String(obj.extract::<String>()?));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        return pydict_to_json(dict);
    }
    if obj.is_instance_of::<PyList>()
        || obj.is_instance_of::<PyTuple>()
        || obj.is_instance_of::<PySet>()
        || obj.is_instance_of::<PyFrozenSet>()
    {
        let mut arr = Vec::new();
        for item in obj.try_iter()? {
            arr.push(pyany_to_json(&item?)?);
        }
        return Ok(serde_json::Value::Array(arr));
    }
    // datetime: integer seconds since the epoch, naive values read as UTC.
    if obj.hasattr("utctimetuple")? && obj.hasattr("hour")? {
        let utc = obj.call_method0("utctimetuple")?;
        let ts: i64 = obj
            .py()
            .import("calendar")?
            .call_method1("timegm", (utc,))?
            .extract()?;
        return Ok(serde_json::Value::Number(ts.into()));
    }
    Err(unsupported())
}

fn json_value_to_py(py: Python<'_>, val: &serde_json::Value) -> Py<PyAny> {
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
                let _ = list.append(json_value_to_py(py, item));
            }
            list.into_any().unbind()
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                let _ = dict.set_item(k, json_value_to_py(py, v));
            }
            dict.into_any().unbind()
        }
    }
}
