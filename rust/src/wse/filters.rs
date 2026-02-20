use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use regex::Regex;
use std::collections::HashMap;
use std::sync::Mutex;

/// Thread-local regex cache to avoid recompiling patterns.
static REGEX_CACHE: std::sync::LazyLock<Mutex<HashMap<String, Regex>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Match an event dict against MongoDB-like filter criteria.
///
/// Supported operators:
///   $eq, $ne, $gt, $lt, $gte, $lte, $in, $nin,
///   $regex, $exists, $contains, $startswith, $endswith,
///   $and, $or
///
/// Nested field access via dot notation: "payload.price"
#[pyfunction]
pub fn rust_match_event(
    py: Python<'_>,
    event: &Bound<'_, PyDict>,
    criteria: &Bound<'_, PyDict>,
) -> PyResult<bool> {
    // Special control filters — always match
    if criteria.contains("start_from_latest")? {
        return Ok(true);
    }

    // Handle top-level $and / $or
    if let Some(and_val) = criteria.get_item("$and")? {
        if let Ok(list) = and_val.extract::<Bound<'_, PyList>>() {
            for item in list.iter() {
                let sub: &Bound<'_, PyDict> = item.cast()?;
                if !rust_match_event(py, event, sub)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
    }
    if let Some(or_val) = criteria.get_item("$or")? {
        if let Ok(list) = or_val.extract::<Bound<'_, PyList>>() {
            for item in list.iter() {
                let sub: &Bound<'_, PyDict> = item.cast()?;
                if rust_match_event(py, event, sub)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }

    for (key_obj, expected) in criteria.iter() {
        let key: String = key_obj.extract()?;

        // Skip $and/$or (already handled)
        if key == "$and" || key == "$or" {
            continue;
        }

        // Get the value (supports dot notation for nested fields)
        let value: Option<Py<PyAny>> = if key.contains('.') {
            get_nested(event.as_any(), &key)?
        } else {
            event.get_item(&key)?.map(|v| v.unbind())
        };

        if !match_value(py, value.as_ref(), &expected)? {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Navigate nested dicts via dot-separated path.
fn get_nested(obj: &Bound<'_, PyAny>, path: &str) -> PyResult<Option<Py<PyAny>>> {
    let keys: Vec<&str> = path.split('.').collect();
    let mut current: Py<PyAny> = obj.clone().unbind();

    for key in keys {
        let py = obj.py();
        let bound = current.bind(py);
        if let Ok(dict) = bound.cast::<PyDict>() {
            match dict.get_item(key)? {
                Some(val) => current = val.unbind(),
                None => return Ok(None),
            }
        } else {
            return Ok(None);
        }
    }

    Ok(Some(current))
}

/// Compare a value against an expected criterion (scalar or operator dict).
fn match_value(
    py: Python<'_>,
    value: Option<&Py<PyAny>>,
    expected: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    // If expected is a dict, it contains operators
    if let Ok(ops) = expected.cast::<PyDict>() {
        return match_operators(py, value, ops);
    }

    // Direct equality comparison
    match value {
        Some(val) => {
            let val_bound = val.bind(py);
            val_bound.eq(expected)
        }
        None => Ok(expected.is_none()),
    }
}

/// Apply operator dict against a value.
fn match_operators(
    py: Python<'_>,
    value: Option<&Py<PyAny>>,
    ops: &Bound<'_, PyDict>,
) -> PyResult<bool> {
    for (op_key, op_val) in ops.iter() {
        let op: String = op_key.extract()?;
        let result = match op.as_str() {
            "$eq" => match value {
                Some(v) => v.bind(py).eq(&op_val)?,
                None => op_val.is_none(),
            },
            "$ne" => match value {
                Some(v) => !v.bind(py).eq(&op_val)?,
                None => !op_val.is_none(),
            },
            "$gt" => match value {
                Some(v) => v.bind(py).gt(&op_val)?,
                None => false,
            },
            "$lt" => match value {
                Some(v) => v.bind(py).lt(&op_val)?,
                None => false,
            },
            "$gte" => match value {
                Some(v) => v.bind(py).ge(&op_val)?,
                None => false,
            },
            "$lte" => match value {
                Some(v) => v.bind(py).le(&op_val)?,
                None => false,
            },
            "$in" => {
                if let Ok(list) = op_val.cast::<PyList>() {
                    match value {
                        Some(v) => list.contains(v.bind(py))?,
                        None => false,
                    }
                } else {
                    false
                }
            }
            "$nin" => {
                if let Ok(list) = op_val.cast::<PyList>() {
                    match value {
                        Some(v) => !list.contains(v.bind(py))?,
                        None => true,
                    }
                } else {
                    true
                }
            }
            "$exists" => {
                let should_exist: bool = op_val.extract()?;
                (value.is_some()) == should_exist
            }
            "$regex" => {
                let pattern: String = op_val.extract()?;
                match value {
                    Some(v) => {
                        let val_str: String = v.bind(py).str()?.extract()?;
                        regex_matches(&pattern, &val_str)?
                    }
                    None => false,
                }
            }
            "$contains" => {
                let substr: String = op_val.extract()?;
                match value {
                    Some(v) => {
                        let val_str: String = v.bind(py).str()?.extract()?;
                        val_str.contains(&substr)
                    }
                    None => false,
                }
            }
            "$startswith" => {
                let prefix: String = op_val.extract()?;
                match value {
                    Some(v) => {
                        let val_str: String = v.bind(py).str()?.extract()?;
                        val_str.starts_with(&prefix)
                    }
                    None => false,
                }
            }
            "$endswith" => {
                let suffix: String = op_val.extract()?;
                match value {
                    Some(v) => {
                        let val_str: String = v.bind(py).str()?.extract()?;
                        val_str.ends_with(&suffix)
                    }
                    None => false,
                }
            }
            _ => true,
        };

        if !result {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Check if a string matches a regex pattern (with caching).
/// Anchors to start-of-string (like Python re.match) unless pattern already starts with ^.
fn regex_matches(pattern: &str, text: &str) -> PyResult<bool> {
    let mut cache = REGEX_CACHE
        .lock()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Regex cache lock: {e}")))?;

    // Python re.match() anchors to start of string. Replicate by prepending ^.
    let anchored = if pattern.starts_with('^') {
        pattern.to_string()
    } else {
        format!("^(?:{})", pattern)
    };

    let re = if let Some(cached) = cache.get(&anchored) {
        cached
    } else {
        let compiled = Regex::new(&anchored).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("Invalid regex '{pattern}': {e}"))
        })?;
        cache.insert(anchored.clone(), compiled);
        cache.get(&anchored).unwrap()
    };

    Ok(re.is_match(text))
}
