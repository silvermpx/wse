use indexmap::IndexMap;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use regex::Regex;
use std::sync::{Arc, Mutex, PoisonError};

/// Global regex cache (insertion-ordered for FIFO eviction, capped at 1024).
/// Entries are shared (`Arc`), so matching happens outside the lock.
static REGEX_CACHE: std::sync::LazyLock<Mutex<IndexMap<String, Arc<Regex>>>> =
    std::sync::LazyLock::new(|| Mutex::new(IndexMap::new()));

const REGEX_CACHE_MAX: usize = 1024;

/// Match an event dict against MongoDB-like filter criteria.
///
/// Supported operators:
///   $eq, $ne, $gt, $lt, $gte, $lte, $in, $nin,
///   $regex, $exists, $contains, $startswith, $endswith,
///   $and, $or
///
/// Every key of `criteria` is a condition and all of them must hold. `$and`
/// and `$or` take a list (or tuple) of sub-criteria dicts and are conditions
/// like any other key, so `{"account": "A", "$or": [...]}` requires both.
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

    for (key_obj, expected) in criteria.iter() {
        let key: String = key_obj.extract()?;

        let matched = match key.as_str() {
            "$and" => {
                let mut all = true;
                for sub in logical_operands(&expected, "$and")? {
                    if !rust_match_event(py, event, &sub)? {
                        all = false;
                        break;
                    }
                }
                all
            }
            "$or" => {
                let mut any = false;
                for sub in logical_operands(&expected, "$or")? {
                    if rust_match_event(py, event, &sub)? {
                        any = true;
                        break;
                    }
                }
                any
            }
            _ => {
                // Get the value (supports dot notation for nested fields)
                let value: Option<Py<PyAny>> = if key.contains('.') {
                    get_nested(event.as_any(), &key)?
                } else {
                    event.get_item(&key)?.map(|v| v.unbind())
                };
                match_value(py, value.as_ref(), &expected)?
            }
        };

        if !matched {
            return Ok(false);
        }
    }

    Ok(true)
}

/// The operand of `$and` / `$or`: a list or tuple of criteria dicts.
///
/// Anything else is a malformed filter and raises `TypeError` - silently
/// matching every event would deliver events the filter was meant to exclude.
fn logical_operands<'py>(value: &Bound<'py, PyAny>, op: &str) -> PyResult<Vec<Bound<'py, PyDict>>> {
    let items: Vec<Bound<'py, PyAny>> = if let Ok(list) = value.cast::<PyList>() {
        list.iter().collect()
    } else if let Ok(tuple) = value.cast::<PyTuple>() {
        tuple.iter().collect()
    } else {
        return Err(PyTypeError::new_err(format!(
            "{op} expects a list of filter dicts"
        )));
    };
    items
        .into_iter()
        .map(|item| {
            item.cast::<PyDict>()
                .cloned()
                .map_err(|_| PyTypeError::new_err(format!("{op} expects a list of filter dicts")))
        })
        .collect()
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
            _ => false,
        };

        if !result {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Check if a string matches a regex pattern (with caching).
/// Anchors to start-of-string (like Python re.match) unless pattern already starts with ^.
///
/// The cache lock is held only to look up or insert; compiling and matching
/// run outside it, so filters in many threads do not serialize on one mutex.
/// The cache holds plain data, so a poisoned lock is simply taken over.
fn regex_matches(pattern: &str, text: &str) -> PyResult<bool> {
    // Python re.match() anchors to start of string. Replicate by prepending ^.
    let anchored = if pattern.starts_with('^') {
        pattern.to_string()
    } else {
        format!("^(?:{})", pattern)
    };

    let cached = REGEX_CACHE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&anchored)
        .cloned();
    let re = match cached {
        Some(re) => re,
        None => {
            let compiled = Arc::new(Regex::new(&anchored).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("Invalid regex '{pattern}': {e}"))
            })?);
            let mut cache = REGEX_CACHE.lock().unwrap_or_else(PoisonError::into_inner);
            if cache.len() >= REGEX_CACHE_MAX {
                cache.shift_remove_index(0); // evict oldest (FIFO)
            }
            cache.insert(anchored, Arc::clone(&compiled));
            compiled
        }
    };

    Ok(re.is_match(text))
}
