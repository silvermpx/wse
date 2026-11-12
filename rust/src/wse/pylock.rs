//! Shared state of Python-facing classes on free-threaded CPython.
//!
//! PyO3 checks borrows of a `#[pyclass]` at run time: a `&mut self` method fails
//! with `RuntimeError: Already borrowed` while any other call is inside the same
//! object. With the GIL that happened only when a method let go of it (for example
//! `RustWSEServer.drain_inbound()` while it waits for events); on free-threaded
//! Python (3.14t, 3.15t) any two threads sharing an object can run into it.
//!
//! The classes are therefore `#[pyclass(frozen)]` (every method takes `&self`) and
//! keep their mutable state behind a `std::sync::Mutex`, taken through [`lock`].

use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Lock `mutex` from a thread attached to the interpreter.
///
/// While it waits for the lock the thread detaches from the interpreter, so a
/// blocked caller never holds up the GIL or a stop-the-world pause of the
/// free-threaded build (`MutexExt::lock_py_attached`).
///
/// A panic inside a method poisons the mutex. PyO3 has already turned that panic
/// into a Python exception and the state is plain data, so later calls carry on,
/// as they did when PyO3's own borrow flag guarded the object.
pub(crate) fn lock<'a, T>(py: Python<'_>, mutex: &'a Mutex<T>) -> MutexGuard<'a, T> {
    mutex
        .lock_py_attached(py)
        .unwrap_or_else(PoisonError::into_inner)
}
