//! The extension module of the `wse-server` Python package: the WSE engine
//! (the `wse` crate) with its Python layer.

use mimalloc::MiMalloc;
use pyo3::prelude::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[pymodule]
fn _wse_accel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    wse::python::init(m)
}
