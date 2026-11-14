use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::sync::Mutex;
use std::time::Instant;

use super::pylock::lock;

/// Token bucket rate limiter.
///
/// Refills tokens at a constant rate. Each `acquire` call refills based on
/// elapsed time since last refill, then attempts to deduct the requested
/// number of tokens. Safe to share between threads, also on free-threaded
/// Python: the refill and the deduction happen under one lock, so concurrent
/// callers never spend more tokens than the bucket holds.
#[pyclass(frozen)]
pub struct RustTokenBucket {
    capacity: f64,
    refill_rate: f64,
    state: Mutex<BucketState>,
}

/// The part of the bucket that changes between calls.
struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

impl BucketState {
    /// Refill tokens based on time elapsed since last refill.
    fn refill(&mut self, capacity: f64, refill_rate: f64) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * refill_rate).min(capacity);
            self.last_refill = now;
        }
    }
}

#[pymethods]
impl RustTokenBucket {
    /// Create a new token bucket.
    ///
    /// # Arguments
    /// * `capacity`       - Maximum number of tokens the bucket can hold.
    /// * `refill_rate`    - Tokens added per second.
    /// * `initial_tokens` - Starting tokens (defaults to capacity if None).
    #[new]
    #[pyo3(signature = (capacity, refill_rate, initial_tokens=None))]
    fn new(capacity: f64, refill_rate: f64, initial_tokens: Option<f64>) -> PyResult<Self> {
        let tokens = initial_tokens.unwrap_or(capacity);
        // NaN fails every comparison, so it is rejected here too.
        if !(capacity.is_finite() && capacity > 0.0) {
            return Err(PyValueError::new_err("capacity must be a positive number"));
        }
        if !(refill_rate.is_finite() && refill_rate >= 0.0) {
            return Err(PyValueError::new_err(
                "refill_rate must be a non-negative number",
            ));
        }
        if !(0.0..=capacity).contains(&tokens) {
            return Err(PyValueError::new_err(
                "initial_tokens must be between 0 and capacity",
            ));
        }
        Ok(Self {
            capacity,
            refill_rate,
            state: Mutex::new(BucketState {
                tokens,
                last_refill: Instant::now(),
            }),
        })
    }

    /// Try to acquire `tokens` from the bucket.
    ///
    /// Refills first based on elapsed time, then checks if enough tokens
    /// are available. If so, deducts them and returns `true`. Otherwise
    /// returns `false` without modifying the token count.
    #[pyo3(signature = (tokens=1.0))]
    fn acquire(&self, py: Python<'_>, tokens: f64) -> PyResult<bool> {
        // A negative count would add tokens; NaN would compare false forever.
        if !(tokens.is_finite() && tokens >= 0.0) {
            return Err(PyValueError::new_err(
                "tokens must be a non-negative number",
            ));
        }
        let mut state = lock(py, &self.state);
        state.refill(self.capacity, self.refill_rate);
        if state.tokens >= tokens {
            state.tokens -= tokens;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Return the current token count after refilling based on elapsed time.
    #[getter]
    fn tokens(&self, py: Python<'_>) -> f64 {
        let mut state = lock(py, &self.state);
        state.refill(self.capacity, self.refill_rate);
        state.tokens
    }

    /// Reset the bucket to full capacity.
    fn reset(&self, py: Python<'_>) {
        let mut state = lock(py, &self.state);
        state.tokens = self.capacity;
        state.last_refill = Instant::now();
    }
}
