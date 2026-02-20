use pyo3::prelude::*;
use std::time::Instant;

/// Token bucket rate limiter.
///
/// Refills tokens at a constant rate. Each `acquire` call refills based on
/// elapsed time since last refill, then attempts to deduct the requested
/// number of tokens. Thread-safety is not needed here because Python's GIL
/// ensures single-threaded access per call.
#[pyclass]
pub struct RustTokenBucket {
    capacity: f64,
    refill_rate: f64,
    tokens: f64,
    last_refill: Instant,
}

impl RustTokenBucket {
    /// Refill tokens based on time elapsed since last refill.
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
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
    fn new(capacity: f64, refill_rate: f64, initial_tokens: Option<f64>) -> Self {
        Self {
            capacity,
            refill_rate,
            tokens: initial_tokens.unwrap_or(capacity),
            last_refill: Instant::now(),
        }
    }

    /// Try to acquire `tokens` from the bucket.
    ///
    /// Refills first based on elapsed time, then checks if enough tokens
    /// are available. If so, deducts them and returns `true`. Otherwise
    /// returns `false` without modifying the token count.
    fn acquire(&mut self, tokens: f64) -> bool {
        self.refill();
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    /// Return the current token count after refilling based on elapsed time.
    #[getter]
    fn tokens(&mut self) -> f64 {
        self.refill();
        self.tokens
    }

    /// Reset the bucket to full capacity.
    fn reset(&mut self) {
        self.tokens = self.capacity;
        self.last_refill = Instant::now();
    }
}
