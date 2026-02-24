use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Circuit Breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

pub(crate) struct CircuitBreaker {
    pub state: CircuitState,
    failure_count: u32,
    success_count: u32,
    half_open_calls: u32,
    last_failure: Option<Instant>,
    pub failure_threshold: u32,
    pub reset_timeout: Duration,
    pub half_open_max_calls: u32,
    pub success_threshold: u32,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            half_open_calls: 0,
            last_failure: None,
            failure_threshold: 10,
            reset_timeout: Duration::from_secs(60),
            half_open_max_calls: 3,
            success_threshold: 3,
        }
    }

    pub fn can_execute(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last) = self.last_failure {
                    if last.elapsed() >= self.reset_timeout {
                        self.state = CircuitState::HalfOpen;
                        self.half_open_calls = 0;
                        self.success_count = 0;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => self.half_open_calls < self.half_open_max_calls,
        }
    }

    pub fn record_success(&mut self) {
        match self.state {
            CircuitState::HalfOpen => {
                self.success_count += 1;
                if self.success_count >= self.success_threshold {
                    self.state = CircuitState::Closed;
                    self.failure_count = 0;
                    self.success_count = 0;
                }
            }
            CircuitState::Closed => {
                self.failure_count = 0;
            }
            CircuitState::Open => {}
        }
    }

    pub fn record_failure(&mut self) {
        self.last_failure = Some(Instant::now());
        match self.state {
            CircuitState::Closed => {
                self.failure_count += 1;
                if self.failure_count >= self.failure_threshold {
                    self.state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                self.state = CircuitState::Open;
                self.half_open_calls = 0;
            }
            CircuitState::Open => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Exponential Backoff with jitter (±20%)
// ---------------------------------------------------------------------------

pub(crate) struct ExponentialBackoff {
    initial: Duration,
    max: Duration,
    multiplier: f64,
    current: Duration,
}

impl ExponentialBackoff {
    pub fn new() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
            multiplier: 1.5,
            current: Duration::from_secs(1),
        }
    }

    pub fn next_delay(&mut self) -> Duration {
        let base = self.current.as_secs_f64();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        let jitter_factor = 0.8 + 0.4 * (nanos % 1000) as f64 / 1000.0;
        let delay = Duration::from_secs_f64((base * jitter_factor).min(self.max.as_secs_f64()));
        let next = base * self.multiplier;
        self.current = Duration::from_secs_f64(next.min(self.max.as_secs_f64()));
        delay
    }

    pub fn reset(&mut self) {
        self.current = self.initial;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_stays_closed() {
        let mut cb = CircuitBreaker::new();
        assert!(cb.can_execute());
        cb.record_success();
        assert_eq!(cb.state, CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let mut cb = CircuitBreaker::new();
        for _ in 0..10 {
            cb.record_failure();
        }
        assert_eq!(cb.state, CircuitState::Open);
        assert!(!cb.can_execute());
    }

    #[test]
    fn test_circuit_breaker_half_open_to_closed() {
        let mut cb = CircuitBreaker {
            reset_timeout: Duration::from_millis(1),
            ..CircuitBreaker::new()
        };
        for _ in 0..10 {
            cb.record_failure();
        }
        assert_eq!(cb.state, CircuitState::Open);
        std::thread::sleep(Duration::from_millis(2));
        assert!(cb.can_execute());
        assert_eq!(cb.state, CircuitState::HalfOpen);
        cb.record_success();
        cb.record_success();
        cb.record_success();
        assert_eq!(cb.state, CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_back_to_open() {
        let mut cb = CircuitBreaker {
            reset_timeout: Duration::from_millis(1),
            ..CircuitBreaker::new()
        };
        for _ in 0..10 {
            cb.record_failure();
        }
        std::thread::sleep(Duration::from_millis(2));
        assert!(cb.can_execute());
        assert_eq!(cb.state, CircuitState::HalfOpen);
        cb.record_failure();
        assert_eq!(cb.state, CircuitState::Open);
    }

    #[test]
    fn test_backoff_increases() {
        let mut b = ExponentialBackoff::new();
        let d1 = b.next_delay();
        let d2 = b.next_delay();
        let d3 = b.next_delay();
        assert!(d1.as_secs_f64() >= 0.7);
        assert!(d2.as_secs_f64() > d1.as_secs_f64() * 0.8);
        assert!(d3.as_secs_f64() <= 61.0);
    }

    #[test]
    fn test_backoff_reset() {
        let mut b = ExponentialBackoff::new();
        for _ in 0..20 {
            b.next_delay();
        }
        b.reset();
        let d = b.next_delay();
        assert!(d.as_secs_f64() < 2.0);
    }

    #[test]
    fn test_backoff_caps_at_max() {
        let mut b = ExponentialBackoff::new();
        for _ in 0..50 {
            b.next_delay();
        }
        let d = b.next_delay();
        assert!(d.as_secs_f64() <= 72.0); // 60 * 1.2 jitter max
    }
}
