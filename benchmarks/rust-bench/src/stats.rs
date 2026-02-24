use hdrhistogram::Histogram;

/// Wrapper around HdrHistogram for latency recording.
/// Range: 1 microsecond to 60 seconds, 3 significant digits.
pub struct LatencyHistogram {
    inner: Histogram<u64>,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self {
            // 1us to 300s, 3 significant digits (fan-out stress tests can buffer 100s+)
            inner: Histogram::new_with_bounds(1, 300_000_000, 3).unwrap(),
        }
    }

    /// Record a latency value in microseconds.
    pub fn record_us(&mut self, us: u64) {
        let _ = self.inner.record(us);
    }

    /// Record a latency value in milliseconds (converts to us internally).
    pub fn record_ms(&mut self, ms: f64) {
        let us = (ms * 1000.0) as u64;
        self.record_us(us.max(1));
    }

    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get percentile value in milliseconds.
    pub fn percentile_ms(&self, p: f64) -> f64 {
        self.inner.value_at_percentile(p) as f64 / 1000.0
    }

    pub fn min_ms(&self) -> f64 {
        self.inner.min() as f64 / 1000.0
    }

    pub fn max_ms(&self) -> f64 {
        self.inner.max() as f64 / 1000.0
    }

    pub fn mean_ms(&self) -> f64 {
        self.inner.mean() / 1000.0
    }

    pub fn stdev_ms(&self) -> f64 {
        self.inner.stdev() / 1000.0
    }

    /// Merge another histogram into this one.
    pub fn merge(&mut self, other: &LatencyHistogram) {
        let _ = self.inner.add(&other.inner);
    }

    /// Print a formatted latency summary.
    pub fn print_summary(&self, indent: &str) {
        if self.is_empty() {
            println!("{indent}NO DATA");
            return;
        }
        println!("{indent}p50:    {:>10.2} ms", self.percentile_ms(50.0));
        println!("{indent}p95:    {:>10.2} ms", self.percentile_ms(95.0));
        println!("{indent}p99:    {:>10.2} ms", self.percentile_ms(99.0));
        if self.len() >= 100 {
            println!("{indent}p99.9:  {:>10.2} ms", self.percentile_ms(99.9));
        }
        if self.len() >= 1000 {
            println!("{indent}p99.99: {:>10.2} ms", self.percentile_ms(99.99));
        }
        println!("{indent}min:    {:>10.2} ms", self.min_ms());
        println!("{indent}max:    {:>10.2} ms", self.max_ms());
        println!("{indent}mean:   {:>10.2} ms", self.mean_ms());
        println!("{indent}stdev:  {:>10.2} ms", self.stdev_ms());
    }

    /// Return summary as JSON-serializable struct.
    #[allow(dead_code)]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "count": self.len(),
            "p50_ms": self.percentile_ms(50.0),
            "p95_ms": self.percentile_ms(95.0),
            "p99_ms": self.percentile_ms(99.0),
            "p99_9_ms": self.percentile_ms(99.9),
            "p99_99_ms": self.percentile_ms(99.99),
            "min_ms": self.min_ms(),
            "max_ms": self.max_ms(),
            "mean_ms": self.mean_ms(),
            "stdev_ms": self.stdev_ms(),
        })
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Format a large number with commas: 1234567 -> "1,234,567"
pub fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Format bytes per second to human-readable.
pub fn fmt_bytes_per_sec(bps: f64) -> String {
    if bps >= 1_000_000_000.0 {
        format!("{:.1} GB/s", bps / 1_000_000_000.0)
    } else if bps >= 1_000_000.0 {
        format!("{:.1} MB/s", bps / 1_000_000.0)
    } else if bps >= 1_000.0 {
        format!("{:.1} KB/s", bps / 1_000.0)
    } else {
        format!("{:.0} B/s", bps)
    }
}

/// Format a float with commas in the integer part.
pub fn fmt_rate(n: f64) -> String {
    fmt_num(n as u64)
}

pub fn print_header(title: &str, detail: &str) {
    println!("\n{}", "=".repeat(70));
    println!("  {title}");
    if !detail.is_empty() {
        println!("  {detail}");
    }
    println!("{}", "=".repeat(70));
}
