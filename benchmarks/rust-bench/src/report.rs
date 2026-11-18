use std::sync::atomic::{AtomicBool, Ordering};

use crate::stats::LatencyHistogram;

/// A check of a run failed: the run says so in its exit code.
static FAILED: AtomicBool = AtomicBool::new(false);

/// A check failed (it was printed FAILED).
pub fn failed() {
    FAILED.store(true, Ordering::Relaxed);
}

/// Whether a check of this run failed.
pub fn any_failed() -> bool {
    FAILED.load(Ordering::Relaxed)
}

/// Single tier result (used across all tests).
/// Each field goes into the JSON report (`--output json`).
#[derive(Clone, serde::Serialize)]
pub struct TierResult {
    pub tier: usize,
    pub connected: usize,
    pub errors: usize,
    pub duration_secs: f64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub latency: Option<LatencySummary>,
    pub extra: serde_json::Value,
}

#[derive(Clone, serde::Serialize)]
pub struct LatencySummary {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p99_9: f64,
    pub p99_99: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stdev: f64,
    pub count: u64,
}

impl LatencySummary {
    pub fn from_histogram(h: &LatencyHistogram) -> Self {
        Self {
            p50: h.percentile_ms(50.0),
            p95: h.percentile_ms(95.0),
            p99: h.percentile_ms(99.0),
            p99_9: h.percentile_ms(99.9),
            p99_99: h.percentile_ms(99.99),
            min: h.min_ms(),
            max: h.max_ms(),
            mean: h.mean_ms(),
            stdev: h.stdev_ms(),
            count: h.len(),
        }
    }
}

/// What the run gives back past the tables each test prints: with `--output
/// json` every tier of every test as JSON, with `--output markdown` a table
/// for each test.
pub fn write(format: crate::config::OutputFormat, results: &[(String, Vec<TierResult>)]) {
    use crate::config::OutputFormat;
    match format {
        OutputFormat::Text => {}
        OutputFormat::Json => {
            let tests: Vec<serde_json::Value> = results
                .iter()
                .map(|(test, tiers)| serde_json::json!({ "test": test, "tiers": tiers }))
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&tests).unwrap_or_default()
            );
        }
        OutputFormat::Markdown => {
            for (test, tiers) in results {
                println!("\n### {test}\n");
                println!("| Tier | Connected | Errors | Msg/s | p50 ms | p99 ms |");
                println!("|---:|---:|---:|---:|---:|---:|");
                for tier in tiers {
                    let (p50, p99) = tier.latency.as_ref().map_or(("-".into(), "-".into()), |l| {
                        (format!("{:.2}", l.p50), format!("{:.2}", l.p99))
                    });
                    println!(
                        "| {} | {} | {} | {:.0} | {p50} | {p99} |",
                        tier.tier,
                        tier.connected,
                        tier.errors,
                        tier.msg_per_sec()
                    );
                }
            }
        }
    }
}

impl TierResult {
    pub fn msg_per_sec(&self) -> f64 {
        if self.duration_secs > 0.0 {
            self.messages_sent as f64 / self.duration_secs
        } else {
            0.0
        }
    }

    pub fn mb_per_sec(&self) -> f64 {
        if self.duration_secs > 0.0 {
            self.bytes_sent as f64 / self.duration_secs / 1_000_000.0
        } else {
            0.0
        }
    }
}

/// Print a markdown-style summary table for throughput tests.
pub fn print_throughput_table(results: &[TierResult]) {
    println!("\n  | Connections | Msg/s | MB/s | Per-conn msg/s | Errors |");
    println!("  |-----------|-------|------|---------------|--------|");
    for r in results {
        let msg_s = r.msg_per_sec();
        let per_conn = if r.connected > 0 {
            msg_s / r.connected as f64
        } else {
            0.0
        };
        println!(
            "  | {:>9} | {:>11} | {:>8.1} | {:>13.0} | {:>6} |",
            format_num(r.tier),
            format_num(msg_s as usize),
            r.mb_per_sec(),
            per_conn,
            r.errors,
        );
    }
}

/// Print a markdown-style matrix table (rows=sizes, cols=connections).
pub fn print_matrix_table(
    sizes: &[usize],
    tiers: &[usize],
    results: &std::collections::HashMap<(usize, usize), TierResult>,
) {
    // Header
    print!("\n  | Size \\ Conns");
    for t in tiers {
        print!(" | {:>7}", format_num(*t));
    }
    println!(" |");

    print!("  |----------");
    for _ in tiers {
        print!("|--------");
    }
    println!("|");

    // Rows
    for &size in sizes {
        print!("  | {:>8}", format_bytes(size));
        for &tier in tiers {
            if let Some(r) = results.get(&(size, tier)) {
                let msg_s = r.msg_per_sec();
                if msg_s >= 1_000_000.0 {
                    print!(" | {:>5.1}M", msg_s / 1_000_000.0);
                } else if msg_s >= 1_000.0 {
                    print!(" | {:>5.1}K", msg_s / 1_000.0);
                } else {
                    print!(" | {:>6.0}", msg_s);
                }
            } else {
                print!(" |    N/A");
            }
        }
        println!(" |");
    }
}

fn format_num(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.0}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_bytes(n: usize) -> String {
    if n >= 1_048_576 {
        format!("{} MB", n / 1_048_576)
    } else if n >= 1_024 {
        format!("{} KB", n / 1_024)
    } else {
        format!("{} B", n)
    }
}

/// Print a markdown-style table for fan-out test results.
pub fn print_fanout_table(results: &[TierResult]) {
    println!(
        "\n  | Subscribers | Published/s | Deliveries/s | MB/s   | p50     | p95     | p99     | Gaps |"
    );
    println!(
        "  |------------|------------|-------------|--------|---------|---------|---------|------|"
    );
    for r in results {
        let deliveries_s = if r.duration_secs > 0.0 {
            r.messages_received as f64 / r.duration_secs
        } else {
            0.0
        };
        let published_s = r
            .extra
            .get("published_per_sec")
            .and_then(|v| v.as_f64())
            .unwrap_or_else(|| {
                if r.connected > 0 {
                    deliveries_s / r.connected as f64
                } else {
                    0.0
                }
            });
        let mb_s = r
            .extra
            .get("mb_per_sec")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let gaps = r
            .extra
            .get("seq_gaps")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let (p50, p95, p99) = if let Some(ref lat) = r.latency {
            (
                format!("{:.2}ms", lat.p50),
                format!("{:.2}ms", lat.p95),
                format!("{:.2}ms", lat.p99),
            )
        } else {
            ("N/A".into(), "N/A".into(), "N/A".into())
        };

        println!(
            "  | {:>10} | {:>10} | {:>11} | {:>6.1} | {:>7} | {:>7} | {:>7} | {:>4} |",
            format_num(r.tier),
            format_num(published_s as usize),
            format_num(deliveries_s as usize),
            mb_s,
            p50,
            p95,
            p99,
            gaps,
        );
    }
}
