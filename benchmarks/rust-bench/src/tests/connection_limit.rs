use crate::config::Cli;
use crate::protocol;
use crate::report::TierResult;
use crate::stats;

/// Test 7: Connection Limit Finder
/// Binary search for the absolute maximum connections the server can handle.
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "TEST 7: Connection Limit Finder",
        &format!(
            "Binary search for max stable connections (up to {}).",
            cli.max_connections
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let max = cli.max_connections;
    let mut results = Vec::new();

    // Phase 1: Exponential probe — double until failure
    println!("\n  Phase 1: Exponential probe (doubling)");
    let mut current = 1_000;
    let mut last_success = 0;
    let mut first_failure = max;

    while current <= max {
        let (ok, errors, rate) = probe_connections(cli, &token, current).await;
        let error_rate = errors as f64 / current as f64;

        if ok && error_rate < 0.05 {
            println!(
                "    {} conns: OK ({} errors, {:.0} conn/s)",
                current, errors, rate
            );
            last_success = current;
            results.push(make_result(current, current - errors, errors, rate));
            current *= 2;
        } else {
            println!(
                "    {} conns: FAIL ({} errors = {:.1}%)",
                current,
                errors,
                error_rate * 100.0
            );
            first_failure = current;
            results.push(make_result(current, current - errors, errors, rate));
            break;
        }
    }

    if last_success == 0 {
        println!("\n    Could not establish even 1000 connections.");
        return results;
    }

    if first_failure > max {
        println!("\n    All probes succeeded up to max ({}).", max);
        return results;
    }

    // Phase 2: Binary search between last_success and first_failure
    println!(
        "\n  Phase 2: Binary search [{} - {}]",
        last_success, first_failure
    );
    let mut lo = last_success;
    let mut hi = first_failure;

    while hi - lo > 1_000 {
        let mid = (lo + hi) / 2;
        // Round to nearest 500 for cleaner numbers
        let mid = (mid / 500) * 500;
        if mid <= lo || mid >= hi {
            break;
        }

        let (ok, errors, rate) = probe_connections(cli, &token, mid).await;
        let error_rate = errors as f64 / mid as f64;

        if ok && error_rate < 0.05 {
            println!(
                "    {} conns: OK ({} errors, {:.0} conn/s)",
                mid, errors, rate
            );
            lo = mid;
            results.push(make_result(mid, mid - errors, errors, rate));
        } else {
            println!(
                "    {} conns: FAIL ({} errors = {:.1}%)",
                mid,
                errors,
                error_rate * 100.0
            );
            hi = mid;
            results.push(make_result(mid, mid - errors, errors, rate));
        }

        // Cooldown between probes
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    println!(
        "\n  Result: Max stable connections = ~{} (error boundary at {})",
        lo, hi
    );

    results
}

/// Try to establish `n` connections, return (success, error_count, rate).
async fn probe_connections(cli: &Cli, token: &str, n: usize) -> (bool, usize, f64) {
    let start = std::time::Instant::now();

    let connections = protocol::connect_batch(
        &cli.host,
        cli.port,
        token,
        n,
        cli.batch_size,
        "compression=false&protocol_version=1",
    )
    .await;

    let elapsed = start.elapsed().as_secs_f64();
    let connected = connections.len();
    let errors = n - connected;
    let rate = connected as f64 / elapsed;

    // Hold for a few seconds to verify stability
    if connected > 0 {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }

    protocol::close_all(connections).await;

    // Allow OS to reclaim resources
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    (connected > 0, errors, rate)
}

fn make_result(tier: usize, connected: usize, errors: usize, rate: f64) -> TierResult {
    TierResult {
        tier,
        connected,
        errors,
        duration_secs: 0.0,
        messages_sent: 0,
        messages_received: 0,
        bytes_sent: 0,
        latency: None,
        extra: serde_json::json!({
            "accept_rate": rate,
        }),
    }
}
