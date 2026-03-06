use crate::config::Cli;
use crate::protocol;
use crate::report::{self, LatencySummary, TierResult};
use crate::stats;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Test 11: Multi-Instance Fan-out (Cluster protocol, two servers)
///
/// Subscribers split 50/50 across Server A and Server B.
/// Server A publishes -> local fan-out to N/2 subs + cluster replication to B -> fan-out to N/2 subs.
/// Tests true horizontal scaling: total deliveries = published * N.
///
/// Architecture:
///   Server A (--port, publisher + N/2 subscribers) -> broadcasts locally + cluster to B
///   Server B (--port2, N/2 subscribers) -> receives cluster MSG -> fan-out to its clients
///   Benchmark clients connect to BOTH servers (split 50/50).
///
/// Requires:
///   bench_fanout_server.py --mode cluster --port 5006 --peers 127.0.0.1:7007 --cluster-port 7006 --cluster-addr 127.0.0.1:7006
///   bench_fanout_server.py --mode cluster --port 5007 --peers 127.0.0.1:7006 --cluster-port 7007 --cluster-addr 127.0.0.1:7007
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2.unwrap_or(cli.port + 1);

    stats::print_header(
        "TEST 11: Multi-Instance Fan-out (Cluster, two servers, split 50/50)",
        &format!(
            "Publish on :{} -> local N/2 + Cluster -> :{} N/2 subs for {}s. Horizontal scaling.",
            cli.port, port2, cli.duration
        ),
    );

    // Verify connectivity to both servers
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");

    print!("    Checking Server A (:{})... ", cli.port);
    match protocol::connect_and_handshake(
        &cli.host,
        cli.port,
        &token,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => {
            protocol::close_all(vec![ws]).await;
            println!("OK");
        }
        Err(e) => {
            eprintln!("FAILED: {e}");
            return Vec::new();
        }
    }

    print!("    Checking Server B (:{})... ", port2);
    match protocol::connect_and_handshake(
        &cli.host,
        port2,
        &token,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => {
            protocol::close_all(vec![ws]).await;
            println!("OK");
        }
        Err(e) => {
            eprintln!("FAILED: {e}");
            eprintln!(
                "\n    Start Server B: python benchmarks/bench_fanout_server.py --mode cluster --port {port2} --peers 127.0.0.1:{}", cli.port
            );
            return Vec::new();
        }
    }

    let tiers = cli.tiers_for(crate::config::TestName::FanoutCluster);
    let mut results = Vec::new();

    for &n in &tiers {
        let half = n / 2;
        let half_b = n - half; // handles odd numbers
        println!(
            "\n  --- {} subscribers ({} on A:{}, {} on B:{}) ---",
            n, half, cli.port, half_b, port2
        );

        // Connect half to Server A, half to Server B (in parallel)
        let (conns_a, conns_b) = tokio::join!(
            protocol::connect_batch(
                &cli.host,
                cli.port,
                &token,
                half,
                cli.batch_size,
                "compression=false&protocol_version=1",
            ),
            protocol::connect_batch(
                &cli.host,
                port2,
                &token,
                half_b,
                cli.batch_size,
                "compression=false&protocol_version=1",
            )
        );

        let actual_a = conns_a.len();
        let actual_b = conns_b.len();
        let actual = actual_a + actual_b;

        if actual == 0 {
            println!("    FAILED: no connections");
            continue;
        }

        println!(
            "    Split: A={}/{}, B={}/{}",
            actual_a, half, actual_b, half_b
        );

        // Merge all connections for unified measurement
        let mut connections = conns_a;
        connections.extend(conns_b);

        // Capture drops counters before measurement
        let drops_a_before =
            protocol::query_slow_consumer_drops(&cli.host, cli.metrics_port_for(cli.port)).await;
        let drops_b_before =
            protocol::query_slow_consumer_drops(&cli.host, cli.metrics_port_for(port2)).await;

        // Measure: Server A publishes, both servers fan out to their local clients
        let result = super::fanout_broadcast::measure_fanout(
            connections,
            std::time::Duration::from_secs(cli.duration),
        )
        .await;

        let deliveries_per_sec = result.total_received as f64 / result.elapsed;
        let mb_per_sec = result.total_bytes as f64 / result.elapsed / 1_000_000.0;
        let published_per_sec = deliveries_per_sec / actual as f64;
        let raw_gaps = result.total_gaps.load(Ordering::Relaxed);

        println!("    Duration:       {:.2}s", result.elapsed);
        println!(
            "    Published:      {}/s (Server A -> local + Cluster -> Server B)",
            stats::fmt_rate(published_per_sec)
        );
        println!(
            "    Deliveries:     {}/s (fan-out: {} x {} subs across 2 nodes)",
            stats::fmt_rate(deliveries_per_sec),
            stats::fmt_rate(published_per_sec),
            actual
        );
        println!(
            "    Bandwidth:      {}",
            stats::fmt_bytes_per_sec(result.total_bytes as f64 / result.elapsed)
        );
        if raw_gaps > 0 {
            println!("    Seq gaps:       ~{} unique messages lost", raw_gaps);
        }

        if !result.latency.is_empty() {
            println!("    Delivery latency:");
            result.latency.print_summary("      ");
        }

        // Query drops from both servers
        let drops_a_after =
            protocol::query_slow_consumer_drops(&cli.host, cli.metrics_port_for(cli.port)).await;
        let drops_b_after =
            protocol::query_slow_consumer_drops(&cli.host, cli.metrics_port_for(port2)).await;
        let da = match (drops_a_before, drops_a_after) {
            (Some(b), Some(a)) => Some(a.saturating_sub(b)),
            _ => drops_a_after,
        };
        let db = match (drops_b_before, drops_b_after) {
            (Some(b), Some(a)) => Some(a.saturating_sub(b)),
            _ => drops_b_after,
        };
        if let (Some(a), Some(b)) = (da, db) {
            if a + b == 0 {
                println!("    Server drops:   0 (ok)");
            } else {
                println!("    Server drops:   A={}, B={}", a, b);
            }
        }

        results.push(TierResult {
            tier: n,
            connected: actual,
            errors: n - actual,
            duration_secs: result.elapsed,
            messages_sent: (published_per_sec * result.elapsed) as u64,
            messages_received: result.total_received,
            bytes_sent: 0,
            latency: if result.latency.is_empty() {
                None
            } else {
                Some(LatencySummary::from_histogram(&result.latency))
            },
            extra: serde_json::json!({
                "published_per_sec": published_per_sec,
                "deliveries_per_sec": deliveries_per_sec,
                "mb_per_sec": mb_per_sec,
                "seq_gaps": raw_gaps,
                "split_a": actual_a,
                "split_b": actual_b,
            }),
        });

        protocol::close_all(result.connections).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        println!();
    }

    report::print_fanout_table(&results);
    results
}
