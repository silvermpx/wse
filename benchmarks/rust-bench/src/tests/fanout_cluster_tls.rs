use crate::config::Cli;
use crate::protocol::{self};
use crate::report::{self, LatencySummary, TierResult};
use crate::stats::{self};
use std::sync::atomic::Ordering;

/// Test 12: Multi-Instance Fan-out with TLS (Cluster protocol, two servers)
///
/// Same as fanout_cluster but with mTLS between cluster peers.
/// Measures TLS overhead on cross-instance delivery throughput.
///
/// Architecture:
///   Server A (--port, publisher) -> Cluster TLS -> Server B (--port2, subscribers)
///   Benchmark clients connect to Server B via plain WebSocket.
///
/// Requires:
///   bench_fanout_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007 --generate-tls
///   bench_fanout_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006 \
///       --tls-cert /tmp/wse_tls_test/server.crt --tls-key /tmp/wse_tls_test/server.key --tls-ca /tmp/wse_tls_test/ca.crt
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2.unwrap_or(cli.port + 1);

    stats::print_header(
        "TEST 12: Multi-Instance Fan-out with TLS (Cluster, two servers)",
        &format!(
            "Publish on :{} -> Cluster TLS -> :{} -> N subscribers for {}s. Cross-instance delivery over mTLS.",
            cli.port, port2, cli.duration
        ),
    );

    // Verify connectivity to Server B (subscribers connect here)
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
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
                "\n    Start servers with TLS (separate cluster ports):\n    \
                python benchmarks/bench_fanout_server.py --mode cluster --port {} --peers 127.0.0.1:6007 \\\n      \
                --cluster-port 6006 --cluster-addr 127.0.0.1:6006 --generate-tls\n    \
                python benchmarks/bench_fanout_server.py --mode cluster-subscribe --port {} --peers 127.0.0.1:6006 \\\n      \
                --cluster-port 6007 --cluster-addr 127.0.0.1:6007 \\\n      \
                --tls-cert /tmp/wse_tls_test/server.crt --tls-key /tmp/wse_tls_test/server.key --tls-ca /tmp/wse_tls_test/ca.crt",
                cli.port, port2
            );
            return Vec::new();
        }
    }

    let tiers = cli.tiers_for(crate::config::TestName::FanoutClusterTls);
    let mut results = Vec::new();

    for &n in &tiers {
        println!("\n  --- {} subscribers on Server B (:{}) [TLS cluster] ---", n, port2);

        // Connect all clients to Server B
        let connections = protocol::connect_batch(
            &cli.host,
            port2,
            &token,
            n,
            cli.batch_size,
            "compression=false&protocol_version=1",
        )
        .await;

        if connections.is_empty() {
            println!("    FAILED: no connections to Server B");
            continue;
        }

        let actual = connections.len();

        // Server A publishes, Server B fans out to our clients via TLS Cluster
        let result = super::fanout_broadcast::measure_fanout(
            connections,
            std::time::Duration::from_secs(cli.duration),
        )
        .await;

        let deliveries_per_sec = result.total_received as f64 / result.elapsed;
        let mb_per_sec = result.total_bytes as f64 / result.elapsed / 1_000_000.0;
        let published_per_sec = deliveries_per_sec / actual as f64;
        let raw_gaps = result.total_gaps.load(Ordering::Relaxed);
        let unique_gaps = if actual > 0 {
            raw_gaps / actual as u64
        } else {
            raw_gaps
        };

        println!("    Duration:       {:.2}s", result.elapsed);
        println!(
            "    Published:      {}/s (Server A -> TLS Cluster -> Server B)",
            stats::fmt_rate(published_per_sec)
        );
        println!(
            "    Deliveries:     {}/s (fan-out: {} x {} subs)",
            stats::fmt_rate(deliveries_per_sec),
            stats::fmt_rate(published_per_sec),
            actual
        );
        println!(
            "    Bandwidth:      {}",
            stats::fmt_bytes_per_sec(result.total_bytes as f64 / result.elapsed)
        );
        if unique_gaps > 0 {
            println!("    Seq gaps:       ~{} unique messages lost", unique_gaps);
        }

        if !result.latency.is_empty() {
            println!("    Delivery latency (publish A -> TLS Cluster -> B -> WS):");
            result.latency.print_summary("      ");
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
                "seq_gaps": unique_gaps,
                "tls": true,
            }),
        });

        protocol::close_all(result.connections).await;
        println!();
    }

    report::print_fanout_table(&results);
    results
}
