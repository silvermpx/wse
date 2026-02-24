use crate::config::Cli;
use crate::protocol;
use crate::report::{LatencySummary, TierResult};
use crate::stats::{self, LatencyHistogram};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Test 1: Connection Storm
/// Opens N connections as fast as possible, measuring accept rate and handshake latency.
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "TEST 1: Connection Storm",
        "All connections opened as fast as possible. Measures accept rate + handshake latency.",
    );

    let tiers = cli.tiers_for(crate::config::TestName::ConnectionStorm);
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut results = Vec::new();

    for &n in &tiers {
        println!("\n  --- {} connections ---", n);

        let errors = Arc::new(AtomicUsize::new(0));
        let total_start = Instant::now();

        // Spawn all connection attempts in batches
        let batch_size = cli.batch_size;
        let mut all_latencies: Vec<f64> = Vec::with_capacity(n);
        let mut connections = Vec::with_capacity(n);

        let use_multi_ip = n > 60_000 && cli.host == "127.0.0.1";
        let mut conn_idx = 0usize;

        for start in (0..n).step_by(batch_size) {
            let end = (start + batch_size).min(n);
            let chunk = end - start;

            let mut handles = Vec::with_capacity(chunk);
            for _ in 0..chunk {
                let host = cli.host.clone();
                let port = cli.port;
                let tok = token.clone();
                let err_count = errors.clone();

                if use_multi_ip {
                    let ip_idx = (conn_idx / 60_000) + 1;
                    assert!(
                        ip_idx <= 254,
                        "exceeded 127.0.0.254 — too many connections for loopback"
                    );
                    let source_addr: std::net::SocketAddr =
                        format!("127.0.0.{}:0", ip_idx).parse().unwrap();
                    conn_idx += 1;
                    handles.push(tokio::spawn(async move {
                        let t0 = Instant::now();
                        match protocol::connect_and_handshake_from(
                            &host,
                            port,
                            &tok,
                            "compression=false&protocol_version=1",
                            source_addr,
                            15,
                        )
                        .await
                        {
                            Ok(ws) => {
                                let lat_ms = t0.elapsed().as_secs_f64() * 1000.0;
                                Ok((ws, lat_ms))
                            }
                            Err(_) => {
                                err_count.fetch_add(1, Ordering::Relaxed);
                                Err(())
                            }
                        }
                    }));
                } else {
                    conn_idx += 1;
                    handles.push(tokio::spawn(async move {
                        let t0 = Instant::now();
                        match protocol::connect_and_handshake(
                            &host,
                            port,
                            &tok,
                            "compression=false&protocol_version=1",
                            15,
                        )
                        .await
                        {
                            Ok(ws) => {
                                let lat_ms = t0.elapsed().as_secs_f64() * 1000.0;
                                Ok((ws, lat_ms))
                            }
                            Err(_) => {
                                err_count.fetch_add(1, Ordering::Relaxed);
                                Err(())
                            }
                        }
                    }));
                }
            }

            let batch_results = futures_util::future::join_all(handles).await;
            for r in batch_results {
                if let Ok(Ok((ws, lat))) = r {
                    connections.push(ws);
                    all_latencies.push(lat);
                }
            }

            eprint!("\r    Progress: {}/{}...", connections.len(), n);

            if end < n {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

        let total_secs = total_start.elapsed().as_secs_f64();
        let err_count = errors.load(Ordering::Relaxed);

        eprintln!(
            "\r    Connected: {}/{} ({} failed)            ",
            connections.len(),
            n,
            err_count
        );
        println!("    Total time: {:.2}s", total_secs);

        // Build histogram
        let mut hist = LatencyHistogram::new();
        for &lat in &all_latencies {
            hist.record_ms(lat);
        }

        if !all_latencies.is_empty() {
            let rate = all_latencies.len() as f64 / total_secs;
            println!("    Accept rate: {:.0} conn/s", rate);
            hist.print_summary("    ");
        }

        results.push(TierResult {
            tier: n,
            connected: connections.len(),
            errors: err_count,
            duration_secs: total_secs,
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            latency: Some(LatencySummary::from_histogram(&hist)),
            extra: serde_json::json!({
                "accept_rate": if total_secs > 0.0 { all_latencies.len() as f64 / total_secs } else { 0.0 }
            }),
        });

        // Cleanup
        protocol::close_all(connections).await;
        println!();
    }

    results
}
