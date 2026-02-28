mod config;
mod jwt;
mod preflight;
mod protocol;
mod report;
mod stats;
mod tests;

use clap::Parser;
use config::{Cli, TestName};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    println!("WSE Benchmark Suite (Rust)");
    println!("=========================");
    println!("  Server:  {}:{}", cli.host, cli.port);
    println!("  Secret:  {}...", &cli.secret[..8.min(cli.secret.len())]);
    println!("  Duration: {}s per tier", cli.duration);

    if let Some(ref tiers) = cli.tiers {
        println!("  Custom tiers: {:?}", tiers);
    }

    // Preflight checks
    preflight::check_os_limits(cli.max_connections);

    // Verify connectivity with a single connection
    print!("  Connectivity check: ");
    let token = jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
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
            println!("OK (connected, server_ready received)");
        }
        Err(e) => {
            eprintln!("FAILED: {}", e);
            eprintln!(
                "\n  Is bench_server.py running on {}:{}?",
                cli.host, cli.port
            );
            eprintln!(
                "  Start with: python benchmarks/bench_server.py --port {}",
                cli.port
            );
            std::process::exit(1);
        }
    }

    // Run tests
    match cli.test {
        Some(test) => run_single_test(&cli, test).await,
        None => run_full_suite(&cli).await,
    }

    println!("\nDone.");
}

async fn run_single_test(cli: &Cli, test: TestName) {
    match test {
        TestName::ConnectionStorm => {
            tests::connection_storm::run(cli).await;
        }
        TestName::PingLatency => {
            tests::ping_latency::run(cli).await;
        }
        TestName::Throughput => {
            tests::throughput::run(cli).await;
        }
        TestName::SizeMatrix => {
            tests::size_matrix::run(cli).await;
        }
        TestName::FormatComparison => {
            tests::format_comparison::run(cli).await;
        }
        TestName::SustainedHold => {
            tests::sustained_hold::run(cli).await;
        }
        TestName::ConnectionLimit => {
            tests::connection_limit::run(cli).await;
        }
        TestName::FanoutBroadcast => {
            tests::fanout_broadcast::run(cli).await;
        }
        TestName::FanoutCluster => {
            tests::fanout_cluster::run(cli).await;
        }
        TestName::FanoutClusterTls => {
            tests::fanout_cluster_tls::run(cli).await;
        }
        TestName::BattleStandalone => {
            tests::battle_standalone::run(cli).await;
        }
        TestName::BattleCluster => {
            tests::battle_cluster::run(cli).await;
        }
        TestName::BattleLoad => {
            tests::battle_load::run(cli).await;
        }
        TestName::BattleCaps => {
            tests::battle_caps::run(cli).await;
        }
        TestName::BattleTls => {
            tests::battle_tls::run(cli).await;
        }
        TestName::BattleDiscovery => {
            tests::battle_discovery::run(cli).await;
        }
        TestName::BattleCompression => {
            tests::battle_compression::run(cli).await;
        }
        TestName::BattlePresence => {
            tests::battle_presence::run(cli).await;
        }
        TestName::BattleRecovery => {
            tests::battle_recovery::run(cli).await;
        }
    }
}

async fn run_full_suite(cli: &Cli) {
    println!("\n  Running full benchmark suite...");

    tests::connection_storm::run(cli).await;
    pause_between_tests().await;

    tests::ping_latency::run(cli).await;
    pause_between_tests().await;

    tests::throughput::run(cli).await;
    pause_between_tests().await;

    tests::size_matrix::run(cli).await;
    pause_between_tests().await;

    tests::format_comparison::run(cli).await;
    pause_between_tests().await;

    tests::sustained_hold::run(cli).await;
    pause_between_tests().await;

    tests::connection_limit::run(cli).await;
    pause_between_tests().await;

    tests::fanout_broadcast::run(cli).await;
    pause_between_tests().await;

    tests::fanout_cluster::run(cli).await;
}

async fn pause_between_tests() {
    println!("\n  [Cooling down 3s before next test...]");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
}
