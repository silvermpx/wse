use clap::Parser;

pub const DEFAULT_SECRET: &str = "bench-secret-key-for-testing-only";
pub const DEFAULT_HOST: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 5006;

pub const DEFAULT_TIERS: &[usize] = &[
    100, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 30_000, 50_000,
];
pub const EXTENDED_TIERS: &[usize] = &[
    100, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 30_000, 50_000, 75_000, 100_000,
];
pub const MATRIX_SIZES: &[usize] = &[64, 256, 1_024, 4_096, 16_384, 65_536];

#[derive(Parser, Debug, Clone)]
#[command(name = "wse-bench", about = "WSE server benchmark suite")]
pub struct Cli {
    /// Server host
    #[arg(long, default_value = DEFAULT_HOST)]
    pub host: String,

    /// Server port
    #[arg(long, default_value_t = DEFAULT_PORT)]
    pub port: u16,

    /// JWT secret (must match bench_server.py)
    #[arg(long, default_value = DEFAULT_SECRET)]
    pub secret: String,

    /// Run specific test (omit for full suite)
    #[arg(long, value_enum)]
    pub test: Option<TestName>,

    /// Custom connection tiers (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub tiers: Option<Vec<usize>>,

    /// Max connections for connection-limit test
    #[arg(long, default_value_t = 150_000)]
    pub max_connections: usize,

    /// Duration per tier in seconds
    #[arg(long, default_value_t = 10)]
    pub duration: u64,

    /// Output format
    #[arg(long, default_value = "text")]
    pub output: OutputFormat,

    /// Connection batch size (how many to open concurrently)
    #[arg(long, default_value_t = 500)]
    pub batch_size: usize,

    /// Second server port (for multi-instance fan-out test)
    #[arg(long)]
    pub port2: Option<u16>,

    /// Third server port (for 3-node discovery test)
    #[arg(long)]
    pub port3: Option<u16>,

    /// The first server's (--port) metrics HTTP port (default: its port + 1000)
    #[arg(long)]
    pub metrics_port: Option<u16>,

    /// The second server's (--port2) metrics HTTP port (default: its port + 1000)
    #[arg(long)]
    pub metrics_port2: Option<u16>,
}

impl Cli {
    /// The second server's port: --port2, or the port after --port.
    pub fn port2(&self) -> u16 {
        self.port2.unwrap_or_else(|| self.port_after(1, "--port2"))
    }

    /// The third server's port: --port3, or two after --port.
    pub fn port3(&self) -> u16 {
        self.port3.unwrap_or_else(|| self.port_after(2, "--port3"))
    }

    /// `n` ports after --port; with no such port, the run ends as on a bad
    /// argument, naming the one to give.
    fn port_after(&self, n: u16, flag: &str) -> u16 {
        self.port.checked_add(n).unwrap_or_else(|| {
            eprintln!(
                "error: --port {} has no port {n} after it: give {flag}",
                self.port
            );
            std::process::exit(2)
        })
    }

    /// The metrics port of the server at `server_port`: the one given for
    /// it (--metrics-port for the first server, --metrics-port2 for the
    /// second), or its port + 1000; a port with no room for that above it
    /// has none.
    pub fn metrics_port_for(&self, server_port: u16) -> Option<u16> {
        let given = if server_port == self.port {
            self.metrics_port
        } else if Some(server_port) == self.port2.or(self.port.checked_add(1)) {
            self.metrics_port2
        } else {
            None
        };
        given.or(server_port.checked_add(1000))
    }
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestName {
    ConnectionStorm,
    PingLatency,
    Throughput,
    SizeMatrix,
    FormatComparison,
    SustainedHold,
    ConnectionLimit,
    FanoutBroadcast,
    FanoutCluster,
    FanoutClusterTls,
    BattleStandalone,
    BattleCluster,
    BattleLoad,
    BattleCaps,
    BattleTls,
    BattleDiscovery,
    BattleCompression,
    BattlePresence,
    BattleRecovery,
    BattleEncryption,
    BattleQueueGroups,
    BattleTopicAcl,
    BattleDrain,
    BattleMetrics,
    BattleTopology,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
    Markdown,
}

impl Cli {
    /// Get connection tiers for a test, with CLI override.
    pub fn tiers_for(&self, test: TestName) -> Vec<usize> {
        if let Some(ref custom) = self.tiers {
            return custom.clone();
        }
        match test {
            TestName::ConnectionStorm | TestName::ConnectionLimit => EXTENDED_TIERS.to_vec(),
            TestName::SustainedHold => vec![1_000, 5_000, 10_000, 30_000, 50_000],
            TestName::FormatComparison => vec![100, 1_000, 5_000, 10_000],
            TestName::FanoutBroadcast => {
                vec![
                    10, 100, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000,
                ]
            }
            TestName::FanoutCluster | TestName::FanoutClusterTls => {
                vec![10, 100, 500, 1_000, 2_000, 5_000, 10_000, 20_000]
            }
            _ => DEFAULT_TIERS.to_vec(),
        }
    }

    #[allow(dead_code)]
    pub fn ws_uri(&self) -> String {
        format!(
            "ws://{}:{}/wse?compression=false&protocol_version=1",
            self.host, self.port
        )
    }

    #[allow(dead_code)]
    pub fn ws_uri_msgpack(&self) -> String {
        format!(
            "ws://{}:{}/wse?format=msgpack&protocol_version=1",
            self.host, self.port
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_port_with_no_room_above_it_has_no_default_metrics_port() {
        let cli = Cli::parse_from(["wse-bench"]);
        assert_eq!(cli.metrics_port_for(5006), Some(6006));
        assert_eq!(cli.metrics_port_for(64_535), Some(65_535));
        assert_eq!(cli.metrics_port_for(64_536), None);
        let given = Cli::parse_from(["wse-bench", "--metrics-port", "9100"]);
        assert_eq!(given.metrics_port_for(given.port), Some(9100));
    }

    #[test]
    fn each_server_has_a_metrics_port_of_its_own() {
        let ports = ["wse-bench", "--port", "5006", "--port2", "5007"];
        let cli = Cli::parse_from(ports);
        assert_eq!(cli.metrics_port_for(5006), Some(6006));
        assert_eq!(cli.metrics_port_for(5007), Some(6007));
        let given = Cli::parse_from(ports.into_iter().chain([
            "--metrics-port",
            "9100",
            "--metrics-port2",
            "9200",
        ]));
        assert_eq!(given.metrics_port_for(5006), Some(9100));
        assert_eq!(given.metrics_port_for(5007), Some(9200));
        // The first server's given, the second's is its own default.
        let first = Cli::parse_from(ports.into_iter().chain(["--metrics-port", "9100"]));
        assert_eq!(first.metrics_port_for(5007), Some(6007));
    }

    #[test]
    fn the_other_servers_default_to_the_ports_after() {
        let cli = Cli::parse_from(["wse-bench", "--port", "65533"]);
        assert_eq!((cli.port2(), cli.port3()), (65_534, 65_535));
        let given = Cli::parse_from([
            "wse-bench",
            "--port",
            "65535",
            "--port2",
            "7",
            "--port3",
            "8",
        ]);
        assert_eq!((given.port2(), given.port3()), (7, 8));
    }
}
