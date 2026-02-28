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
