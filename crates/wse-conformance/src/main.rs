//! The server every WSE client is checked against.
//!
//!     wse-conformance serve [--control 127.0.0.1:0] [--out target/conformance]
//!
//! prints one line, `{"ready":true,"control":"ws://127.0.0.1:P/control"}`,
//! and serves drivers until it is stopped; each driver's report goes to
//! `--out` as `<agent>.json` and `<agent>.junit.xml`.
//!
//!     wse-conformance check --manifest tests/conformance/agents.json --reports target/conformance
//!
//! fails (exit 1) unless every agent the manifest names passed every case
//! its features reach. `wse-conformance cases` prints the built-in catalog.
//!
//! Beside the control channel, `/bridge` serves a window of the bridge's
//! Rust owner: the TypeScript driver runs the cases through it.

mod bridge;
mod cases;
mod control;
mod oracle;
mod report;
mod stand;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

fn usage() -> ExitCode {
    eprintln!(
        "usage: wse-conformance serve [--control ADDR] [--out DIR]\n       \
         wse-conformance check --manifest FILE --reports DIR\n       \
         wse-conformance cases"
    );
    ExitCode::from(2)
}

/// The value after `name` among `args`.
fn option(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|at| args.get(at + 1))
        .cloned()
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let catalog = Arc::new(cases::catalog());
    match args.first().map(String::as_str) {
        Some("serve") => {
            let control = option(&args, "--control").unwrap_or_else(|| "127.0.0.1:0".to_owned());
            let out = PathBuf::from(
                option(&args, "--out").unwrap_or_else(|| "target/conformance".to_owned()),
            );
            let listener = match tokio::net::TcpListener::bind(&control).await {
                Ok(listener) => listener,
                Err(e) => {
                    eprintln!("wse-conformance: {control}: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let Ok(addr) = listener.local_addr() else {
                return ExitCode::FAILURE;
            };
            println!(
                "{}",
                serde_json::json!({"ready": true, "control": format!("ws://{addr}/control")})
            );
            tokio::select! {
                () = control::serve(listener, catalog, out) => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            ExitCode::SUCCESS
        }
        Some("check") => {
            let (Some(manifest), Some(reports)) =
                (option(&args, "--manifest"), option(&args, "--reports"))
            else {
                return usage();
            };
            match report::check(manifest.as_ref(), reports.as_ref(), &catalog) {
                Ok(problems) if problems.is_empty() => {
                    println!("conformance: every agent passed what it must");
                    ExitCode::SUCCESS
                }
                Ok(problems) => {
                    for problem in problems {
                        println!("conformance: {problem}");
                    }
                    ExitCode::FAILURE
                }
                Err(why) => {
                    eprintln!("conformance: {why}");
                    ExitCode::FAILURE
                }
            }
        }
        Some("cases") => {
            println!("{}", include_str!("../../../tests/conformance/cases.json"));
            ExitCode::SUCCESS
        }
        _ => usage(),
    }
}
