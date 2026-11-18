//! The bench as whatever runs it sees it: a check that failed fails the run,
//! however its test ends, and `--output json` is the report alone on stdout.

use std::process::Command;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// A server that lets the first connection in (the bench's connectivity
/// check) and closes every later one before its server_ready.
async fn one_connection() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut first = true;
        while let Ok((stream, _)) = listener.accept().await {
            let lets_in = std::mem::replace(&mut first, false);
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                if lets_in {
                    let _ = ws.send(Message::text(r#"{"t":"server_ready"}"#)).await;
                    while ws.next().await.is_some() {}
                } else {
                    let _ = ws.close(None).await;
                }
            });
        }
    });
    port
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_test_that_stops_early_fails_the_run_and_json_is_alone_on_stdout() {
    let port = one_connection().await;
    let out = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_wse-bench"))
            .args(["--test", "battle-presence", "--output", "json", "--port"])
            .arg(port.to_string())
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "a failed check fails the run: {stderr}");
    assert!(stderr.contains("[FAIL]"), "{stderr}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    let report: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout is the JSON report alone ({e}): {stdout}"));
    assert_eq!(report[0]["test"], "BattlePresence");
}
