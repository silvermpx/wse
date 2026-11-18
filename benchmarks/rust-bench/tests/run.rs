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

/// A server that lets every connection in and then answers nothing: no
/// PONG, no message, ever.
async fn silent() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                let _ = ws.send(Message::text(r#"{"t":"server_ready"}"#)).await;
                while ws.next().await.is_some() {}
            });
        }
    });
    port
}

/// The bench's run of `test` against `port`, with `args`: whether it
/// exited 0, and what it said.
async fn run(test: &'static str, port: u16, args: &'static [&'static str]) -> (bool, String) {
    let out = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_wse-bench"))
            .args(["--test", test, "--port"])
            .arg(port.to_string())
            .args(args)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), said)
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
    assert!(
        !out.status.success(),
        "a failed check fails the run: {stderr}"
    );
    assert!(stderr.contains("[FAIL]"), "{stderr}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    let report: serde_json::Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("stdout is the JSON report alone ({e}): {stdout}"));
    assert_eq!(report[0]["test"], "BattlePresence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_test_that_cannot_connect_fails_the_run() {
    for test in [
        "battle-presence",
        "battle-recovery",
        "battle-encryption",
        "battle-queue-groups",
        "battle-topic-acl",
        "battle-drain",
        "battle-metrics",
    ] {
        let port = one_connection().await;
        let out = tokio::task::spawn_blocking(move || {
            Command::new(env!("CARGO_BIN_EXE_wse-bench"))
                .args(["--test", test, "--port"])
                .arg(port.to_string())
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains("[FAIL]"), "{test}: {stdout}");
        assert!(
            !out.status.success(),
            "{test} printed FAIL and exited 0: {stdout}"
        );
    }
}

/// Every measurement that gets fewer connections than its tier asks for
/// fails the run, whatever else it measured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_measurement_that_cannot_connect_fails_the_run() {
    let tests = [
        "battle-compression",
        "format-comparison",
        "size-matrix",
        "connection-storm",
        "ping-latency",
        "throughput",
        "sustained-hold",
        "connection-limit",
        "fanout-broadcast",
        "fanout-cluster",
        "fanout-cluster-tls",
        "battle-load",
    ];
    let runs = tests.map(|test| {
        tokio::spawn(async move {
            let port = one_connection().await;
            let args = &["--tiers", "2", "--duration", "1", "--max-connections", "2"];
            (test, run(test, port, args).await)
        })
    });
    for done in runs {
        let (test, (ok, said)) = done.await.unwrap();
        assert!(!ok, "{test} measured less than asked and exited 0: {said}");
    }
}

/// A server that takes the connections and answers nothing: a measurement
/// of answers or deliveries that got none fails the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_measurement_of_what_never_came_fails_the_run() {
    let runs = ["ping-latency", "fanout-broadcast", "battle-load"].map(|test| {
        tokio::spawn(async move {
            let port = silent().await;
            (
                test,
                run(test, port, &["--tiers", "2", "--duration", "1"]).await,
            )
        })
    });
    for done in runs {
        let (test, (ok, said)) = done.await.unwrap();
        assert!(!ok, "{test} got nothing and exited 0: {said}");
        assert!(said.contains("FAILED"), "{test} said no FAILED: {said}");
    }
}
