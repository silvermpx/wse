//! The control channel: a WebSocket of JSON requests, `{id, op, ...}`
//! answered `{id, ok, ...}`, one session for each driver. A driver says who
//! it is (`hello`), then runs each case it can: `begin` (the case's server,
//! its steps), `act` (a server step, in its place), `trace` (what its
//! client saw, in pieces), `end` (the verdict). `bye`, or the channel
//! closing, writes its report. A window of the bridge's Rust owner connects
//! at `/bridge` instead (see `bridge.rs`).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;

use crate::cases::{Case, Catalog};
use crate::oracle;
use crate::report::{Agent, Report, Verdict};
use crate::stand::{Stand, lock_log};

/// The case a session runs now.
struct Running {
    case: Case,
    stand: Stand,
    trace: Vec<Value>,
    started: Instant,
}

struct Session {
    catalog: Arc<Catalog>,
    out: PathBuf,
    /// The commit the server was started in.
    source: Arc<str>,
    /// The name the driver reached this machine by.
    host: String,
    agent: Option<Agent>,
    running: Option<Running>,
    verdicts: Vec<Verdict>,
}

/// The host of a request's `Host` (`10.0.2.2:5000` is `10.0.2.2`, `[::1]:5000`
/// is `[::1]`): the name the driver reached this machine by, which its case's
/// server goes by too (an Android emulator reaches this machine's loopback
/// as 10.0.2.2). Anything a URL could not hold is 127.0.0.1.
fn host_of(authority: &str) -> &str {
    let host = match authority.strip_prefix('[') {
        Some(rest) => rest
            .find(']')
            .map_or("", |end| &authority[..end + 2])
            .trim_end_matches(':'),
        None => authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host),
    };
    let fits = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '[' | ']');
    if host.is_empty() || !host.chars().all(fits) {
        "127.0.0.1"
    } else {
        host
    }
}

/// Serves drivers on `listener` until the process ends.
// The handshake's callback answers with tungstenite's own error response.
#[allow(clippy::result_large_err)]
pub async fn serve(listener: TcpListener, catalog: Arc<Catalog>, out: PathBuf, source: Arc<str>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let catalog = Arc::clone(&catalog);
        let out = out.clone();
        let source = Arc::clone(&source);
        tokio::spawn(async move {
            let mut path = String::new();
            let mut host = String::new();
            let accepted = tokio_tungstenite::accept_hdr_async(
                stream,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    request.uri().path().clone_into(&mut path);
                    let authority = request.headers().get("host").and_then(|h| h.to_str().ok());
                    host_of(authority.unwrap_or_default()).clone_into(&mut host);
                    Ok(response)
                },
            )
            .await;
            let served = match accepted {
                Ok(ws) if path == "/bridge" => crate::bridge::host(ws).await,
                Ok(ws) => session(ws, catalog, out, host, source).await,
                Err(e) => Err(e.to_string()),
            };
            if let Err(why) = served {
                eprintln!("wse-conformance: a driver's channel: {why}");
            }
        });
    }
}

async fn session(
    mut ws: tokio_tungstenite::WebSocketStream<TcpStream>,
    catalog: Arc<Catalog>,
    out: PathBuf,
    host: String,
    source: Arc<str>,
) -> Result<(), String> {
    let mut session = Session {
        catalog,
        out,
        source,
        host,
        agent: None,
        running: None,
        verdicts: Vec::new(),
    };
    while let Some(frame) = ws.next().await {
        let text = match frame {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => continue,
        };
        let request: Value = match serde_json::from_str(&text) {
            Ok(request) => request,
            Err(e) => json!({"op": "", "error": e.to_string()}),
        };
        let id = request["id"].clone();
        let answer = match session.handle(&request).await {
            Ok(mut answer) => {
                answer["ok"] = Value::Bool(true);
                answer
            }
            Err(why) => json!({"ok": false, "error": why}),
        };
        let mut answer = answer;
        answer["id"] = id;
        if ws.send(Message::text(answer.to_string())).await.is_err() {
            break;
        }
        if request["op"] == "bye" {
            break;
        }
    }
    session.finish().await;
    Ok(())
}

impl Session {
    async fn handle(&mut self, request: &Value) -> Result<Value, String> {
        match request["op"].as_str().unwrap_or_default() {
            "hello" => self.hello(request),
            "begin" => self.begin(request).await,
            "act" => {
                let running = self.running.as_mut().ok_or("no case runs")?;
                running.stand.act(&request["step"]).await
            }
            "trace" => {
                let running = self.running.as_mut().ok_or("no case runs")?;
                let events = request["events"].as_array().ok_or("events")?;
                running.trace.extend(events.iter().cloned());
                Ok(json!({}))
            }
            "end" => self.end().await,
            "bye" => Ok(json!({"report": self.write()?})),
            other => Err(format!("no such op: {other}")),
        }
    }

    /// Who the driver is; the cases it can run, and why not the others.
    fn hello(&mut self, request: &Value) -> Result<Value, String> {
        let agent: Agent = serde_json::from_value(request.clone()).map_err(|e| e.to_string())?;
        if agent.agent.is_empty() {
            return Err("an agent's name".to_owned());
        }
        let features: Vec<&str> = agent.features.iter().map(String::as_str).collect();
        let mut cases = Vec::new();
        for case in &self.catalog.cases {
            let missing: Vec<&str> = case
                .needs
                .iter()
                .map(String::as_str)
                .filter(|n| !features.contains(n))
                .collect();
            if missing.is_empty() {
                cases.push(json!({"case": case.case, "about": case.about}));
            } else {
                self.verdicts.push(Verdict {
                    case: case.case.clone(),
                    verdict: "not-applicable".to_owned(),
                    why: Some(format!("needs {}", missing.join(", "))),
                    failures: Vec::new(),
                    ms: 0,
                });
            }
        }
        self.agent = Some(agent);
        Ok(json!({"cases": cases}))
    }

    /// The case's server, and what the driver needs to run it.
    async fn begin(&mut self, request: &Value) -> Result<Value, String> {
        if self.agent.is_none() {
            return Err("hello first".to_owned());
        }
        self.end().await.ok();
        let name = request["case"].as_str().ok_or("a case")?;
        let case = self
            .catalog
            .cases
            .iter()
            .find(|c| c.case == name)
            .cloned()
            .ok_or_else(|| format!("no such case: {name}"))?;
        let stand = Stand::open(case.server.clone()).await?;
        // Every user a step names, and the default one.
        let mut users = vec!["ann"];
        for step in &case.steps {
            if let Some(user) = step["user"].as_str()
                && !users.contains(&user)
            {
                users.push(user);
            }
        }
        let mut answer = stand.about(&users, &self.host)?;
        answer["case"] = Value::String(case.case.clone());
        answer["steps"] = Value::Array(case.steps.clone());
        self.running = Some(Running {
            case,
            stand,
            trace: Vec::new(),
            started: Instant::now(),
        });
        Ok(answer)
    }

    /// The running case's verdict; its server goes.
    async fn end(&mut self) -> Result<Value, String> {
        let mut running = self.running.take().ok_or("no case runs")?;
        running.stand.stop().await;
        let failures = {
            let log = lock_log(&running.stand.log);
            oracle::judge(&running.case, &running.trace, &log)
        };
        let verdict = if failures.is_empty() { "pass" } else { "fail" };
        self.verdicts.retain(|v| v.case != running.case.case);
        self.verdicts.push(Verdict {
            case: running.case.case.clone(),
            verdict: verdict.to_owned(),
            why: None,
            failures: failures.clone(),
            ms: u64::try_from(running.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        });
        Ok(json!({"case": running.case.case, "verdict": verdict, "failures": failures}))
    }

    /// The report, written: where.
    fn write(&self) -> Result<String, String> {
        let agent = self.agent.clone().ok_or("hello first")?;
        let report = Report {
            agent,
            source: self.source.to_string(),
            server: env!("CARGO_PKG_VERSION").to_owned(),
            cases: self.verdicts.clone(),
        };
        report.write(&self.out).map_err(|e| e.to_string())?;
        Ok(self.out.display().to_string())
    }

    /// The channel closed: a case left running failed, and the report is
    /// written.
    async fn finish(&mut self) {
        if let Some(running) = &mut self.running {
            running
                .trace
                .push(json!({"ev": "error", "message": "the driver went before the case ended"}));
            let _ = self.end().await;
        }
        if self.agent.is_some()
            && let Err(why) = self.write()
        {
            eprintln!("wse-conformance: the report: {why}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    #[test]
    fn the_host_is_the_authority_without_its_port() {
        assert_eq!(host_of("10.0.2.2:5000"), "10.0.2.2");
        assert_eq!(host_of("localhost"), "localhost");
        assert_eq!(host_of("[::1]:5000"), "[::1]");
        assert_eq!(host_of("[::1]"), "[::1]");
        // Nothing a URL could not hold.
        assert_eq!(host_of("a b:1"), "127.0.0.1");
        assert_eq!(host_of("x/y:1"), "127.0.0.1");
        assert_eq!(host_of(""), "127.0.0.1");
    }

    /// A driver that reached the control channel by another name of this
    /// machine (an Android emulator's 10.0.2.2) reaches the case's server
    /// by it too.
    #[tokio::test]
    async fn the_case_server_is_named_as_the_driver_named_the_control() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let out = std::env::temp_dir().join(format!("wse-conformance-host-{}", std::process::id()));
        let served = tokio::spawn(serve(
            listener,
            Arc::new(crate::cases::catalog()),
            out.clone(),
            Arc::from("test"),
        ));
        let stream = TcpStream::connect(addr).await.unwrap();
        let request = format!("ws://10.0.2.2:{}/control", addr.port())
            .into_client_request()
            .unwrap();
        let (mut ws, _) = tokio_tungstenite::client_async(request, stream)
            .await
            .unwrap();
        let mut call = async |body: Value| {
            ws.send(Message::text(body.to_string())).await.unwrap();
            loop {
                if let Some(Ok(Message::Text(text))) = ws.next().await {
                    return serde_json::from_str::<Value>(&text).unwrap();
                }
            }
        };
        let hello = json!({"id": 1, "op": "hello", "agent": "host-check", "features": []});
        assert_eq!(call(hello).await["ok"], true);
        let begun = call(json!({"id": 2, "op": "begin", "case": "connect.plain"})).await;
        let url = begun["url"].as_str().unwrap_or_default().to_owned();
        assert!(url.starts_with("ws://10.0.2.2:"), "{begun}");
        call(json!({"id": 3, "op": "bye"})).await;
        served.abort();
        std::fs::remove_dir_all(&out).ok();
    }
}
