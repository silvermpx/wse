//! The control channel: a WebSocket of JSON requests, `{id, op, ...}`
//! answered `{id, ok, ...}`, one session for each driver. A driver says who
//! it is (`hello`), then runs each case it can: `begin` (the case's server,
//! its steps), `act` (a server step, in its place), `trace` (what its
//! client saw, in pieces), `end` (the verdict). `bye`, or the channel
//! closing, writes its report.

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
    agent: Option<Agent>,
    running: Option<Running>,
    verdicts: Vec<Verdict>,
}

/// Serves drivers on `listener` until the process ends.
pub async fn serve(listener: TcpListener, catalog: Arc<Catalog>, out: PathBuf) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let catalog = Arc::clone(&catalog);
        let out = out.clone();
        tokio::spawn(async move {
            if let Err(why) = session(stream, catalog, out).await {
                eprintln!("wse-conformance: a driver's channel: {why}");
            }
        });
    }
}

async fn session(stream: TcpStream, catalog: Arc<Catalog>, out: PathBuf) -> Result<(), String> {
    let mut ws = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(|e| e.to_string())?;
    let mut session = Session {
        catalog,
        out,
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
        let mut answer = stand.about(&users)?;
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
