//! The judge: a case's rules, held against what the client recorded (its
//! trace) and what the case's server published and took (its log). A
//! driver asserts nothing; this decides.
//!
//! The trace, in the order the client saw things:
//! - `{"ev":"ready"}`: the server took a connection;
//! - `{"ev":"message","type","topic"?,"n"?,"payload"}`: a message reached
//!   the application (`n`, the number of a published one, `p.n`);
//! - `{"ev":"break","topic","kind"}`: the client said a topic broke off (a
//!   gap, its continuity broken);
//! - `{"ev":"resync","topics"}`, `{"ev":"resynced","topics"}`: the client
//!   asked the application to resync, and the application said it did;
//! - `{"ev":"sent","id","type","live","ok","error"?}`: the application sent;
//! - `{"ev":"delivered","ids"}`, `{"ev":"not_delivered","reason","ids"}`;
//! - `{"ev":"step","server"}`: the driver asked for a server step;
//! - `{"ev":"timeout","step","waiting"}`, `{"ev":"error","message"}`: the
//!   driver could not go on.
//!
//! Each event may carry `t`, the milliseconds since the case began.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::cases::{Case, Rule};
use crate::stand::Log;

/// What the client recorded.
struct Trace<'a>(&'a [Value]);

impl Trace<'_> {
    fn of<'b>(&'b self, ev: &'b str) -> impl Iterator<Item = (usize, &'b Value)> + 'b {
        self.0
            .iter()
            .enumerate()
            .filter(move |(_, e)| e["ev"] == ev)
    }

    /// Where each numbered message of `topic` came, and its number.
    fn numbers(&self, topic: &str) -> Vec<(usize, u64)> {
        self.of("message")
            .filter(|(_, e)| e["topic"] == topic)
            .filter_map(|(at, e)| e["n"].as_u64().map(|n| (at, n)))
            .collect()
    }

    /// Where the client said `topic` broke off, or asked for its resync.
    fn breaks(&self, topic: &str) -> Vec<usize> {
        self.0
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                (e["ev"] == "break" && e["topic"] == topic)
                    || (e["ev"] == "resync" && names(&e["topics"], topic))
            })
            .map(|(at, _)| at)
            .collect()
    }

    /// The sends the application made of `kind`, that went into the client
    /// (not live): their ids, in order.
    fn sent(&self, kind: Option<&str>) -> Vec<String> {
        self.of("sent")
            .filter(|(_, e)| e["ok"] == true && e["live"] != true)
            .filter(|(_, e)| kind.is_none_or(|k| e["type"] == k))
            .filter_map(|(_, e)| e["id"].as_str().map(str::to_owned))
            .collect()
    }

    fn ids(&self, ev: &str) -> HashSet<String> {
        self.of(ev)
            .flat_map(|(_, e)| e["ids"].as_array().cloned().unwrap_or_default())
            .filter_map(|id| id.as_str().map(str::to_owned))
            .collect()
    }
}

fn names(list: &Value, topic: &str) -> bool {
    list.as_array()
        .is_some_and(|l| l.iter().any(|t| t == topic))
}

/// The case's failures; none: it passed.
pub fn judge(case: &Case, events: &[Value], log: &Log) -> Vec<String> {
    let trace = Trace(events);
    let mut failures = Vec::new();
    for (_, event) in trace.of("timeout") {
        failures.push(format!(
            "step {} timed out waiting for {}",
            event["step"], event["waiting"]
        ));
    }
    for (_, event) in trace.of("error") {
        failures.push(format!("the driver could not go on: {}", event["message"]));
    }
    // The application hears outcomes of its own messages alone.
    let sent: HashSet<String> = trace.sent(None).into_iter().collect();
    for ev in ["delivered", "not_delivered"] {
        let foreign: Vec<String> = trace
            .ids(ev)
            .into_iter()
            .filter(|id| !sent.contains(id))
            .collect();
        if !foreign.is_empty() {
            failures.push(format!(
                "{ev} names what the application never sent: {foreign:?}"
            ));
        }
    }
    for rule in &case.rules {
        if let Err(why) = hold(rule, &trace, log) {
            failures.push(format!("{}: {why}", rule.rule));
        }
    }
    failures
}

fn hold(rule: &Rule, trace: &Trace<'_>, log: &Log) -> Result<(), String> {
    let topic = || {
        rule.topic
            .as_deref()
            .ok_or_else(|| "the rule names no topic".to_owned())
    };
    let kind = rule.kind.as_deref();
    match rule.rule.as_str() {
        "connections" => {
            let count = rule.count.unwrap_or(1) as u64;
            if log.connections == count {
                Ok(())
            } else {
                Err(format!("{} connection(s), not {count}", log.connections))
            }
        }
        "echoed" => {
            let found = trace.of("message").any(|(_, e)| {
                e["type"] == "echoed" && Some(&e["payload"]) == rule.payload.as_ref()
            });
            if found {
                Ok(())
            } else {
                Err(format!(
                    "no echoed message with {}",
                    rule.payload.clone().unwrap_or_default()
                ))
            }
        }
        "order" => {
            let topic = topic()?;
            let breaks = trace.breaks(topic);
            let mut last: Option<u64> = None;
            let mut segment = 0;
            for (at, n) in trace.numbers(topic) {
                let now = breaks.iter().filter(|b| **b < at).count();
                if now != segment {
                    segment = now;
                    last = None;
                }
                if last.is_some_and(|l| n <= l) {
                    return Err(format!(
                        "{n} came after {} with no break between",
                        last.unwrap_or_default()
                    ));
                }
                last = Some(n);
            }
            Ok(())
        }
        "no_duplicates" => {
            let topic = topic()?;
            let breaks = trace.breaks(topic);
            let mut seen: HashMap<u64, usize> = HashMap::new();
            for (at, n) in trace.numbers(topic) {
                if let Some(before) = seen.insert(n, at)
                    && !breaks.iter().any(|b| before < *b && *b < at)
                {
                    return Err(format!("{n} came twice with no break between"));
                }
            }
            Ok(())
        }
        "all_published_delivered" => {
            let topic = topic()?;
            let got: HashSet<u64> = trace.numbers(topic).into_iter().map(|(_, n)| n).collect();
            let missing: Vec<u64> = published(log, topic).filter(|n| !got.contains(n)).collect();
            if missing.is_empty() {
                Ok(())
            } else {
                Err(format!("never came: {missing:?}"))
            }
        }
        "no_silent_loss" => {
            let topic = topic()?;
            let numbers = trace.numbers(topic);
            let got: HashSet<u64> = numbers.iter().map(|(_, n)| *n).collect();
            let breaks = trace.breaks(topic);
            let silent: Vec<u64> = published(log, topic)
                .filter(|n| !got.contains(n))
                .filter(|n| {
                    // A break said after the last message before it.
                    let before = numbers
                        .iter()
                        .filter(|(_, m)| m < n)
                        .map(|(at, _)| *at)
                        .max();
                    !breaks.iter().any(|b| before.is_none_or(|at| *b > at))
                })
                .collect();
            if silent.is_empty() {
                Ok(())
            } else {
                Err(format!("lost with no break said: {silent:?}"))
            }
        }
        "break_said" => {
            if trace.breaks(topic()?).is_empty() {
                Err("the client never said the topic broke off".to_owned())
            } else {
                Ok(())
            }
        }
        "no_break" => {
            let breaks = trace.breaks(topic()?);
            if breaks.is_empty() {
                Ok(())
            } else {
                Err(format!("{} break(s) said where none was", breaks.len()))
            }
        }
        "taken_once" => {
            let sent = trace.sent(kind);
            if sent.is_empty() {
                return Err("nothing was sent".to_owned());
            }
            for id in &sent {
                let taken: Vec<_> = log
                    .taken
                    .iter()
                    .filter(|t| t.id.as_deref() == Some(id.as_str()))
                    .collect();
                // A repeat the server could not tell is marked so.
                let firm = taken.iter().filter(|t| !t.possible_repeat).count();
                if taken.is_empty() {
                    return Err(format!("{id} was never taken"));
                }
                if firm > 1 {
                    return Err(format!("{id} was taken {firm} times"));
                }
            }
            Ok(())
        }
        "taken_in_order" => {
            let sent = trace.sent(kind);
            let wanted: HashSet<&str> = sent.iter().map(String::as_str).collect();
            let mut first = Vec::new();
            let mut seen = HashSet::new();
            for taken in &log.taken {
                if let Some(id) = taken.id.as_deref()
                    && wanted.contains(id)
                    && seen.insert(id)
                {
                    first.push(id);
                }
            }
            let order: Vec<&str> = sent
                .iter()
                .map(String::as_str)
                .filter(|id| seen.contains(id))
                .collect();
            if first == order {
                Ok(())
            } else {
                Err(format!("taken as {first:?}, sent as {order:?}"))
            }
        }
        "delivered_all" => {
            let delivered = trace.ids("delivered");
            let not = trace.ids("not_delivered");
            for id in trace.sent(kind) {
                if not.contains(&id) {
                    return Err(format!("{id} was said not delivered"));
                }
                if !delivered.contains(&id) {
                    return Err(format!("{id} was never said delivered"));
                }
            }
            Ok(())
        }
        "never_taken" => {
            let kind = kind.ok_or_else(|| "the rule names no type".to_owned())?;
            let taken = log.taken.iter().filter(|t| t.kind == kind).count();
            if taken == 0 {
                Ok(())
            } else {
                Err(format!("the server took {taken} message(s) of type {kind}"))
            }
        }
        "signed_taken" => {
            let count = rule.count.unwrap_or(1);
            let taken = log.taken.iter().filter(|t| t.signed).count();
            if taken == count {
                Ok(())
            } else {
                Err(format!("{taken} signed message(s) taken, not {count}"))
            }
        }
        "held_until_resynced" => {
            let topic = topic()?;
            let asked = trace
                .of("resync")
                .find(|(_, e)| names(&e["topics"], topic))
                .map(|(at, _)| at)
                .ok_or("the client never asked for a resync")?;
            let said = trace
                .of("resynced")
                .find(|(at, e)| *at > asked && names(&e["topics"], topic))
                .map(|(at, _)| at)
                .ok_or("the application never said it resynced")?;
            let early: Vec<u64> = trace
                .numbers(topic)
                .into_iter()
                .filter(|(at, _)| asked < *at && *at < said)
                .map(|(_, n)| n)
                .collect();
            if early.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "{early:?} reached the application before it resynced"
                ))
            }
        }
        "back_within" => {
            // After each cut or restart of the server, the client is back.
            let ms = rule.count.unwrap_or(3000) as u64;
            for (at, step) in trace.of("step") {
                if step["server"] != "kick" && step["server"] != "restart" {
                    continue;
                }
                let since = step["t"].as_u64().ok_or("the trace has no times")?;
                let back = trace
                    .of("ready")
                    .find(|(after, _)| *after > at)
                    .and_then(|(_, ready)| ready["t"].as_u64())
                    .ok_or_else(|| format!("never back after the {}", step["server"]))?;
                if back.saturating_sub(since) > ms {
                    return Err(format!(
                        "back {} ms after the {}, not within {ms}",
                        back.saturating_sub(since),
                        step["server"]
                    ));
                }
            }
            Ok(())
        }
        other => Err(format!("no such rule: {other}")),
    }
}

fn published<'a>(log: &'a Log, topic: &'a str) -> impl Iterator<Item = u64> + 'a {
    log.published
        .iter()
        .filter(move |p| p.topic == topic)
        .map(|p| p.n)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::stand::{Published, Taken};

    fn case(rules: Value) -> Case {
        serde_json::from_value(json!({"case": "c", "steps": [], "rules": rules})).unwrap()
    }

    fn published(topic: &str, numbers: &[u64]) -> Log {
        Log {
            published: numbers
                .iter()
                .map(|n| Published {
                    topic: topic.to_owned(),
                    n: *n,
                })
                .collect(),
            ..Log::default()
        }
    }

    fn message(n: u64) -> Value {
        json!({"ev": "message", "type": "tick", "topic": "t", "n": n})
    }

    fn gap() -> Value {
        json!({"ev": "break", "topic": "t", "kind": "gap"})
    }

    fn taken(id: &str, conn: u64, possible_repeat: bool) -> Taken {
        Taken {
            conn,
            user: None,
            kind: "order".to_owned(),
            id: Some(id.to_owned()),
            s: None,
            possible_repeat,
            signed: false,
        }
    }

    fn sent(id: &str) -> Value {
        json!({"ev": "sent", "id": id, "type": "order", "live": false, "ok": true})
    }

    fn judged(rules: Value, trace: &[Value], log: &Log) -> Vec<String> {
        judge(&case(rules), trace, log)
    }

    #[test]
    fn order_holds_within_a_stream_and_starts_over_after_a_break() {
        let rules = json!([{"rule": "order", "topic": "t"}]);
        let log = Log::default();
        assert!(judged(rules.clone(), &[message(1), message(2), message(3)], &log).is_empty());
        assert_eq!(
            judged(rules.clone(), &[message(1), message(3), message(2)], &log).len(),
            1
        );
        assert!(judged(rules, &[message(4), message(5), gap(), message(1)], &log).is_empty());
    }

    #[test]
    fn a_repeat_needs_a_break_between() {
        let rules = json!([{"rule": "no_duplicates", "topic": "t"}]);
        let log = Log::default();
        assert_eq!(
            judged(rules.clone(), &[message(1), message(1)], &log).len(),
            1
        );
        assert!(judged(rules, &[message(1), gap(), message(1)], &log).is_empty());
    }

    #[test]
    fn what_was_published_must_come() {
        let rules = json!([{"rule": "all_published_delivered", "topic": "t"}]);
        let log = published("t", &[1, 2, 3]);
        assert!(judged(rules.clone(), &[message(1), message(2), message(3)], &log).is_empty());
        let failures = judged(rules, &[message(1), message(3)], &log);
        assert_eq!(
            failures,
            vec!["all_published_delivered: never came: [2]".to_owned()]
        );
    }

    #[test]
    fn a_loss_is_said_after_the_last_message_before_it() {
        let rules = json!([{"rule": "no_silent_loss", "topic": "t"}]);
        let log = published("t", &[1, 2, 3, 4]);
        // 2 and 3 are gone, and the break is said after 1.
        assert!(judged(rules.clone(), &[message(1), gap(), message(4)], &log).is_empty());
        // Said before 1 came: it says nothing of what went after.
        assert_eq!(
            judged(rules.clone(), &[gap(), message(1), message(4)], &log).len(),
            1
        );
        // Never said.
        assert_eq!(judged(rules, &[message(1), message(4)], &log).len(), 1);
    }

    #[test]
    fn a_break_is_said_or_not() {
        let log = Log::default();
        assert_eq!(
            judged(
                json!([{"rule": "break_said", "topic": "t"}]),
                &[message(1)],
                &log
            )
            .len(),
            1
        );
        assert!(
            judged(
                json!([{"rule": "break_said", "topic": "t"}]),
                &[gap()],
                &log
            )
            .is_empty()
        );
        assert_eq!(
            judged(json!([{"rule": "no_break", "topic": "t"}]), &[gap()], &log).len(),
            1
        );
        let resync = json!({"ev": "resync", "topics": ["t"]});
        assert!(
            judged(
                json!([{"rule": "break_said", "topic": "t"}]),
                &[resync],
                &log
            )
            .is_empty()
        );
    }

    #[test]
    fn each_message_is_taken_once_unless_marked_a_possible_repeat() {
        let rules = json!([{"rule": "taken_once", "type": "order"}]);
        let trace = [sent("a"), sent("b")];
        let mut log = Log {
            taken: vec![taken("a", 1, false), taken("b", 1, false)],
            ..Log::default()
        };
        assert!(judged(rules.clone(), &trace, &log).is_empty());
        log.taken.push(taken("a", 2, true));
        assert!(judged(rules.clone(), &trace, &log).is_empty());
        log.taken.push(taken("b", 2, false));
        assert_eq!(
            judged(rules.clone(), &trace, &log),
            vec!["taken_once: b was taken 2 times".to_owned()]
        );
        log.taken.clear();
        assert_eq!(
            judged(rules, &trace, &log),
            vec!["taken_once: a was never taken".to_owned()]
        );
    }

    #[test]
    fn messages_are_taken_in_the_order_sent() {
        let rules = json!([{"rule": "taken_in_order", "type": "order"}]);
        let trace = [sent("a"), sent("b")];
        let mut log = Log {
            taken: vec![
                taken("a", 1, false),
                taken("b", 1, false),
                taken("a", 2, true),
            ],
            ..Log::default()
        };
        assert!(judged(rules.clone(), &trace, &log).is_empty());
        log.taken = vec![taken("b", 1, false), taken("a", 1, false)];
        assert_eq!(judged(rules, &trace, &log).len(), 1);
    }

    #[test]
    fn each_message_sent_is_said_delivered() {
        let rules = json!([{"rule": "delivered_all", "type": "order"}]);
        let log = Log::default();
        let delivered = json!({"ev": "delivered", "ids": ["a"]});
        assert!(judged(rules.clone(), &[sent("a"), delivered.clone()], &log).is_empty());
        assert_eq!(
            judged(rules.clone(), &[sent("a"), sent("b"), delivered], &log).len(),
            1
        );
        let not = json!({"ev": "not_delivered", "reason": "busy", "ids": ["a"]});
        assert_eq!(judged(rules, &[sent("a"), not], &log).len(), 1);
    }

    #[test]
    fn what_is_held_does_not_reach_the_application_before_its_resync() {
        let rules = json!([{"rule": "held_until_resynced", "topic": "t"}]);
        let log = Log::default();
        let resync = json!({"ev": "resync", "topics": ["t"]});
        let resynced = json!({"ev": "resynced", "topics": ["t"]});
        assert!(
            judged(
                rules.clone(),
                &[resync.clone(), resynced.clone(), message(5)],
                &log
            )
            .is_empty()
        );
        assert_eq!(
            judged(rules.clone(), &[resync.clone(), message(5), resynced], &log).len(),
            1
        );
        assert_eq!(judged(rules, &[resync], &log).len(), 1);
    }

    #[test]
    fn a_timeout_or_a_driver_that_could_not_go_on_fails() {
        let log = Log::default();
        let timeout = json!({"ev": "timeout", "step": 3, "waiting": {"for": "ready"}});
        assert_eq!(judged(json!([]), &[timeout], &log).len(), 1);
        let error = json!({"ev": "error", "message": "no"});
        assert_eq!(judged(json!([]), &[error], &log).len(), 1);
    }

    #[test]
    fn outcomes_name_the_applications_messages_alone() {
        let log = Log::default();
        let delivered = json!({"ev": "delivered", "ids": ["a", "the-client's-own"]});
        let failures = judged(json!([]), &[sent("a"), delivered], &log);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("the-client's-own"));
    }

    #[test]
    fn the_client_is_back_soon_after_a_cut() {
        let rules = json!([{"rule": "back_within", "count": 3000}]);
        let log = Log::default();
        let kick = json!({"ev": "step", "server": "kick", "t": 100});
        let soon = json!({"ev": "ready", "t": 400});
        let late = json!({"ev": "ready", "t": 10_100});
        assert!(judged(rules.clone(), &[kick.clone(), soon], &log).is_empty());
        assert_eq!(judged(rules.clone(), &[kick.clone(), late], &log).len(), 1);
        assert_eq!(judged(rules, &[kick], &log).len(), 1);
    }

    #[test]
    fn the_rest_of_the_rules() {
        let mut log = Log {
            connections: 2,
            ..Log::default()
        };
        assert_eq!(
            judged(json!([{"rule": "connections", "count": 1}]), &[], &log).len(),
            1
        );
        let echoed = json!({"ev": "message", "type": "echoed", "payload": {"a": 1}});
        assert!(
            judged(
                json!([{"rule": "echoed", "payload": {"a": 1}}]),
                std::slice::from_ref(&echoed),
                &log
            )
            .is_empty()
        );
        assert_eq!(
            judged(
                json!([{"rule": "echoed", "payload": {"a": 2}}]),
                &[echoed],
                &log
            )
            .len(),
            1
        );
        log.taken = vec![Taken {
            kind: "valve".to_owned(),
            signed: true,
            ..taken("v", 1, false)
        }];
        assert_eq!(
            judged(json!([{"rule": "never_taken", "type": "valve"}]), &[], &log).len(),
            1
        );
        assert!(judged(json!([{"rule": "signed_taken", "count": 1}]), &[], &log).is_empty());
        assert_eq!(judged(json!([{"rule": "nonsense"}]), &[], &log).len(), 1);
    }
}
