//! What a run says: a report for each agent (JSON, and JUnit XML for CI's
//! pages), and the check of the reports against the agents the release
//! needs.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::cases::Catalog;

/// The client a driver drives.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Agent {
    pub agent: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub runtime: String,
    #[serde(default)]
    pub os: String,
    #[serde(default)]
    pub arch: String,
    #[serde(default)]
    pub features: Vec<String>,
}

/// One case's verdict.
#[derive(Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub case: String,
    /// `pass`, `fail`, or `not-applicable`.
    pub verdict: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<String>,
    #[serde(default)]
    pub ms: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Report {
    #[serde(flatten)]
    pub agent: Agent,
    /// What the run came from: the commit the conformance server was started
    /// in (empty when it could not tell), and the server's own version.
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub server: String,
    pub cases: Vec<Verdict>,
}

fn xml(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// A name a driver gave, as part of a file's name: letters, digits, `-` and
/// `_` only, so it names a file in the folder it is written to and nowhere
/// else (`../x` is `___x`).
pub fn file_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl Report {
    /// `<dir>/<agent>.json` and `<dir>/<agent>.junit.xml`.
    pub fn write(&self, dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let name = file_name(&self.agent.agent);
        let json = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(dir.join(format!("{name}.json")), json + "\n")?;
        let failed = self.cases.iter().filter(|c| c.verdict == "fail").count();
        let skipped = self
            .cases
            .iter()
            .filter(|c| c.verdict == "not-applicable")
            .count();
        let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        let _ = writeln!(
            out,
            "<testsuite name=\"{}\" tests=\"{}\" failures=\"{failed}\" skipped=\"{skipped}\">",
            xml(&format!(
                "conformance {} ({} {})",
                self.agent.agent, self.agent.runtime, self.agent.os
            )),
            self.cases.len(),
        );
        for case in &self.cases {
            let _ = write!(
                out,
                "  <testcase classname=\"{}\" name=\"{}\" time=\"{:.3}\"",
                xml(&self.agent.agent),
                xml(&case.case),
                case.ms as f64 / 1000.0
            );
            match case.verdict.as_str() {
                "fail" => {
                    let _ = writeln!(
                        out,
                        ">\n    <failure message=\"{}\">{}</failure>\n  </testcase>",
                        xml(case.failures.first().map_or("failed", String::as_str)),
                        xml(&case.failures.join("\n"))
                    );
                }
                "not-applicable" => {
                    let _ = writeln!(
                        out,
                        ">\n    <skipped message=\"{}\"/>\n  </testcase>",
                        xml(case.why.as_deref().unwrap_or("not applicable"))
                    );
                }
                _ => out.push_str("/>\n"),
            }
        }
        out.push_str("</testsuite>\n");
        std::fs::write(dir.join(format!("{name}.junit.xml")), out)
    }
}

/// The agents a release needs, and the features each must have.
#[derive(Deserialize)]
struct Manifest {
    agents: BTreeMap<String, Needed>,
}

#[derive(Deserialize)]
struct Needed {
    features: Vec<String>,
    /// What the agent's runtime and system must say, one of these words of
    /// each (in any case): a report of another platform is no evidence of
    /// this one. None: any.
    #[serde(default)]
    runtime: Vec<String>,
    #[serde(default)]
    os: Vec<String>,
}

/// `value` has one of `words`, in any case; none needed: it has.
fn names_one(value: &str, words: &[String]) -> bool {
    let value = value.to_lowercase();
    words.is_empty() || words.iter().any(|w| value.contains(&w.to_lowercase()))
}

/// Every agent the manifest names has a report from `source`, of the
/// runtime and system named for it, claims at least the features named for
/// it, and passed every case those features reach. What fails, one line
/// each; none: the run holds.
pub fn check(
    manifest: &Path,
    reports: &Path,
    catalog: &Catalog,
    source: &str,
) -> Result<Vec<String>, String> {
    let manifest: Manifest = serde_json::from_str(
        &std::fs::read_to_string(manifest).map_err(|e| format!("{}: {e}", manifest.display()))?,
    )
    .map_err(|e| format!("{}: {e}", manifest.display()))?;
    let mut found = BTreeMap::new();
    for entry in std::fs::read_dir(reports).map_err(|e| format!("{}: {e}", reports.display()))? {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().is_some_and(|e| e == "json") {
            let report: Report = serde_json::from_str(
                &std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?,
            )
            .map_err(|e| format!("{}: {e}", path.display()))?;
            found.insert(report.agent.agent.clone(), report);
        }
    }
    let mut problems = Vec::new();
    for (agent, needed) in &manifest.agents {
        let Some(report) = found.get(agent) else {
            problems.push(format!("{agent}: no report"));
            continue;
        };
        // What ran: a report that does not say is no evidence.
        let named = &report.agent;
        for (field, value) in [
            ("version", &named.version),
            ("runtime", &named.runtime),
            ("os", &named.os),
            ("arch", &named.arch),
        ] {
            if value.trim().is_empty() {
                problems.push(format!("{agent}: the report names no {field}"));
            }
        }
        for (field, value, words) in [
            ("runtime", &named.runtime, &needed.runtime),
            ("os", &named.os, &needed.os),
        ] {
            if !value.trim().is_empty() && !names_one(value, words) {
                problems.push(format!(
                    "{agent}: the report's {field} is {value:?}, not one of {words:?}"
                ));
            }
        }
        if report.source != source {
            problems.push(format!(
                "{agent}: the report comes from {}, not {source}",
                if report.source.is_empty() {
                    "an unnamed source"
                } else {
                    &report.source
                }
            ));
        }
        let claimed: HashSet<&str> = report.agent.features.iter().map(String::as_str).collect();
        for feature in &needed.features {
            if !claimed.contains(feature.as_str()) {
                problems.push(format!("{agent}: does not claim {feature}"));
            }
        }
        let has: HashSet<&str> = needed.features.iter().map(String::as_str).collect();
        for case in &catalog.cases {
            if !case.needs.iter().all(|n| has.contains(n.as_str())) {
                continue;
            }
            match report.cases.iter().find(|v| v.case == case.case) {
                Some(v) if v.verdict == "pass" => {}
                Some(v) => problems.push(format!(
                    "{agent}: {} {}{}",
                    case.case,
                    v.verdict,
                    v.failures
                        .first()
                        .map(|f| format!(": {f}"))
                        .unwrap_or_default()
                )),
                None => problems.push(format!("{agent}: {} was not run", case.case)),
            }
        }
    }
    Ok(problems)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reports in a directory of their own, checked against a manifest
    /// that needs `agent` with no features (and `needed`'s runtime and
    /// system): what the check says.
    fn checked_as(
        report: serde_json::Value,
        source: &str,
        needed: serde_json::Value,
    ) -> Vec<String> {
        let dir = std::env::temp_dir().join(format!(
            "wse-conformance-report-{}-{}",
            std::process::id(),
            report["agent"].as_str().unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let manifest = dir.join("manifest.json");
        let agent = report["agent"].as_str().unwrap_or_default().to_owned();
        let mut needs = serde_json::json!({"features": []});
        if let Some(extra) = needed.as_object() {
            for (key, value) in extra {
                needs[key] = value.clone();
            }
        }
        std::fs::write(
            &manifest,
            serde_json::json!({"agents": {agent: needs}}).to_string(),
        )
        .unwrap();
        let reports = dir.join("reports");
        std::fs::create_dir_all(&reports).unwrap();
        std::fs::write(reports.join("r.json"), report.to_string()).unwrap();
        let catalog = Catalog { cases: Vec::new() };
        let problems = check(&manifest, &reports, &catalog, source).unwrap();
        std::fs::remove_dir_all(&dir).ok();
        problems
    }

    fn checked(report: serde_json::Value, source: &str) -> Vec<String> {
        checked_as(report, source, serde_json::Value::Null)
    }

    fn whole(agent: &str) -> serde_json::Value {
        serde_json::json!({
            "agent": agent, "version": "3.2.0", "runtime": "swift", "os": "iOS 27.2",
            "arch": "aarch64", "features": [], "source": "4398651abc", "server": "3.2.0", "cases": []
        })
    }

    #[test]
    fn a_report_names_what_ran_it() {
        assert!(checked(whole("whole"), "4398651abc").is_empty());
        for field in ["version", "runtime", "os", "arch"] {
            let mut report = whole(&format!("no-{field}"));
            report.as_object_mut().unwrap().remove(field);
            let problems = checked(report, "4398651abc");
            assert_eq!(problems.len(), 1, "{field}: {problems:?}");
            assert!(problems[0].contains(field), "{problems:?}");
        }
    }

    #[test]
    fn a_report_comes_from_the_source_asked_for() {
        assert!(checked(whole("this-source"), "4398651abc").is_empty());
        assert_eq!(checked(whole("other-source"), "1e2d7fd").len(), 1);
        let mut unnamed = whole("no-source");
        unnamed.as_object_mut().unwrap().remove("source");
        assert_eq!(checked(unnamed, "4398651abc").len(), 1);
    }

    #[test]
    fn a_report_is_of_the_runtime_and_system_its_agent_runs_on() {
        let ios = serde_json::json!({"runtime": ["swift"], "os": ["ios"]});
        assert!(checked_as(whole("on-ios"), "4398651abc", ios.clone()).is_empty());
        let mut linux = whole("on-linux");
        linux["os"] = "Linux 6.8".into();
        linux["runtime"] = "node 24.22.3".into();
        let problems = checked_as(linux, "4398651abc", ios);
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(
            problems[0].contains("runtime") && problems[1].contains("os"),
            "{problems:?}"
        );
    }
}
