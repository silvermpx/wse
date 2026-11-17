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
    pub cases: Vec<Verdict>,
}

fn xml(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

impl Report {
    /// `<dir>/<agent>.json` and `<dir>/<agent>.junit.xml`.
    pub fn write(&self, dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let name: String = self
            .agent
            .agent
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
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
}

/// Every agent the manifest names has a report, claims at least the
/// features named for it, and passed every case those features reach.
/// What fails, one line each; none: the run holds.
pub fn check(manifest: &Path, reports: &Path, catalog: &Catalog) -> Result<Vec<String>, String> {
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
