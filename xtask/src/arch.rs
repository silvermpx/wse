//! The engine stays free of the Python layer.
//!
//! The WebSocket and cluster engine is meant to become a Rust library of its
//! own (wse-rs), with the PyO3 bindings as one consumer. The compiler cannot
//! see that boundary inside one crate, so this check does:
//!
//! - `engine-pyo3`: engine code uses no PyO3 (no `pyo3`, `Python`, `Py<..>`,
//!   `PyResult`, `Bound<'..>`, no `#[py...]` attributes).
//! - `engine-imports`: engine code does not reach into the Python layer: the
//!   `server::py` module, `pylock`, or the utility modules that define Python
//!   functions (`jwt`, `compression`, ...).
//! - `one-pymethods`: `RustWSEServer` has one `#[pymethods]` block, in
//!   `server/py/mod.rs`, so every Python-visible signature is in one place.
//!
//! What still crosses the boundary is listed in `xtask/arch_allow.toml`, each
//! entry with its reason. An entry that matches nothing any more fails the
//! check, so the list shrinks with the code it excuses.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use regex::Regex;
use serde::Deserialize;

use crate::lexer::{self, Lang};

/// Engine sources: what a native wse-rs keeps.
fn is_engine(path: &str) -> bool {
    let engine_dirs = ["rust/src/wse/cluster/", "rust/src/wse/server/"];
    let engine_files = [
        "rust/src/crypto.rs",
        "rust/src/wse/presence.rs",
        "rust/src/wse/recovery.rs",
        "rust/src/wse/reliability.rs",
    ];
    if path.starts_with("rust/src/wse/server/py/") || path == "rust/src/wse/server/mod.rs" {
        // The Python layer, and the module root that wires it in.
        return false;
    }
    engine_files.contains(&path) || engine_dirs.iter().any(|d| path.starts_with(d))
}

fn pyo3_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"\bpyo3\b|\bPython\s*(?:<|::)|\bPy<|\bPy(?:Result|Err|Any|Dict|List|Bytes|String|Tuple)\b|\bBound<'|#\[py(?:class|function|methods|module)\b",
        )
        .expect("a fixed pattern")
    })
}

fn python_layer_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"\bpylock\b|\bserver::py\b|\bsuper::py\b|\bcrate::jwt\b|\bcrate::wse::(?:compression|filters|queue|rate_limiter|security|sequencer|transformer)\b",
        )
        .expect("a fixed pattern")
    })
}

#[derive(Debug, Deserialize)]
struct AllowFile {
    #[serde(default)]
    allow: Vec<Allow>,
}

#[derive(Debug, Deserialize)]
struct Allow {
    rule: String,
    file: String,
    reason: String,
}

pub struct Report {
    pub violations: Vec<String>,
    pub stale: Vec<String>,
    pub allowed: usize,
}

/// `sources`: (repository path, contents) of every Rust file under rust/src.
pub fn check(sources: &[(String, String)], allow_toml: &str) -> Result<Report, String> {
    let allow: AllowFile =
        toml::from_str(allow_toml).map_err(|e| format!("xtask/arch_allow.toml: {e}"))?;
    for entry in &allow.allow {
        if entry.reason.trim().is_empty() {
            return Err(format!(
                "xtask/arch_allow.toml: the entry for {} ({}) has no reason",
                entry.file, entry.rule
            ));
        }
    }

    // (rule, file) -> the offending lines.
    let mut hits: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    let mut pymethods: Vec<String> = Vec::new();

    for (path, src) in sources {
        let lines = lexer::split(src, Lang::Rust);
        let in_tests = path.ends_with("/tests.rs");
        for line in &lines {
            if path.starts_with("rust/src/wse/server/py/") && line.code.contains("#[pymethods]") {
                pymethods.push(format!("{path}:{}", line.number));
            }
            if !is_engine(path) || in_tests {
                continue;
            }
            let code = line.code.trim();
            if pyo3_pattern().is_match(code) {
                hits.entry(("engine-pyo3".into(), path.clone()))
                    .or_default()
                    .push(format!("{path}:{}: {code}", line.number));
            }
            if python_layer_pattern().is_match(code) {
                hits.entry(("engine-imports".into(), path.clone()))
                    .or_default()
                    .push(format!("{path}:{}: {code}", line.number));
            }
        }
    }

    let mut report = Report {
        violations: Vec::new(),
        stale: Vec::new(),
        allowed: 0,
    };

    for ((rule, file), lines) in &hits {
        if allow
            .allow
            .iter()
            .any(|a| &a.rule == rule && &a.file == file)
        {
            report.allowed += lines.len();
        } else {
            report
                .violations
                .extend(lines.iter().map(|l| format!("[{rule}] {l}")));
        }
    }
    for a in &allow.allow {
        if !hits.contains_key(&(a.rule.clone(), a.file.clone())) {
            report.stale.push(format!(
                "[{}] {}: the allowance matches nothing any more; remove it",
                a.rule, a.file
            ));
        }
    }

    let expected = "rust/src/wse/server/py/mod.rs";
    let elsewhere: Vec<&String> = pymethods
        .iter()
        .filter(|p| !p.starts_with(expected))
        .collect();
    if pymethods.len() != 1 || !elsewhere.is_empty() {
        report.violations.push(format!(
            "[one-pymethods] RustWSEServer needs exactly one #[pymethods] block, in {expected}; found: {}",
            if pymethods.is_empty() { "none".to_string() } else { pymethods.join(", ") }
        ));
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(path: &str, body: &str) -> (String, String) {
        (path.to_string(), body.to_string())
    }

    fn one_pymethods() -> (String, String) {
        src(
            "rust/src/wse/server/py/mod.rs",
            "#[pymethods]\nimpl RustWSEServer {}\n",
        )
    }

    #[test]
    fn engine_code_may_not_use_pyo3_or_the_python_layer() {
        let sources = vec![
            one_pymethods(),
            src(
                "rust/src/wse/cluster/peer.rs",
                "use pyo3::prelude::*;\n// pyo3 in a comment is fine\nlet s = \"crate::jwt\";\nuse crate::jwt::decode;\n",
            ),
            src(
                "rust/src/wse/server/py/messaging.rs",
                "use pyo3::prelude::*;\n",
            ),
        ];
        let report = check(&sources, "").unwrap();
        assert_eq!(report.violations.len(), 2, "{:?}", report.violations);
        assert!(report.violations[0].starts_with("[engine-imports]"));
        assert!(report.violations[1].starts_with("[engine-pyo3]"));
    }

    #[test]
    fn allowances_need_a_reason_and_go_stale() {
        let sources = vec![
            one_pymethods(),
            src("rust/src/wse/server/state.rs", "use pyo3::Py;\n"),
        ];
        let allow = r#"
[[allow]]
rule = "engine-pyo3"
file = "rust/src/wse/server/state.rs"
reason = "callbacks"

[[allow]]
rule = "engine-pyo3"
file = "rust/src/wse/server/connection.rs"
reason = "gone already"
"#;
        let report = check(&sources, allow).unwrap();
        assert!(report.violations.is_empty());
        assert_eq!(report.allowed, 1);
        assert_eq!(report.stale.len(), 1);

        let no_reason = "[[allow]]\nrule = \"engine-pyo3\"\nfile = \"x.rs\"\nreason = \" \"\n";
        assert!(check(&sources, no_reason).is_err());
    }

    #[test]
    fn a_second_pymethods_block_is_refused() {
        let sources = vec![
            one_pymethods(),
            src(
                "rust/src/wse/server/py/extra.rs",
                "#[pymethods]\nimpl RustWSEServer {}\n",
            ),
        ];
        let report = check(&sources, "").unwrap();
        assert_eq!(report.violations.len(), 1);
        assert!(report.violations[0].starts_with("[one-pymethods]"));
    }
}
