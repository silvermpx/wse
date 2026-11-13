//! Repository checks for WSE, run as `cargo xtask <command>` from the
//! repository root (the alias lives in `.cargo/config.toml`).
//!
//!   comments        comments are English prose: no audit ids, issue, task or
//!                   phase numbers, no dates of our own changes
//!   arch            the engine stays free of the Python layer (see arch.rs)
//!   lint            comments + arch
//!   commits [RANGE] commit messages carry no AI co-author or
//!                   "generated with" lines (default: origin/main..HEAD)
//!   fast            the quick local gate: cargo fmt --check, clippy with
//!                   warnings denied, then lint
//!
//! Each command exits non-zero on a finding, so CI runs the same commands.

mod arch;
mod comments;
mod lexer;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use regex::Regex;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let root = repo_root();
    let ok = match args.first().map(String::as_str) {
        Some("comments") => run_comments(&root),
        Some("arch") => run_arch(&root),
        Some("lint") => run_comments(&root) & run_arch(&root),
        Some("commits") => run_commits(&root, args.get(1).map(String::as_str)),
        Some("fast") => run_fast(&root),
        _ => {
            eprintln!("usage: cargo xtask <comments | arch | lint | commits [RANGE] | fast>");
            return ExitCode::from(2);
        }
    };
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in the repository root")
        .to_path_buf()
}

/// Tracked files plus new ones git does not ignore, as repository paths.
fn repo_files(root: &Path) -> Vec<String> {
    let out = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ])
        .current_dir(root)
        .output()
        .expect("git runs");
    assert!(out.status.success(), "git ls-files failed");
    let mut files: Vec<String> = String::from_utf8_lossy(&out.stdout)
        .split('\0')
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .filter(|p| root.join(p).is_file())
        .collect();
    files.sort();
    files.dedup();
    files
}

fn run_comments(root: &Path) -> bool {
    let mut scanned = 0;
    let mut findings = Vec::new();
    for path in repo_files(root) {
        let Some(lang) = lexer::Lang::of(&path) else {
            continue;
        };
        let Ok(src) = std::fs::read_to_string(root.join(&path)) else {
            continue;
        };
        scanned += 1;
        findings.extend(comments::check(&path, &src, lang));
    }
    if scanned == 0 {
        eprintln!("comments: no files scanned; refusing to pass");
        return false;
    }
    for f in &findings {
        eprintln!("{}:{}: [{}] {}", f.path, f.line, f.rule, f.text);
    }
    if findings.is_empty() {
        println!("comments: ok ({scanned} files)");
        true
    } else {
        eprintln!(
            "\ncomments: {} line(s) to rewrite. A comment says why, in plain English, as a person \
             would write it to a colleague: drop audit ids, issue, task and phase numbers and the \
             dates of our own changes (the commit log has them). A standard's name the term list \
             misses can be marked `comment-kitchen:allow` on its line.",
            findings.len()
        );
        false
    }
}

fn run_arch(root: &Path) -> bool {
    let sources: Vec<(String, String)> = repo_files(root)
        .into_iter()
        .filter(|p| p.starts_with("rust/src/") && p.ends_with(".rs"))
        .filter_map(|p| std::fs::read_to_string(root.join(&p)).ok().map(|s| (p, s)))
        .collect();
    let allow = std::fs::read_to_string(root.join("xtask/arch_allow.toml")).unwrap_or_default();
    let report = match arch::check(&sources, &allow) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("arch: {e}");
            return false;
        }
    };
    for v in &report.violations {
        eprintln!("{v}");
    }
    for s in &report.stale {
        eprintln!("{s}");
    }
    if report.violations.is_empty() && report.stale.is_empty() {
        println!(
            "arch: ok ({} files, {} allowed crossing(s) listed in xtask/arch_allow.toml)",
            sources.len(),
            report.allowed
        );
        true
    } else {
        eprintln!(
            "\narch: the engine (cluster/, server/ outside server/py/, presence, recovery, \
             reliability) must not depend on the Python layer. Move the Python part into \
             server/py/, or list a crossing that must stay for now in xtask/arch_allow.toml with \
             its reason."
        );
        false
    }
}

fn run_commits(root: &Path, range: Option<&str>) -> bool {
    let range = range.unwrap_or("origin/main..HEAD");
    let out = Command::new("git")
        .args(["log", "--format=%H%x00%B%x1e", range])
        .current_dir(root)
        .output()
        .expect("git runs");
    if !out.status.success() {
        eprintln!(
            "commits: git log {range} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
        return false;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut bad = 0;
    let mut checked = 0;
    for entry in text.split('\x1e').map(str::trim).filter(|e| !e.is_empty()) {
        checked += 1;
        let (hash, body) = entry.split_once('\0').unwrap_or((entry, ""));
        for line in ai_attribution(body) {
            bad += 1;
            eprintln!("{}: {line}", &hash[..hash.len().min(12)]);
        }
    }
    if bad == 0 {
        println!("commits: ok ({checked} in {range})");
        true
    } else {
        eprintln!(
            "\ncommits: commit messages must not carry AI co-author or \"generated with\" lines; \
             reword the commits."
        );
        false
    }
}

/// Lines of a commit message that credit an AI assistant as co-author or
/// generator.
fn ai_attribution(body: &str) -> Vec<String> {
    use std::sync::OnceLock;
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            r"(?im)^\s*co-authored-by:.*\b(?:claude|anthropic|copilot|chatgpt|openai|gemini)\b.*$|^.*generated (?:with|by) \[?(?:claude|chatgpt|copilot|gemini).*$",
        )
        .expect("a fixed pattern")
    });
    re.find_iter(body)
        .map(|m| m.as_str().trim().to_string())
        .collect()
}

fn run_fast(root: &Path) -> bool {
    let steps: [(&str, &[&str], &str); 4] = [
        ("rustfmt", &["fmt", "--check"], "rust"),
        ("rustfmt (xtask)", &["fmt", "--check"], "xtask"),
        (
            "clippy",
            &[
                "clippy",
                "--locked",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
            "rust",
        ),
        (
            "clippy (xtask)",
            &[
                "clippy",
                "--locked",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
            "xtask",
        ),
    ];
    for (name, args, dir) in steps {
        println!("== {name}");
        let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
            .args(args)
            .current_dir(root.join(dir))
            .status();
        if !matches!(status, Ok(s) if s.success()) {
            eprintln!("fast: {name} failed");
            return false;
        }
    }
    println!("== lint");
    run_comments(root) & run_arch(root)
}

#[cfg(test)]
mod tests {
    use super::ai_attribution;

    #[test]
    fn ai_co_author_lines_are_found() {
        let body = "fix: a thing\n\nCo-Authored-By: Claude <noreply@anthropic.com>\n🤖 Generated with [Claude Code](https://claude.com)\n";
        assert_eq!(ai_attribution(body).len(), 2);
        let human = "fix: a thing\n\nCo-authored-by: Jane Doe <jane@example.com>\nThe Claude Shannon bound\n";
        assert!(ai_attribution(human).is_empty());
    }
}
