//! Repository checks for WSE, run as `cargo xtask <command>` from the
//! repository root (the alias lives in `.cargo/config.toml`).
//!
//!   comments        comments are English prose: no audit ids, issue, task or
//!                   phase numbers, no dates of our own changes
//!   arch            the engine stays free of the Python layer (see arch.rs)
//!   names           no version numbers (v2, v3) in names of code or files
//!   lint            comments + arch + names
//!   commits [RANGE] commit messages carry no AI co-author or
//!                   "generated with" lines (default: origin/main..HEAD)
//!   fast            the quick local gate, CI's Rust lint job: cargo fmt
//!                   --check, clippy with warnings denied for each build of
//!                   the crates, then lint
//!   hooks           installs the gate before every push (hooks/pre-push):
//!                   git takes its hooks from hooks/
//!   check [LANE]    fast, then CI's jobs on this machine side by side (see
//!                   check.rs): each lane this machine has the tools for,
//!                   the others reported as skipped with the reason; with
//!                   LANE, that lane alone
//!
//! Each command exits non-zero on a finding, so CI runs the same commands.

mod arch;
mod check;
mod comments;
mod lexer;
mod names;

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use regex::Regex;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let root = repo_root();
    let ok = match args.first().map(String::as_str) {
        Some("comments") => run_comments(&root),
        Some("arch") => run_arch(&root),
        Some("names") => run_names(&root),
        Some("lint") => run_comments(&root) & run_arch(&root) & run_names(&root),
        Some("commits") => run_commits(&root, args.get(1).map(String::as_str)),
        Some("fast") => run_fast(&root),
        Some("hooks") => run_hooks(&root),
        // Without a lane, CI's Rust lint first, as CI runs it before the tests.
        Some("check") => match args.get(1) {
            Some(lane) => check::run(&root, Some(lane)),
            None => run_fast(&root) && check::run(&root, None),
        },
        _ => {
            eprintln!(
                "usage: cargo xtask <comments | arch | names | lint | commits [RANGE] | fast | hooks | check [LANE]>"
            );
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

fn run_names(root: &Path) -> bool {
    let mut findings: Vec<String> = Vec::new();
    for path in repo_files(root) {
        if names::versioned_file(&path) {
            findings.push(format!("{path}: the file name carries a version number"));
        }
        let Some(lang) = lexer::Lang::of(&path) else {
            continue;
        };
        if lang == lexer::Lang::Hash {
            continue;
        }
        let Ok(src) = std::fs::read_to_string(root.join(&path)) else {
            continue;
        };
        for f in names::check_code(&path, &src, lang) {
            findings.push(format!("{}:{}: {}", f.path, f.line, f.name));
        }
    }
    for f in &findings {
        eprintln!("{f}");
    }
    if findings.is_empty() {
        println!("names: ok");
        true
    } else {
        eprintln!(
            "\nnames: {} name(s) carry a version number. Name a function, type, module or \
             file by what it does, not by its version (a protocol version is data, not a \
             name). A library's own name can be marked `names:allow` on its line.",
            findings.len()
        );
        false
    }
}

fn run_arch(root: &Path) -> bool {
    let sources: Vec<(String, String)> = repo_files(root)
        .into_iter()
        .filter(|p| p.starts_with("crates/") && p.ends_with(".rs"))
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

fn run_hooks(root: &Path) -> bool {
    let status = Command::new("git")
        .args(["config", "core.hooksPath", "hooks"])
        .current_dir(root)
        .status();
    if !matches!(status, Ok(s) if s.success()) {
        eprintln!("hooks: git config core.hooksPath failed");
        return false;
    }
    println!("hooks: git runs hooks/pre-push before every push");
    true
}

/// The toolchain cargo runs has the standard library of `target`.
fn has_target(target: &str) -> bool {
    Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .is_some_and(|sysroot| {
            Path::new(sysroot.trim())
                .join("lib/rustlib")
                .join(target)
                .join("lib")
                .is_dir()
        })
}

fn run_fast(root: &Path) -> bool {
    // CI's Rust lint job, then the xtask's own: format, then clippy with
    // warnings denied for every build the crates are made in.
    let xtask = root.join("xtask/Cargo.toml");
    let xtask = xtask.to_str().expect("a UTF-8 repository path");
    let clippy = |extra: &[&'static str]| -> Vec<&'static str> {
        let mut args = vec!["clippy", "--locked", "--all-targets"];
        args.extend_from_slice(extra);
        args.extend_from_slice(&["--", "-D", "warnings"]);
        args
    };
    let steps: Vec<(&str, Vec<&str>)> = vec![
        ("rustfmt", vec!["fmt", "--all", "--", "--check"]),
        (
            "rustfmt (xtask)",
            vec!["fmt", "--manifest-path", xtask, "--", "--check"],
        ),
        ("clippy wse", clippy(&["-p", "wse"])),
        (
            "clippy wse, no default features",
            clippy(&["-p", "wse", "--no-default-features"]),
        ),
        ("clippy workspace", clippy(&["--workspace"])),
        (
            "clippy wse-client, no default features",
            clippy(&["-p", "wse-client", "--no-default-features"]),
        ),
        (
            "clippy wse-client, native",
            clippy(&["-p", "wse-client", "--features", "native"]),
        ),
        ("clippy (xtask)", {
            let mut args = vec![
                "clippy",
                "--locked",
                "--all-targets",
                "--manifest-path",
                xtask,
            ];
            args.extend_from_slice(&["--", "-D", "warnings"]);
            args
        }),
    ];
    // CI's browser build too, where this toolchain has the target.
    let wasm = ["--target", "wasm32-unknown-unknown"];
    let mut steps = steps;
    if has_target("wasm32-unknown-unknown") {
        steps.extend([
            (
                "clippy wse, browser",
                clippy(&["-p", "wse", "--no-default-features", wasm[0], wasm[1]]),
            ),
            (
                "clippy wse-client, browser",
                clippy(&["-p", "wse-client", wasm[0], wasm[1]]),
            ),
            (
                "clippy wse-client, browser, no default features",
                clippy(&[
                    "-p",
                    "wse-client",
                    "--no-default-features",
                    wasm[0],
                    wasm[1],
                ]),
            ),
        ]);
    } else {
        println!(
            "fast: no wasm32-unknown-unknown here (rustup target add it): CI checks the browser build"
        );
    }
    for (name, args) in steps {
        println!("== {name}");
        let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
            .args(&args)
            .current_dir(root)
            .status();
        if !matches!(status, Ok(s) if s.success()) {
            eprintln!("fast: {name} failed");
            return false;
        }
    }
    println!("== lint");
    run_comments(root) & run_arch(root) & run_names(root)
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
