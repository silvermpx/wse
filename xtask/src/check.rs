//! `cargo xtask check [LANE]`: CI's jobs on this machine, side by side.
//!
//! Each lane is a shell script with the tools it needs. A lane whose tools
//! this machine lacks is skipped with the reason, so a Mac and a Linux
//! machine between them cover what CI covers (Swift needs a Mac, Gradle and
//! a desktop for Linux apps need Linux, and so on). Each lane builds in a
//! target directory of its own under target/xtask-check/, kept between runs,
//! and writes its log beside it. The summary says what passed, failed and
//! was skipped, and how long each took.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// A tool a lane needs: found on PATH, or named by an environment variable
/// (FLUTTER=..., GRADLE=...).
struct Tool {
    command: &'static str,
    env: Option<&'static str>,
}

const fn tool(command: &'static str) -> Tool {
    Tool { command, env: None }
}

const fn tool_or(command: &'static str, env: &'static str) -> Tool {
    Tool {
        command,
        env: Some(env),
    }
}

struct Lane {
    name: &'static str,
    needs: &'static [Tool],
    /// Runs only on this operating system (std::env::consts::OS).
    only_on: Option<&'static str>,
    script: &'static str,
}

const LANES: &[Lane] = &[
    Lane {
        name: "rust",
        needs: &[tool("cargo")],
        only_on: None,
        script: "cargo test --locked --workspace
cargo test --locked -p wse-client --features native",
    },
    Lane {
        // The client built for the browser, against the example server (in
        // Node, whose WebSocket is the browser's).
        name: "rust-browser",
        needs: &[tool("cargo"), tool("wasm-bindgen-test-runner")],
        only_on: None,
        script: "cargo build -q -p wse-client --locked --example server
log=\"$CARGO_TARGET_DIR/browser-server.log\"
WSE_PORT=$((20000 + RANDOM % 20000)) \"$CARGO_TARGET_DIR/debug/examples/server\" > \"$log\" 2>&1 &
server=$!
trap 'kill $server 2> /dev/null' EXIT
for _ in $(seq 1 100); do grep -q '^signing_public_key ' \"$log\" && break; sleep 0.1; done
WSE_TEST_URL=\"$(sed -n 's/^url //p' \"$log\")\" \\
WSE_TEST_IDENTITY=\"$(sed -n 's/^e2e_identity_public_key //p' \"$log\")\" \\
WSE_TEST_SIGNING=\"$(sed -n 's/^signing_public_key //p' \"$log\")\" \\
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \\
  cargo test -p wse-client --locked --target wasm32-unknown-unknown --test browser",
    },
    Lane {
        name: "typescript",
        needs: &[tool("node"), tool("npx")],
        only_on: None,
        script: "[ -d node_modules ] || { echo 'npm ci first: no node_modules'; exit 1; }
npx tsc --noEmit
npx vitest run",
    },
    Lane {
        // The Angular and Solid adapters in applications, built against the
        // packed client (Angular 22 needs Node 22.22 or newer).
        name: "frameworks",
        needs: &[tool("node"), tool("npm"), tool("cargo")],
        only_on: None,
        script: "[ -d node_modules ] || { echo 'npm ci first: no node_modules'; exit 1; }
node -e 'const [a, b] = process.versions.node.split(\".\").map(Number); process.exit(a > 22 || (a === 22 && b >= 22) ? 0 : 1)' \\
  || { echo 'skip: Angular 22 needs Node 22.22 or newer on PATH'; exit 3; }
npm run build > /dev/null
rm -rf \"${CARGO_TARGET_DIR:?}/npm-dist\" && mkdir -p \"$CARGO_TARGET_DIR/npm-dist\"
npm pack --ignore-scripts --pack-destination \"$CARGO_TARGET_DIR/npm-dist\" > /dev/null
for fixture in angular20 angular21 angular22 solid1 solid2; do
  .github/scripts/framework-consumer-check.sh \"$CARGO_TARGET_DIR\"/npm-dist/wse-client-*.tgz \"$fixture\"
done",
    },
    Lane {
        name: "python-client",
        needs: &[tool("uvx")],
        only_on: None,
        script: "uvx --quiet ruff@0.17.5 check python-client/
uvx --quiet ruff@0.17.5 format --check python-client/
uvx --quiet --from pytest --with-editable './python-client[all,dev]' pytest python-client/tests -q -p no:cacheprovider",
    },
    Lane {
        // The Python server package, and the TypeScript client against it.
        name: "python-server",
        needs: &[tool("uv"), tool("node"), tool("npx")],
        only_on: None,
        script: "venv=\"$CARGO_TARGET_DIR/venv\"
uv venv --quiet --allow-existing --python 3.12 \"$venv\"
uv pip install --quiet --python \"$venv\" 'maturin==1.16.0' pytest pytest-asyncio pytest-cov httpx websockets cryptography prometheus-client -e python-client
VIRTUAL_ENV=\"$venv\" \"$venv/bin/maturin\" develop --release --locked
\"$venv/bin/pytest\" tests/ -q -p no:cacheprovider
WSE_E2E_PYTHON=\"$venv/bin/python\" WSE_E2E_REQUIRED=1 npx vitest run --project client ts-client/__tests__/realServer.test.ts",
    },
    Lane {
        // Every client against the conformance server, one case at a time,
        // each on a server of its own; then the check of their reports.
        name: "conformance",
        needs: &[tool("cargo"), tool("node"), tool("npx"), tool("uvx")],
        only_on: None,
        script: "[ -d node_modules ] || { echo 'npm ci first: no node_modules'; exit 1; }
cargo build --locked -p wse-conformance
out=\"$(mktemp -d \"$CARGO_TARGET_DIR/conformance.XXXXXX\")\"
\"$CARGO_TARGET_DIR/debug/wse-conformance\" serve --out \"$out\" > \"$out/serve.log\" 2>&1 &
server=$!
trap 'kill $server 2> /dev/null' EXIT
for _ in $(seq 1 100); do grep -q '\"ready\"' \"$out/serve.log\" && break; sleep 0.1; done
export WSE_CONFORMANCE=\"$(sed -n 's/.*\"control\":\"\\([^\"]*\\)\".*/\\1/p' \"$out/serve.log\")\" WSE_CONFORMANCE_REQUIRED=1
npx vitest run --project client ts-client/__tests__/conformance.test.ts
cargo test --locked -p wse-client --test conformance
uvx --quiet --from pytest --with-editable './python-client[all,dev]' pytest python-client/tests/test_conformance.py -q -p no:cacheprovider
\"$CARGO_TARGET_DIR/debug/wse-conformance\" check --manifest tests/conformance/agents.json --reports \"$out\"",
    },
    Lane {
        // The tests on the JVM, then the JVM package as an application gets it.
        name: "kotlin-jvm",
        needs: &[tool("java"), tool_or("gradle", "GRADLE"), tool("cargo")],
        only_on: None,
        script: "bindings/kotlin/run-tests.sh
bindings/kotlin/run-jvm-consumer-check.sh",
    },
    Lane {
        name: "dart",
        needs: &[tool_or("flutter", "FLUTTER"), tool("flutter_rust_bridge_codegen"), tool("cargo")],
        only_on: None,
        script: "bindings/dart/run-tests.sh",
    },
    Lane {
        // The Flutter example as a desktop app on this machine, then its release build run.
        name: "flutter-desktop",
        needs: &[tool_or("flutter", "FLUTTER"), tool("cargo")],
        only_on: None,
        script: "case \"$(uname -s)\" in
  Darwin) device=macos; run=\"\" ;;
  Linux) device=linux; run=\"xvfb-run -a\"
    command -v xvfb-run > /dev/null || { echo 'skip: no xvfb-run'; exit 3; }
    pkg-config --exists gtk+-3.0 || { echo 'skip: no GTK 3 to build a Linux app'; exit 3; } ;;
  *) device=windows; run=\"\" ;;
esac
$run bindings/dart/example/run-integration-tests.sh \"$device\"
MODE=release $run bindings/dart/example/run-integration-tests.sh \"$device\"",
    },
    Lane {
        name: "swift",
        needs: &[tool("xcodebuild"), tool("cargo")],
        only_on: Some("macos"),
        script: "bindings/swift/run-tests.sh",
    },
];

/// Exit code a lane uses to say it was skipped after all (a missing piece
/// only the script can see).
const SKIPPED: i32 = 3;

enum Outcome {
    Passed(Duration),
    Failed(Duration),
    Skipped(String),
}

fn on_path(command: &str) -> bool {
    std::env::var_os("PATH")
        .is_some_and(|path| std::env::split_paths(&path).any(|dir| dir.join(command).is_file()))
}

fn missing(lane: &Lane) -> Option<String> {
    if let Some(os) = lane.only_on
        && std::env::consts::OS != os
    {
        return Some(format!("runs on {os} only"));
    }
    let absent: Vec<String> = lane
        .needs
        .iter()
        .filter(|t| !(t.env.is_some_and(|e| std::env::var_os(e).is_some()) || on_path(t.command)))
        .map(|t| match t.env {
            Some(e) => format!("{} (or {e}=)", t.command),
            None => t.command.to_owned(),
        })
        .collect();
    (!absent.is_empty()).then(|| format!("no {}", absent.join(", ")))
}

fn run_lane(root: &Path, lane: &Lane, dir: &Path) -> Outcome {
    let target = dir.join(lane.name);
    if let Err(e) = std::fs::create_dir_all(&target) {
        return Outcome::Skipped(format!("cannot make {}: {e}", target.display()));
    }
    let log = dir.join(format!("{}.log", lane.name));
    let out = match File::create(&log) {
        Ok(f) => f,
        Err(e) => return Outcome::Skipped(format!("cannot write {}: {e}", log.display())),
    };
    let err = match out.try_clone() {
        Ok(f) => f,
        Err(e) => return Outcome::Skipped(format!("cannot write {}: {e}", log.display())),
    };
    let started = Instant::now();
    let status = Command::new("bash")
        .arg("-c")
        .arg(format!("set -euo pipefail\n{}", lane.script))
        .current_dir(root)
        .env("CARGO_TARGET_DIR", &target)
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(err)
        .status();
    let took = started.elapsed();
    match status {
        Ok(s) if s.success() => Outcome::Passed(took),
        Ok(s) if s.code() == Some(SKIPPED) => {
            let reason = std::fs::read_to_string(&log)
                .ok()
                .and_then(|text| {
                    text.lines()
                        .rev()
                        .find_map(|l| l.strip_prefix("skip: ").map(str::to_owned))
                })
                .unwrap_or_else(|| "the lane skipped itself".to_owned());
            Outcome::Skipped(reason)
        }
        _ => Outcome::Failed(took),
    }
}

/// Runs the lanes named in `only` side by side, every lane when it is
/// empty; a name that is no lane runs nothing.
pub fn run(root: &Path, only: &[&str]) -> bool {
    let unknown: Vec<&str> = only
        .iter()
        .copied()
        .filter(|o| !LANES.iter().any(|l| l.name == *o))
        .collect();
    if !unknown.is_empty() {
        let names: Vec<&str> = LANES.iter().map(|l| l.name).collect();
        eprintln!(
            "check: no lane {}; the lanes: {}",
            unknown.join(", "),
            names.join(", ")
        );
        return false;
    }
    let lanes: Vec<&Lane> = LANES
        .iter()
        .filter(|l| only.is_empty() || only.contains(&l.name))
        .collect();
    let dir: PathBuf = root.join("target/xtask-check");
    let started = Instant::now();
    let results: Vec<(&str, Outcome)> = thread::scope(|scope| {
        let running: Vec<_> = lanes
            .iter()
            .map(|lane| {
                let dir = &dir;
                (
                    lane.name,
                    scope.spawn(move || match missing(lane) {
                        Some(reason) => Outcome::Skipped(reason),
                        None => {
                            println!("check: {} started", lane.name);
                            run_lane(root, lane, dir)
                        }
                    }),
                )
            })
            .collect();
        running
            .into_iter()
            .map(|(name, handle)| {
                let outcome = handle.join().unwrap_or(Outcome::Failed(Duration::ZERO));
                (name, outcome)
            })
            .collect()
    });
    println!(
        "\ncheck: {} lanes on {} in {:.0?}",
        lanes.len(),
        std::env::consts::OS,
        started.elapsed()
    );
    let mut ok = true;
    for (name, outcome) in &results {
        match outcome {
            Outcome::Passed(took) => println!("  passed   {name:<16} {:>6.0?}", took),
            Outcome::Failed(took) => {
                ok = false;
                println!(
                    "  FAILED   {name:<16} {:>6.0?}  target/xtask-check/{name}.log",
                    took
                );
            }
            Outcome::Skipped(reason) => println!("  skipped  {name:<16}         {reason}"),
        }
    }
    ok
}
