# Contributing to WSE

Thank you for your interest in contributing to WSE. This guide covers the development setup, coding standards, and submission process.

## Development Setup

### Prerequisites

- Python 3.12+ for the server (the Python client supports 3.11+)
- Rust 1.95+ (stable; the minimum is `rust-version` in `Cargo.toml`)
- Node.js 22 for the TS client's tests and build (the published package runs on Node 18+)
- maturin 1.16+ (`pip install maturin`)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/silvermpx/wse.git
cd wse

# Build the Rust extension into the active virtualenv
maturin develop --release

# Install Python dependencies
pip install -e ".[dev]"

# Install TypeScript dependencies
npm install
```

### Running Tests

```bash
# Rust tests: the engine, then with its Python module; the protocol alone
# (the crate without its server, as the client uses it); the Rust client,
# with its crypto and without
cargo test -p wse
cargo test -p wse --features python
cargo test -p wse --no-default-features
cargo test -p wse-client
cargo test -p wse-client --no-default-features

# The pure-Rust crypto the client has in the browser, natively
RUSTFLAGS="--cfg wse_rustcrypto" CARGO_TARGET_DIR=target/rustcrypto \
  cargo test -p wse --no-default-features --lib
RUSTFLAGS="--cfg wse_rustcrypto" CARGO_TARGET_DIR=target/rustcrypto \
  cargo test -p wse-client --lib --tests

# The client in the browser (wasm32), in Node 22, against the server example:
# rustup target add wasm32-unknown-unknown; cargo install wasm-bindgen-cli
# at the version in Cargo.lock; then see crates/wse-client/tests/browser.rs

# Python server tests
pytest tests/

# Python client tests
cd python-client && pytest tests/

# TS client
npm test

# The mobile clients, each against the Rust client's example server:
# Swift on the Mac (SIMULATOR=1 also on an iOS simulator of its own),
# Kotlin on the JVM, Dart with Flutter; and on an Android emulator the
# Kotlin library, an app built with R8 and the Flutter example
bindings/swift/run-tests.sh
bindings/kotlin/run-tests.sh
bindings/dart/run-tests.sh
bindings/kotlin/android/run-emulator-tests.sh
bindings/kotlin/android/run-consumer-check.sh
bindings/dart/example/run-integration-tests.sh <device id>
```

Builds are portable by default: no CPU-specific flags are committed. To
benchmark on your own machine with its CPU features turned on:

```bash
RUSTFLAGS="-C target-cpu=native" maturin develop --release
```

### Repository Checks

```bash
cargo xtask hooks     # once: the gate before every push (hooks/pre-push)
cargo xtask fast      # CI's Rust lint: rustfmt, clippy for each build, then the checks below
cargo xtask lint      # comments, version-free names and the engine boundary
cargo xtask commits   # commit messages of your branch (origin/main..HEAD)
cargo xtask check     # fast, then CI's jobs on this machine side by side (check LANE...: those)
```

The gate before a push checks the pushed commit in a worktree of its own
(kept under `~/.cache/prepush/`): `cargo xtask fast`, the TypeScript client's
types and tests, the Python client's lint and tests (through uv), and for the
pushed range the commit messages and a gitleaks scan. A push leaves only once
it passes.

`cargo xtask check` runs `fast`, then CI's jobs here: the Rust tests, the TypeScript client,
the Python client and server (and the TypeScript client against it), Kotlin
on the JVM, the Dart package, the Flutter example as a desktop app, and Swift.
A lane this machine has no tools for is skipped with the reason (Swift needs
a Mac; a Linux desktop app needs GTK 3 and xvfb), so a Mac and a Linux
machine between them cover CI. `FLUTTER=` and `GRADLE=` name those tools when
they are not on PATH. Each lane builds under `target/xtask-check/<lane>/` and
logs to `target/xtask-check/<lane>.log`. `cargo xtask check swift dart` runs
only the lanes named, side by side, without `fast`.

- **Comments** say why the code is the way it is, in plain English, for the
  next reader. Issue, ticket and audit numbers, plan or wave numbers and the
  dates of a change belong in the commit message, not in the code. Python
  docstrings count as comments.
- **Names** carry no version number: a function, type, module or file is
  named by what it does, not `..._v2`. A wire protocol's version is data
  (`protocol_version`), not part of a name.
- **Engine boundary**: the WebSocket and cluster engine (`cluster/`,
  `server/` outside `server/py/`, presence, recovery, reliability) does not
  use PyO3; the Python bindings live in `server/py/`. The places that still
  cross are listed with their reasons in `xtask/arch_allow.toml`.

CI runs the same commands, plus cargo-deny (`deny.toml`) for the Rust
dependencies and gitleaks (`.gitleaks.toml`) for secrets. The `ci-ok` job is
the one result a pull request needs.

## Project Structure

```
wse/
  crates/wse/            # The engine (server, cluster, presence, recovery) and the protocol it shares with the client; its Python module behind `python`
  crates/wse-client/     # The Rust client (wse-client on crates.io)
  crates/wse-py/         # The Python extension built from it (wse-server on PyPI)
  wse_server/            # Python server package (thin wrapper over Rust)
  python-client/         # Python client SDK (wse-client on PyPI)
  ts-client/             # TypeScript/React client SDK (wse-client on npm)
  xtask/                 # Repository checks (cargo xtask)
  examples/              # Working example scripts
  tests/                 # Integration tests
  benchmarks/            # Performance benchmarks (Rust, Python, TypeScript)
  docs/                  # Documentation
```

## Code Standards

### Rust

- Format with `cargo fmt` before every commit.
- Pass `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p wse --all-targets -- -D warnings` and `cargo clippy -p wse-client --no-default-features --all-targets -- -D warnings` with zero warnings.
- Use `DashMap` for concurrent state, `crossbeam` for channels.
- Prefer `Arc<Bytes>` for shared message payloads (zero-copy broadcast).

### Performance

Speed is a requirement from the first line, not something to win back later.

- The hot paths (fan-out, each connection's write task and read loop, the
  cluster links, the codecs) take no new lock, allocation, syscall, clock read
  or map lookup per message. Work that does not depend on the connection is
  done once per message, not once per subscriber; shared payloads are `Bytes`.
- Know what a primitive costs before it goes into a loop that runs per
  message: `CancellationToken::is_cancelled()` locks a mutex, a `Notify` wait
  registers under one, `UnboundedReceiver::len()` walks the queue, and a
  tokio task woken from a thread outside the runtime goes through a shared
  queue under a lock. Atomics, `Abortable` and the stack (arrays,
  `SmallVec`) are the defaults.
- Use every core where ordering allows it.
- A change that touches a hot path is measured against the previous release
  on the same machine and with the same build flags, and the numbers go in the
  pull request: `benchmarks/rust-bench` (`--test fanout-broadcast`,
  `--test throughput`, `--test fanout-cluster`) against
  `benchmarks/bench_fanout_server.py` and `benchmarks/bench_server.py`.

### Python

- Format with `ruff format`.
- Lint with `ruff check`.
- Type annotations on all public API methods.
- Docstrings on all public classes and methods (Google style).

### TypeScript

- Strict TypeScript mode (`strict: true`); `npx tsc --noEmit` must pass.
- Tests with vitest (`npm test`).

## Commit Messages

Write commit messages in imperative form. Describe what the change does, not what you did.

```
Add per-topic presence stats endpoint

- PresenceManager exposes O(1) member/connection counts
- New presence_stats() method on RustWSEServer
- Avoids full member iteration for dashboard use cases
```

For version bumps, include the version in the title:

```
v2.0.1 - Fix cluster frame size mismatch

Tighten sender payload limit to account for 8-byte header.
Receiver now correctly validates total wire frame size.
```

## Pull Requests

1. Create a feature branch from `main`.
2. Make your changes with clear, focused commits.
3. Run `cargo xtask check` (and install the push gate once: `cargo xtask hooks`).
4. Open a PR against `main` with a clear description.

## Reporting Issues

Open an issue on GitHub with:

- WSE version (`pip show wse-server`)
- Python version
- OS and architecture
- Steps to reproduce
- Expected vs actual behavior
- Relevant log output

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
