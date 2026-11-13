# Contributing to WSE

Thank you for your interest in contributing to WSE. This guide covers the development setup, coding standards, and submission process.

## Development Setup

### Prerequisites

- Python 3.12+ for the server (the Python client supports 3.11+)
- Rust 1.95+ (stable; the minimum is `rust-version` in `rust/Cargo.toml`)
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
# Rust tests
cargo test --manifest-path rust/Cargo.toml

# Python server tests
pytest tests/

# Python client tests
cd python-client && pytest tests/

# TS client
npm test
```

Builds are portable by default: no CPU-specific flags are committed. To
benchmark on your own machine with its CPU features turned on:

```bash
RUSTFLAGS="-C target-cpu=native" maturin develop --release
```

### Repository Checks

```bash
cargo xtask fast      # rustfmt, clippy, then the checks below
cargo xtask lint      # comments and the engine boundary only
cargo xtask commits   # commit messages of your branch (origin/main..HEAD)
```

- **Comments** say why the code is the way it is, in plain English, for the
  next reader. Issue, ticket and audit numbers, plan or wave numbers and the
  dates of a change belong in the commit message, not in the code. Python
  docstrings count as comments.
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
  rust/                  # Rust core (server, cluster, presence, recovery, PyO3 bindings)
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
- Pass `cargo clippy -- -D warnings` with zero warnings.
- Use `DashMap` for concurrent state, `crossbeam` for channels.
- Prefer `Arc<Bytes>` for shared message payloads (zero-copy broadcast).

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
3. Ensure all tests pass locally.
4. Run `cargo xtask fast`.
5. Run `ruff format` and `ruff check` for Python changes.
6. Open a PR against `main` with a clear description.

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
