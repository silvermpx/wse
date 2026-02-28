# Contributing to WSE

Thank you for your interest in contributing to WSE. This guide covers the development setup, coding standards, and submission process.

## Development Setup

### Prerequisites

- Python 3.12+
- Rust 1.75+ (stable)
- Node.js 18+ (for TypeScript client)
- maturin (`pip install maturin`)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/niceguy135/wse.git
cd wse

# Build the Rust extension (development mode)
maturin develop --manifest-path rust/Cargo.toml

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

# TypeScript client
npm test
```

## Project Structure

```
wse/
  rust/                  # Rust core (PyO3 bindings, server, cluster, presence, recovery)
  wse_server/            # Python server package (thin wrapper over Rust)
  python-client/         # Python client SDK (wse-client on PyPI)
  client/                # TypeScript/React client SDK (wse-client on npm)
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

- Format with Prettier.
- Lint with ESLint.
- Strict TypeScript mode (`strict: true`).

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
4. Run `cargo fmt` and `cargo clippy` for Rust changes.
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
