# WSE Stress Test — Rust Client (wse-bench)

Tested the true server limits by removing the Python client bottleneck.
Previous Python bench (`bench_brutal.py`) topped out at ~6.9M msg/s with 1,000 connections —
the asyncio event loop and GIL were the ceiling, not the server.

Built a native Rust benchmark client (`wse-bench`) using tokio with one task per connection.
This is the first time we've been able to push the server past the Python overhead.

## Hardware

AMD EPYC 7502P (64 cores, 128 threads), 128 GB RAM, Ubuntu 24.04.
Client and server on the same machine (loopback, zero network noise).

Server: `bench_server.py` with `RustWSEServer` — same maturin-compiled binary as production,
running through the full Python wrapper with drain_mode=ON and JWT auth enabled.

## OS Tuning

```
ulimit -n 500000
net.ipv4.ip_local_port_range = 1024 65535
net.core.somaxconn = 65535
net.ipv4.tcp_tw_reuse = 2
```

For >64K connections: multi-IP via loopback aliases (127.0.0.1, 127.0.0.2).

---

## Throughput Saturation

All connections send 194-byte JSON trading messages as fast as possible for 10 seconds.

| Connections | Msg/s | GB/s | Per-conn msg/s | Errors |
|-------------|-------|------|----------------|--------|
| 100 | 13.2M | 2.3 | 131,931 | 0 |
| 500 | 13.3M | 2.3 | 26,548 | 0 |
| 1,000 | 13.1M | 2.3 | 13,148 | 0 |
| 2,000 | 10.6M | 1.9 | 5,309 | 0 |
| 5,000 | 10.8M | 1.9 | 2,159 | 0 |
| 10,000 | 10.3M | 1.8 | 1,028 | 0 |
| 20,000 | 9.8M | 1.7 | 488 | 0 |
| 30,000 | 11.5M | 2.0 | 385 | 0 |
| 50,000 | 10.3M | 1.8 | 207 | 0 |
| 75,000 | 11.3M | 2.0 | 150 | 0 |
| 100,000 | 9.5M | 1.7 | 95 | 0 |

Peak throughput: **13.3M msg/s** at 500 connections (2.3 GB/s).
The server stays above 9M msg/s even at 100K concurrent connections.
Zero errors at every tier — not a single dropped connection.

### Python vs Rust Client Comparison

| Metric | Python Client | Rust Client | Improvement |
|--------|--------------|-------------|-------------|
| Peak msg/s | 6.9M (1K conns) | 13.3M (500 conns) | **1.9x** |
| Max connections tested | ~1,000 | **100,000** | **100x** |
| Throughput at 100 conns | 2.3M | 13.2M | **5.7x** |
| Connection errors | 0 | 0 | same |

The Python client was the bottleneck all along. The server had 2x more headroom
than we could measure with Python.

---

## Connection Storm

All connections opened as fast as possible in batches of 500. Measures TCP handshake +
HTTP upgrade + JWT validation + `server_ready` round-trip time.

| Connections | Accept Rate | p50 | p95 | p99 | p99.9 | Failed |
|-------------|-------------|-----|-----|-----|-------|--------|
| 100 | 3,753/s | 2 ms | 26 ms | 26 ms | 26 ms | 0 |
| 1,000 | 11,096/s | 16 ms | 35 ms | 44 ms | 49 ms | 0 |
| 5,000 | 14,876/s | 11 ms | 22 ms | 27 ms | 28 ms | 0 |
| 10,000 | 14,651/s | 10 ms | 20 ms | 77 ms | 80 ms | 0 |
| 20,000 | 15,469/s | 10 ms | 18 ms | 51 ms | 54 ms | 0 |
| 30,000 | 14,897/s | 10 ms | 17 ms | 115 ms | 120 ms | 0 |
| 50,000 | 7,512/s | 14 ms | 89 ms | 120 ms | 174 ms | 0 |
| 75,000 | 4,981/s | 16 ms | 155 ms | 179 ms | 330 ms | 0 |
| 100,000 | 5,090/s | 16 ms | 156 ms | 179 ms | 371 ms | 0 |

Peak accept rate: **15,469 conn/s** at 20K tier. Even at 100K, the p50 handshake latency
is only 16ms and the failure rate is zero. Every single connection completes the full
JWT-authenticated handshake.

---

## Ping/Pong Latency Under Load

Each connection sends 50 application-level PINGs, server responds with PONGs.
All connections ping concurrently — this is worst-case latency, not sequential.

| Connections | Total Pings | p50 | p95 | p99 | p99.9 | p99.99 |
|-------------|-------------|-----|-----|-----|-------|--------|
| 100 | 5,000 | 1 ms | 1 ms | 2 ms | 37 ms | 39 ms |
| 1,000 | 50,000 | 6 ms | 14 ms | 20 ms | 35 ms | 43 ms |
| 5,000 | 250,000 | 32 ms | 90 ms | 120 ms | 153 ms | 182 ms |
| 10,000 | 500,000 | 61 ms | 180 ms | 231 ms | 276 ms | 339 ms |
| 20,000 | 1,000,000 | 154 ms | 306 ms | 411 ms | 583 ms | 728 ms |
| 30,000 | 1,500,000 | 262 ms | 437 ms | 645 ms | 918 ms | 1,264 ms |
| 50,000 | 2,500,000 | 485 ms | 695 ms | 1,112 ms | 1,624 ms | 2,078 ms |

At 100 connections, p50 is 1ms. At 50K concurrent connections all pinging simultaneously,
p50 is 485ms — which is reasonable given 50,000 round-trips competing for the same server.
The server never drops a connection or fails to respond.

---

## Key Takeaways

1. **13.3M msg/s peak** — the true server ceiling is roughly 2x what the Python client showed
2. **100K concurrent connections** with zero failures — the server handles it clean
3. **Latency stays reasonable** — 16ms p50 handshake even at 100K, 1ms ping RTT at low load
4. **The bottleneck was always the client** — Python's asyncio/GIL limited us, not the Rust server core
5. **Production-identical server** — these results are from the actual maturin-compiled binary
   running through the full Python wrapper with drain_mode, JWT auth, the whole stack

## Reproducing

```bash
# Build the client
cd benchmarks/rust-bench
cargo build --release

# Start the server (separate terminal)
cd /root/wse && source venv/bin/activate
DRAIN_MODE=1 python benchmarks/bench_server.py --max-connections 100000

# Run the full suite
ulimit -n 500000
./target/release/wse-bench \
  --host 127.0.0.1 --port 5006 \
  --secret "bench-secret-key-for-testing-only" \
  --tiers 100,500,1000,2000,5000,10000,20000,30000,50000,75000,100000 \
  --duration 10

# Quick test
./target/release/wse-bench --test throughput --tiers 100,1000 --duration 3
```

---

*Tested February 24, 2026. WSE v1.3.8, wse-bench v0.1.0.*
