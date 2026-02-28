# WSE Performance Benchmarks

Overview of WSE server performance across different benchmark clients and hardware.
Each client has its own detailed results page.

## Benchmark Clients

| Client | Language | Max Connections Tested | Peak Throughput | Details |
|--------|----------|----------------------|-----------------|---------|
| **wse-bench** | Rust (tokio) | **500,000** | **30M msg/s** | [Rust Client Results](BENCHMARKS_RUST_CLIENT.md) |
| **wse-bench** (fan-out) | Rust (tokio) | **500,000** | **5.0M del/s** | [Fan-out Results](BENCHMARKS_FANOUT.md) |
| **bench_brutal.py** | Python (sync) | 1,000 | 6.9M msg/s | [Python Client Results](BENCHMARKS_PYTHON_CLIENT.md) |
| **bench_wse_multiprocess.py** | Python (multi-proc) | 128 | 2.1M msg/s | [Python Client Results](BENCHMARKS_PYTHON_CLIENT.md) |

The Rust client removed the Python overhead and revealed the true server ceiling.
The Python client was always the bottleneck - the server had roughly 2x more headroom
than Python could measure.

---

## Server Hardware

Primary test environment: AMD EPYC 7502P (32 cores, 64 threads), 128 GB RAM, Ubuntu 24.04.
Client and server on the same machine (loopback, zero network noise). M2 MacBook used
for development benchmarks.

Server binary is the production maturin-compiled `RustWSEServer` running through the
full Python wrapper with drain_mode, JWT auth, the whole stack.

---

## Headline Numbers

| Metric | Value | Source |
|--------|-------|--------|
| Peak throughput (JSON) | **14.7M msg/s** (2.6 GB/s) | Rust client, 500 conns |
| Peak throughput (binary) | **30M msg/s** | Rust client, 100 conns |
| Fan-out broadcast | **5.0M del/s** | 100 subs, 0 gaps |
| Fan-out cluster (2 nodes) | **9.5M del/s** | Native TCP mesh, 20K subs, 0 gaps |
| Peak message rate | **20.7M msg/s** | Rust client, 64B payload |
| Throughput at 50K conns | **13.8M msg/s** (2.4 GB/s) | Rust client |
| Max concurrent connections | **500,000** (0 errors) | Rust client |
| Connection accept rate | **15,020 conn/s** | Rust client, 2K tier |
| Ping RTT at low load | **0.38 ms** p50 | Rust client, 100 conns |
| Peak bandwidth | **20.4 GB/s** | Rust client, 16KB msgs |
| Sustained hold (100K conns, 30s) | **100% survival** | Rust client |
| drain_mode overhead | **< 2%** | Python client comparison |

---

## Rust Acceleration by Component

Per-operation speedup from moving hot-path logic from Python to Rust (PyO3/maturin).

| Component | Pure Python | Rust | Speedup |
|-----------|------------|------|---------|
| JWT decode (HS256) | 850 us | 10 us | **85x** |
| Msgpack inbound parse | ~50 us | ~1 us | **~50x** |
| Rate limiter (check) | 2 us | 0.05 us | **40x** |
| Sequence + dedup | 8 us | 0.3 us | **27x** |
| Priority queue (enqueue) | 5 us | 0.2 us | **25x** |
| HMAC-SHA256 sign | 18 us | 0.8 us | **22x** |
| Event transform | 45 us | 3 us | **15x** |
| Filter match | 15 us | 1.2 us | **12.5x** |
| Compression (1KB zlib) | 12 us | 1.8 us | **6.7x** |

---

## Performance Optimization History

| Phase | Throughput | Connection Latency | Improvement |
|-------|-----------|-------------------|-------------|
| Initial (unoptimized) | 34,000 msg/s | ~50 ms | Baseline |
| Python optimization (9 fixes) | 106,000 msg/s | 23 ms | **3.1x** throughput |
| Rust acceleration (v1.0) | 113,000 msg/s | 23 ms | 1.07x throughput |
| Rust JWT + transport (v1.2) | 113,000 msg/s | **0.53 ms** | **27x** latency |
| Multi-process (M2, 10w) | **356,000 msg/s** | 2.20 ms | 3.1x vs single |
| Multi-process burst (M2) | **488,000 msg/s** | -- | ~0.5M msg/s |
| EPYC 7502P (64w, Python) | **2,045,000 msg/s** | 2.60 ms | 5.7x vs M2 |
| EPYC 7502P (64w, MsgPack) | **2,072,000 msg/s** | -- | ~2M msg/s |
| **Rust client (JSON)** | **14,700,000 msg/s** | -- | **7.2x vs Python** |
| **Rust client (compressed)** | **30,000,000 msg/s** | -- | **14.7x vs Python** |

### Python Optimization Highlights

1. **orjson** (3-5x faster JSON) - replaced stdlib `json.dumps`/`json.loads`
2. **Log level downgrade** - hot-path INFO to DEBUG (eliminated disk I/O per message)
3. **deque(maxlen)** - replaced `list` + `pop(0)` with O(1) eviction
4. **OrderedDict dedup** - FIFO duplicate tracking without linear scan
5. **Priority queue reduction** - 10K to 1K capacity (5x memory savings per connection)

### Rust Acceleration Highlights

1. **Rust JWT in handshake** - 27x faster connection setup (GIL eliminated from critical path)
2. **flate2 compression** - 6.7x faster than Python zlib
3. **AHashSet dedup** - 27x faster duplicate detection
4. **BinaryHeap priority queue** - 25x faster message ordering
5. **Zero-copy transforms** - 15x faster event envelope construction
6. **Atomic rate limiter** - 40x faster token bucket
7. **Large message path** - 5x faster at 64KB (2.7 GB/s single client)
8. **Inbound msgpack parsing** - binary frames parsed in Rust via rmpv (zero Python overhead)

---

## Benchmark Methodology

All benchmarks run on localhost to eliminate network variance. Each test runs multiple
rounds and reports percentile statistics. Results are deterministic within ~5% variance
across runs.

**Server**: Standalone `RustWSEServer` on dedicated port, Rust JWT auth in handshake.
No FastAPI, no database overhead. Production-identical binary through full Python wrapper.

**Protocol**: Connect to `ws://host:port/wse` with `access_token` cookie (JWT) ->
Rust validates JWT during WebSocket handshake -> receive `server_ready` -> send
`client_hello` -> run test workload -> collect statistics.

### Reproducing

Python client:
```bash
python benchmarks/bench_server.py
python benchmarks/bench_wse.py
python benchmarks/bench_wse_multiprocess.py --workers 64
python benchmarks/bench_brutal.py --token <JWT> --tiers 100,500,1000 --duration 5
```

Rust client:
```bash
cd benchmarks/rust-bench && cargo build --release
./target/release/wse-bench --host 127.0.0.1 --port 5006 \
  --secret "bench-secret-key-for-testing-only" \
  --tiers 100,500,1000,5000,10000,50000,100000 --duration 10
```

---

*Tested February 2026. WSE v2.0.0, wse-bench v0.1.0.*
