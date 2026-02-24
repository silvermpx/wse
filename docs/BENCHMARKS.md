# WSE Performance Benchmarks

Overview of WSE server performance across different benchmark clients and hardware.
Each client has its own detailed results page.

## Benchmark Clients

| Client | Language | Max Connections Tested | Peak Throughput | Details |
|--------|----------|----------------------|-----------------|---------|
| **wse-bench** | Rust (tokio) | **100,000** | **20.5M msg/s** | [Rust Client Results](BENCHMARKS_RUST_CLIENT.md) |
| **bench_brutal.py** | Python (sync) | 1,000 | 6.9M msg/s | [Python Client Results](BENCHMARKS_PYTHON_CLIENT.md) |
| **bench_multiprocess.py** | Python (multi-proc) | 128 | 2.1M msg/s | [Python Client Results](BENCHMARKS_PYTHON_CLIENT.md) |

The Rust client removed the Python overhead and revealed the true server ceiling.
The Python client was always the bottleneck — the server had roughly 2x more headroom
than Python could measure.

---

## Server Hardware

Primary test environment: AMD EPYC 7502P (64 cores, 128 threads), 128 GB RAM, Ubuntu 24.04.
Client and server on the same machine (loopback, zero network noise). M2 MacBook used
for development benchmarks.

Server binary is the production maturin-compiled `RustWSEServer` running through the
full Python wrapper with drain_mode, JWT auth, the whole stack.

---

## Headline Numbers

| Metric | Value | Source |
|--------|-------|--------|
| Peak throughput (JSON) | **13.5M msg/s** (2.4 GB/s) | Rust client, 100 conns |
| Peak throughput (compressed) | **20.5M msg/s** | Rust client, 500 conns |
| Peak message rate | **19.4M msg/s** | Rust client, 64B payload |
| Throughput at 100K conns | **9.1M msg/s** (1.6 GB/s) | Rust client |
| Max concurrent connections | **100,000** (0 errors) | Rust client |
| Connection accept rate | **15,020 conn/s** | Rust client, 2K tier |
| Ping RTT at low load | **1 ms** p50 | Rust client, 100 conns |
| Peak bandwidth | **19.9 GB/s** | Rust client, 16KB msgs |
| Sustained hold (100K, 30s) | **100% survival** | Rust client |
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

## Comparison with Alternatives

All alternatives primarily benchmark fan-out (server broadcasts to clients) or idle
connection count, not inbound throughput (clients sending to server). WSE benchmarks
measure inbound throughput — the harder and more realistic workload.

### Throughput

| Metric | WSE | Centrifugo | uWebSockets | Socket.IO | ws (Node.js) |
|--------|-----|------------|-------------|-----------|--------------|
| **Inbound sustained** | **20.5M msg/s** (Rust client) | Not published | Not published | Not published | 19K echo RT/s |
| **Inbound sustained** | **2.0M msg/s** (Python 64w) | -- | -- | -- | -- |
| **Single client** | **113K msg/s** | -- | -- | ~10K | ~19K echo |
| **Fan-out (broadcast)** | N/A | 500K msg/s (20 pods) | 120K msg/s (4 cores) | 30K msg/s | 13K (i7) |
| **Connection count** | **100K tested** | 1M (K8s cluster) | 120K (4 cores) | 30K (4 cores) | 100K+ |
| **Connection latency** | **0.53 ms** (single) | Not published | Not published | ~50 ms | ~10 ms |
| **Ping RTT** | **0.09 ms** (single) | Not published | Not published | ~1 ms | ~0.1 ms |

Sources: Centrifugo 1M blog, ezioda004 uWS vs Socket.IO benchmark (AWS c5a.xlarge),
Hashrocket WebSocket Shootout (i7-4790K), Lemire ws benchmark (Xeon Gold).

### Features

| Feature | WSE | Centrifugo | uWebSockets | Socket.IO | ws |
|---------|-----|------------|-------------|-----------|-----|
| Language | Python + Rust | Go | C++ | Node.js | Node.js |
| JWT auth in handshake | Rust (0.01ms) | Go | No | No | No |
| Wire formats | JSON + MsgPack | JSON + Protobuf | JSON | JSON | JSON |
| Compression | zlib (Rust) | -- | per-msg deflate | per-msg deflate | per-msg deflate |
| Priority queue | 5-level (Rust) | No | No | No | No |
| Circuit breaker | Client + Server | No | No | No | No |
| Offline queue | IndexedDB | No | No | No | No |
| E2E encryption | AES-GCM-256 + ECDH | No | No | No | No |
| Message signing | HMAC-SHA256 | HMAC-SHA256 | No | No | No |
| Message ordering | Sequence + gap detect | Sequence numbers | No | No | No |
| Dead letter queue | Redis-backed | No | No | No | No |
| Multi-instance | Redis Pub/Sub | Redis/Nats/Tarantool | Manual | Redis adapter | Manual |
| Health monitoring | Quality scoring | No | No | No | No |
| React integration | useWSE hook + Zustand | JS client | No | React adapter | No |

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
| **Rust client (JSON)** | **13,500,000 msg/s** | -- | **6.6x vs Python** |
| **Rust client (compressed)** | **20,500,000 msg/s** | -- | **10x vs Python** |

### Python Optimization Highlights

1. **orjson** (3-5x faster JSON) — replaced stdlib `json.dumps`/`json.loads`
2. **Log level downgrade** — hot-path INFO to DEBUG (eliminated disk I/O per message)
3. **deque(maxlen)** — replaced `list` + `pop(0)` with O(1) eviction
4. **OrderedDict dedup** — FIFO duplicate tracking without linear scan
5. **Priority queue reduction** — 10K to 1K capacity (5x memory savings per connection)

### Rust Acceleration Highlights

1. **Rust JWT in handshake** — 27x faster connection setup (GIL eliminated from critical path)
2. **flate2 compression** — 6.7x faster than Python zlib
3. **AHashSet dedup** — 27x faster duplicate detection
4. **BinaryHeap priority queue** — 25x faster message ordering
5. **Zero-copy transforms** — 15x faster event envelope construction
6. **Atomic rate limiter** — 40x faster token bucket
7. **Large message path** — 5x faster at 64KB (2.7 GB/s single client)
8. **Inbound msgpack parsing** — binary frames parsed in Rust via rmpv (zero Python overhead)

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
python benchmarks/bench_single_client.py
python benchmarks/bench_multiprocess.py --workers 64
python benchmarks/bench_brutal.py --connections 1000 --duration 5
```

Rust client:
```bash
cd benchmarks/rust-bench && cargo build --release
./target/release/wse-bench --host 127.0.0.1 --port 5006 \
  --secret "bench-secret-key-for-testing-only" \
  --tiers 100,500,1000,5000,10000,50000,100000 --duration 10
```

---

*Tested February 2026. WSE v1.3.8, wse-bench v0.1.0.*
