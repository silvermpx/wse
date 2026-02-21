# WSE Performance Benchmarks

## Test Environment

- **Hardware**: Apple M3 Max (16 cores)
- **OS**: macOS 15.3.2
- **Python**: 3.14
- **Rust**: 1.84+ (PyO3 0.28, maturin 1.8)
- **Server**: FastAPI + Granian (ASGI)
- **Network**: localhost (127.0.0.1)
- **Compression**: Disabled for throughput tests (raw protocol performance)

---

## v1.2 Results (Rust Transport + JWT)

### Connection Latency

Time from WebSocket connect to receiving `server_ready` (includes TCP handshake, HTTP upgrade, JWT validation, connection registration).

| Metric | v1.2 (Rust JWT) | v1.0 (Python JWT) | Improvement |
|--------|----------------|-------------------|-------------|
| **Mean** | **1.25 ms** | 23.12 ms | **19x** |
| **Median** | **0.47 ms** | 21.81 ms | **46x** |
| p95 | 15.33 ms | 34.82 ms | 2.3x |
| Min | 0.29 ms | 18.08 ms | 62x |

### Ping/Pong RTT

| Metric | v1.2 | v1.0 |
|--------|------|------|
| **Mean** | **0.17 ms** | 0.14 ms |
| **Median** | **0.14 ms** | 0.11 ms |
| p95 | 0.32 ms | 0.19 ms |
| p99 | 0.56 ms | 0.38 ms |

### Throughput

| Test | v1.2 Rate | v1.0 Rate | Improvement |
|------|----------|----------|-------------|
| **Burst (5000 msgs)** | **121,654 msg/s** | 106,180 msg/s | 1.15x |
| **Sustained (5s)** | **132,013 msg/s** | ~85,000 msg/s | 1.55x |
| **Concurrent (50 senders)** | **126,481 msg/s** | N/A | — |
| **Binary 1KB** | **102,459 msg/s** | N/A | — |

### Large Message Performance

| Size | v1.2 Rate | v1.2 Bandwidth | v1.0 Rate | v1.0 Bandwidth | Improvement |
|------|----------|----------------|----------|----------------|-------------|
| 64 KB | **40,604 msg/s** | **2.5 GB/s** | 8,229 msg/s | 514 MB/s | **5x** |
| 16 KB | 75,187 msg/s | 1.2 GB/s | 27,624 msg/s | 432 MB/s | 2.7x |
| 4 KB | 98,231 msg/s | 385 MB/s | 82,248 msg/s | 324 MB/s | 1.2x |
| 1 KB | 121,654 msg/s | 116 MB/s | 105,882 msg/s | 106 MB/s | 1.15x |

### Rapid Connect/Disconnect

| Metric | v1.2 | v1.0 | Improvement |
|--------|------|------|-------------|
| **Mean** | **1.16 ms** | 21.49 ms | **19x** |
| Median | 0.82 ms | 20.29 ms | 25x |
| Min | 0.41 ms | 16.28 ms | 40x |
| Max | 3.89 ms | 28.24 ms | 7x |

---

## Burst Throughput (Rust Engine)

| Mode | Messages | Time | Rate | Per Message |
|------|----------|------|------|-------------|
| **Rust binary (msgpack)** | 10,000 | 0.009 s | **1,000,000 msg/s** | 0.0009 ms |
| **Rust JSON** | 10,000 | 0.035 s | **285,000 msg/s** | 0.0035 ms |
| Pure Python JSON | 1,000 | 0.009 s | 106,000 msg/s | 0.009 ms |

## Sustained Throughput (Rust Engine, 60s)

| Mode | Rate | Latency (p50) | Latency (p99) |
|------|------|---------------|---------------|
| **Rust binary** | **285,000 msg/s** | 0.009 ms | 0.04 ms |
| **Rust JSON** | **180,000 msg/s** | 0.03 ms | 0.15 ms |
| Pure Python JSON | 85,000 msg/s | 0.09 ms | 0.4 ms |

## Rust Acceleration by Component

| Component | Pure Python | Rust | Speedup |
|-----------|------------|------|---------|
| JWT decode (HS256) | 850 us | 10 us | **85x** |
| Compression (1KB zlib) | 12 us | 1.8 us | **6.7x** |
| Event transform | 45 us | 3 us | **15x** |
| Sequence + dedup | 8 us | 0.3 us | **27x** |
| Filter match | 15 us | 1.2 us | **12.5x** |
| Priority queue (enqueue) | 5 us | 0.2 us | **25x** |
| Rate limiter (check) | 2 us | 0.05 us | **40x** |
| HMAC-SHA256 sign | 18 us | 0.8 us | **22x** |

---

## Pure Python Baseline Results (v1.0)

### Connection Latency (20 rounds)

| Metric | Value |
|--------|-------|
| Mean | 23.12 ms |
| Median | 21.81 ms |
| p95 | 34.82 ms |
| Min | 18.08 ms |
| Max | 45.28 ms |

### Ping/Pong RTT (100 rounds)

| Metric | Value |
|--------|-------|
| Mean | 0.14 ms |
| Median | 0.11 ms |
| p95 | 0.19 ms |
| p99 | 0.38 ms |

### Message Size Impact (200 messages per size)

| Size | Rate | Bandwidth | Per Message |
|------|------|-----------|-------------|
| 93 B | 94,832 msg/s | 8.4 MB/s | 0.011 ms |
| 285 B | 107,508 msg/s | 29.2 MB/s | 0.009 ms |
| 1,053 B | 105,882 msg/s | 106.4 MB/s | 0.009 ms |
| 4,125 B | 82,248 msg/s | 323.5 MB/s | 0.012 ms |
| 16,413 B | 27,624 msg/s | 432.2 MB/s | 0.036 ms |
| 65,565 B | 8,229 msg/s | 514.3 MB/s | 0.122 ms |

---

## Comparison with Alternatives

| Feature | WSE (Rust) | WSE (Python) | Socket.IO | ws (Node.js) | Pusher |
|---------|-----------|-------------|-----------|--------------|--------|
| Throughput (msg/s) | **1M** | 106K | ~25K | ~50K | N/A |
| Connection latency | **0.47 ms** | 23 ms | ~50 ms | ~10 ms | ~100 ms |
| 64KB throughput | **40K msg/s** | 8K msg/s | ~2K | ~5K | N/A |
| Ping RTT | 0.14 ms | 0.11 ms | ~1 ms | ~0.1 ms | N/A |
| JWT auth | Rust (0.01ms) | Python | N/A | N/A | N/A |
| Compression | zlib (Rust) | zlib + msgpack | per-msg deflate | per-msg deflate | N/A |
| Priority queue | 5-level (Rust) | 5-level | No | No | No |
| Circuit breaker | Yes | Yes | No | No | No |
| Offline queue | IndexedDB | IndexedDB | No | No | No |
| Multi-instance | Redis Pub/Sub | Redis Pub/Sub | Redis adapter | Manual | Built-in |
| E2E encryption | AES-GCM-256 | AES-GCM-256 | No | No | No |
| Message signing | HMAC-SHA256 | HMAC-SHA256 | No | No | No |

---

## Performance Optimization History

| Phase | Throughput | Connection Latency | Improvement |
|-------|-----------|-------------------|-------------|
| Initial (unoptimized) | 34,000 msg/s | ~50 ms | Baseline |
| After Python optimization (9 fixes) | 106,000 msg/s | 23 ms | **3.1x** throughput |
| After Rust acceleration (v1.0) | 1,000,000 msg/s | 23 ms | **10.4x** throughput |
| After Rust JWT + transport (v1.2) | 1,000,000 msg/s | **0.47 ms** | **19x** latency, **5x** large msg |

### Python Optimization Highlights

1. **orjson** (3-5x faster JSON): Replaced stdlib `json.dumps`/`json.loads`
2. **Log level downgrade**: Hot-path `INFO` to `DEBUG` (eliminated disk I/O per message)
3. **deque(maxlen)**: Replaced `list` + `pop(0)` with O(1) eviction
4. **OrderedDict dedup**: FIFO duplicate tracking without linear scan
5. **Priority queue reduction**: 10K to 1K capacity (5x memory savings per connection)

### Rust Acceleration Highlights

1. **Rust JWT in handshake**: 19x faster connection setup (GIL eliminated from critical path)
2. **flate2 compression**: 6.7x faster than Python zlib
3. **AHashSet dedup**: 27x faster duplicate detection
4. **BinaryHeap priority queue**: 25x faster message ordering
5. **Zero-copy transforms**: 15x faster event envelope construction
6. **Atomic rate limiter**: 40x faster token bucket
7. **Large message path**: 5x faster at 64KB (2.5 GB/s bandwidth)

---

## Benchmark Methodology

### Tool

Custom Python benchmark using `websockets` library and `httpx` for authentication.

Location: `benchmarks/bench_wse.py`

```bash
# Start the server
python -m server

# Run benchmarks (in another terminal)
python benchmarks/bench_wse.py --host 127.0.0.1 --port 5005
```

### Protocol

1. Authenticate via HTTP to get JWT token
2. Connect to `ws://host:port/wse?token=<JWT>&compression=false`
3. Wait for `server_ready` message
4. Run test-specific workload
5. Collect timing statistics

### Message Decoding

The benchmark handles the WSE wire protocol:
- Strips `WSE`/`S`/`U` category prefixes
- Decompresses `C:` prefixed messages (zlib)
- Parses JSON payloads

### Reproducibility

All benchmarks run on localhost to eliminate network variance.
Each test runs multiple rounds and reports percentile statistics.
Results are deterministic within ~5% variance across runs.
