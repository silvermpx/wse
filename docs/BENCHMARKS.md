# WSE Performance Benchmarks

## Test Environment

- **Hardware**: Apple M2
- **OS**: macOS 15.3.2
- **Python**: 3.14
- **Rust**: 1.84+ (PyO3 0.28, maturin 1.8)
- **Server**: FastAPI + Granian (ASGI)
- **Network**: localhost (127.0.0.1)
- **Compression**: Disabled for throughput tests (raw protocol performance)

---

## v1.2 Results (Rust Transport + JWT)

### Connection Latency

Time from WebSocket connect to receiving `server_ready` (includes TCP handshake, HTTP upgrade, JWT validation, connection registration). 50 rounds.

| Metric | v1.2 (Rust JWT) | v1.0 (Python JWT) | Improvement |
|--------|----------------|-------------------|-------------|
| **Mean** | **0.87 ms** | 23.12 ms | **27x** |
| **Median** | **0.53 ms** | 21.81 ms | **41x** |
| p95 | 1.10 ms | 34.82 ms | 32x |
| p99 | 14.64 ms | 45.28 ms | 3x |
| Min | 0.44 ms | 18.08 ms | 41x |

### Ping/Pong RTT

500 rounds on a single persistent connection.

| Metric | v1.2 | v1.0 |
|--------|------|------|
| **Mean** | **0.10 ms** | 0.14 ms |
| **Median** | **0.09 ms** | 0.11 ms |
| p95 | 0.19 ms | 0.19 ms |
| p99 | 0.39 ms | 0.38 ms |

### Throughput

| Test | v1.2 Rate | v1.0 Rate | Improvement |
|------|----------|----------|-------------|
| **Sequential (50K msgs)** | **124,352 msg/s** | 106,180 msg/s | 1.17x |
| **Sustained (10s)** | **128,844 msg/s** | ~85,000 msg/s | 1.52x |
| **Concurrent (50 senders x 1K)** | **129,867 msg/s** | N/A | — |
| **Binary 1KB** | **112,924 msg/s** | N/A | — |

### Large Message Performance

| Size | v1.2 Rate | v1.2 Bandwidth | v1.0 Rate | v1.0 Bandwidth | Improvement |
|------|----------|----------------|----------|----------------|-------------|
| 64 KB | **39,267 msg/s** | **2.5 GB/s** | 8,229 msg/s | 514 MB/s | **4.8x** |
| 16 KB | **73,565 msg/s** | **1.2 GB/s** | 27,624 msg/s | 432 MB/s | 2.7x |
| 4 KB | **105,032 msg/s** | **413 MB/s** | 82,248 msg/s | 324 MB/s | 1.3x |
| 1 KB | **113,906 msg/s** | **114 MB/s** | 105,882 msg/s | 106 MB/s | 1.08x |

### Rapid Connect/Disconnect

50 connect-authenticate-disconnect cycles.

| Metric | v1.2 | v1.0 | Improvement |
|--------|------|------|-------------|
| **Mean** | **0.58 ms** | 21.49 ms | **37x** |
| Median | 0.51 ms | 20.29 ms | 40x |
| Min | 0.38 ms | 16.28 ms | 43x |
| Max | 1.23 ms | 28.24 ms | 23x |

---

## Burst Throughput (Rust Engine)

| Mode | Messages | Time | Rate | Per Message |
|------|----------|------|------|-------------|
| **Rust binary (msgpack)** | 10,000 | 0.009 s | **1,000,000 msg/s** | 0.0009 ms |
| **Rust JSON** | 10,000 | 0.035 s | **285,000 msg/s** | 0.0035 ms |
| Pure Python JSON | 1,000 | 0.009 s | 106,000 msg/s | 0.009 ms |

## Sustained Throughput (10s continuous send)

| Second | Rate |
|--------|------|
| 1 | 94,201 msg/s (warmup) |
| 2 | 132,681 msg/s |
| 3 | 120,251 msg/s |
| 4 | 135,315 msg/s |
| 5 | 134,859 msg/s |
| 6 | 133,560 msg/s |
| 7 | 133,036 msg/s |
| 8 | 134,492 msg/s |
| 9 | 134,486 msg/s |
| 10 | 135,556 msg/s |
| **Average** | **128,844 msg/s** |
| **Stddev** | **12,976 msg/s** |

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
| Connection latency | **0.53 ms** | 23 ms | ~50 ms | ~10 ms | ~100 ms |
| 64KB throughput | **39K msg/s** | 8K msg/s | ~2K | ~5K | N/A |
| Ping RTT | 0.09 ms | 0.11 ms | ~1 ms | ~0.1 ms | N/A |
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
| After Rust JWT + transport (v1.2) | 1,000,000 msg/s | **0.53 ms** | **27x** latency, **5x** large msg |

### Python Optimization Highlights

1. **orjson** (3-5x faster JSON): Replaced stdlib `json.dumps`/`json.loads`
2. **Log level downgrade**: Hot-path `INFO` to `DEBUG` (eliminated disk I/O per message)
3. **deque(maxlen)**: Replaced `list` + `pop(0)` with O(1) eviction
4. **OrderedDict dedup**: FIFO duplicate tracking without linear scan
5. **Priority queue reduction**: 10K to 1K capacity (5x memory savings per connection)

### Rust Acceleration Highlights

1. **Rust JWT in handshake**: 27x faster connection setup (GIL eliminated from critical path)
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

```bash
# Start the server
python -m server

# Run benchmarks (in another terminal)
python benchmarks/bench_single_client.py
python benchmarks/bench_multiprocess.py --workers 16
```

### Protocol

1. Authenticate via HTTP to get JWT token
2. Connect to `ws://host:port/wse` with `access_token` cookie
3. Wait for `server_ready` message (Rust JWT validation)
4. Drain buffered messages (snapshots, subscriptions)
5. Run test-specific workload
6. Collect timing statistics

### Message Counts

Tests use high message counts for statistical accuracy:

| Test | Messages |
|------|----------|
| Connection latency | 50 rounds |
| Ping RTT | 500 rounds |
| Sequential throughput | 50,000 |
| Rapid connect | 50 cycles |
| Message sizes (64B-1KB) | 10,000-20,000 per size |
| Message sizes (4KB-64KB) | 1,000-5,000 per size |
| Concurrent senders | 50 x 1,000 = 50,000 |
| Binary frames | 10,000 |
| Sustained load | 10 seconds continuous |

### Reproducibility

All benchmarks run on localhost to eliminate network variance.
Each test runs multiple rounds and reports percentile statistics.
Results are deterministic within ~5% variance across runs.
