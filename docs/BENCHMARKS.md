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

## v1.2 Results — Single Client

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

### Single Client Throughput

| Test | v1.2 Rate | v1.0 Rate | Improvement |
|------|----------|----------|-------------|
| **Sequential (50K msgs)** | **124,352 msg/s** | 106,180 msg/s | 1.17x |
| **Sustained JSON (10s)** | **113,000 msg/s** | ~85,000 msg/s | 1.33x |
| **Sustained MsgPack (10s)** | **116,000 msg/s** | N/A | — |
| **Concurrent (50 senders x 1K)** | **115,464 msg/s** | N/A | — |
| **Binary 1KB** | **99,565 msg/s** | N/A | — |

### Large Message Performance (Single Client)

| Size | v1.2 Rate | v1.2 Bandwidth | v1.0 Rate | v1.0 Bandwidth | Improvement |
|------|----------|----------------|----------|----------------|-------------|
| 64 KB | **43,356 msg/s** | **2.7 GB/s** | 8,229 msg/s | 514 MB/s | **5.3x** |
| 16 KB | **64,163 msg/s** | **1.0 GB/s** | 27,624 msg/s | 432 MB/s | 2.3x |
| 4 KB | **75,932 msg/s** | **299 MB/s** | 82,248 msg/s | 324 MB/s | 0.9x |
| 1 KB | **106,641 msg/s** | **107 MB/s** | 105,882 msg/s | 106 MB/s | 1.01x |

### Rapid Connect/Disconnect

50 connect-authenticate-disconnect cycles.

| Metric | v1.2 | v1.0 | Improvement |
|--------|------|------|-------------|
| **Mean** | **0.58 ms** | 21.49 ms | **37x** |
| Median | 0.51 ms | 20.29 ms | 40x |
| Min | 0.38 ms | 16.28 ms | 43x |
| Max | 1.23 ms | 28.24 ms | 23x |

---

## v1.2 Results — Multi-Process (10 Workers)

10 independent Python processes, each with its own WebSocket connection sending concurrently.
Measures aggregate server throughput under parallel load.

### Aggregate Throughput

| Test | JSON | MsgPack |
|------|------|---------|
| **Burst (100K msgs)** | **318,000 msg/s** | **311,000 msg/s** |
| **Sustained (5s)** | **356,000 msg/s** | **345,000 msg/s** |

### JSON Burst: Up to 0.5M msg/s

Small message (93 bytes) throughput with 10 workers — approaches half a million messages per second:

| Size | Rate | Bandwidth | Workers |
|------|------|-----------|---------|
| 93 B | **488,457 msg/s** | 43 MB/s | 10/10 |
| 285 B | 479,581 msg/s | 130 MB/s | 10/10 |
| 1 KB | 455,190 msg/s | 457 MB/s | 10/10 |
| 4 KB | 407,345 msg/s | 1.6 GB/s | 10/10 |
| 16 KB | 243,457 msg/s | 3.8 GB/s | 10/10 |
| 64 KB | **163,912 msg/s** | **10.2 GB/s** | 10/10 |

### Sustained JSON (10 workers x 5s)

1.78M total messages, stddev 3.3%.

| Second | Rate |
|--------|------|
| 1 | 344,193 msg/s |
| 2 | 343,709 msg/s |
| 3 | 366,249 msg/s |
| 4 | 365,605 msg/s |
| 5 | 365,375 msg/s |
| **Average** | **357,026 msg/s** |
| **Stddev** | **11,941 msg/s (3.3%)** |

### Sustained MsgPack (10 workers x 5s)

1.72M total messages, stddev 3.9%. Msgpack parsed in Rust (zero Python overhead).

| Second | Rate |
|--------|------|
| 1 | 348,812 msg/s |
| 2 | 354,235 msg/s |
| 3 | 359,253 msg/s |
| 4 | 325,851 msg/s |
| 5 | 336,832 msg/s |
| **Average** | **344,997 msg/s** |
| **Stddev** | **13,568 msg/s (3.9%)** |

### Multi-Process Connection Latency (10 workers x 5 rounds)

| Metric | Value |
|--------|-------|
| Mean | 8.95 ms |
| Median | 2.20 ms |
| p95 | 35.02 ms |
| Min | 1.35 ms |

### Multi-Process Ping RTT (10 workers x 20 pings)

| Metric | Value |
|--------|-------|
| Mean | 0.33 ms |
| Median | 0.18 ms |
| p95 | 0.90 ms |
| Min | 0.10 ms |

---

## v1.2 Results — AMD EPYC 7502P (64 cores, 128 GB)

**Hardware**: AMD EPYC 7502P, 64 cores / 128 threads, 128 GB RAM, Ubuntu 24.04.
Minimal benchmark server (Rust WSE + JWT auth, no FastAPI/DB overhead).

### Worker Scaling Summary

| Workers | Sustained JSON | Sustained MsgPack | Burst (93B) | Per Worker | Stddev |
|---------|---------------|-------------------|-------------|------------|--------|
| **64** | **2,045K msg/s** | **2,072K msg/s** | 1,557K | 31,943 | 0.3% |
| 80 | 2,014K msg/s | 2,036K msg/s | 1,675K | 25,143 | 0.3% |
| 128 | 2,013K msg/s | 2,041K msg/s | 1,836K | 15,662 | 0.7% |

**2M msg/s is the single-server ceiling.** 64 workers = optimal for sustained throughput.

### Connection Latency (64 workers x 5 rounds)

| Metric | Value |
|--------|-------|
| Mean | 10.69 ms |
| **Median** | **2.60 ms** |
| p95 | 25.35 ms |
| p99 | 48.15 ms |
| Min | 0.65 ms |

### Ping/Pong RTT (64 connections x 20 pings)

| Metric | Value |
|--------|-------|
| Mean | 0.29 ms |
| **Median** | **0.26 ms** |
| p95 | 0.52 ms |
| Min | 0.10 ms |

### Message Size Impact (64 workers)

| Size | Rate | Bandwidth | Workers |
|------|------|-----------|---------|
| 93 B | **1,557K msg/s** | 138 MB/s | 64/64 |
| 285 B | **1,678K msg/s** | 456 MB/s | 64/64 |
| 1 KB | **1,382K msg/s** | 1.4 GB/s | 64/64 |
| 4 KB | **1,214K msg/s** | 4.8 GB/s | 64/64 |
| 16 KB | **732K msg/s** | 11.5 GB/s | 64/64 |
| 64 KB | **256K msg/s** | **16.0 GB/s** | 64/64 |

### Sustained JSON (64 workers x 5s)

10.2M total messages, stddev 0.3%.

| Second | Rate |
|--------|------|
| 1 | 2,038K msg/s |
| 2 | 2,048K msg/s |
| 3 | 2,041K msg/s |
| 4 | 2,043K msg/s |
| 5 | 2,055K msg/s |
| **Average** | **2,045K msg/s** |

### Sustained MsgPack (64 workers x 5s)

10.4M total messages, stddev 0.5%.

| Second | Rate |
|--------|------|
| 1 | 2,075K msg/s |
| 2 | 2,070K msg/s |
| 3 | 2,073K msg/s |
| 4 | 2,056K msg/s |
| 5 | 2,085K msg/s |
| **Average** | **2,072K msg/s** |

### M2 vs EPYC Comparison

| Metric | M2 (10w) | EPYC (64w) | Speedup |
|--------|---------|-----------|---------|
| Sustained JSON | 356K | **2,045K** | **5.7x** |
| Sustained MsgPack | 345K | **2,072K** | **6.0x** |
| Burst JSON | 488K | **1,836K** | **3.8x** |
| 64KB throughput | 10.2 GB/s | **16.0 GB/s** | **1.6x** |
| Connection latency | 2.20 ms | 2.60 ms | ~same |

---

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
| Msgpack inbound parse | ~50 us | ~1 us | **~50x** |

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

| Feature | WSE (Rust) | WSE (Python) | Socket.IO | ws (Node.js) |
|---------|-----------|-------------|-----------|--------------|
| Single client sustained | **113K msg/s** | ~85K | ~25K | ~50K |
| 10-worker sustained (M2) | **356K msg/s** | N/A | N/A | N/A |
| 64-worker sustained (EPYC) | **2,045K msg/s** | N/A | N/A | N/A |
| Burst (M2, 10w) | **488K msg/s** | N/A | N/A | N/A |
| Burst (EPYC, 128w) | **1,836K msg/s** | N/A | N/A | N/A |
| Connection latency | **0.53 ms** | 23 ms | ~50 ms | ~10 ms |
| 64KB throughput (EPYC, 64w) | **16.0 GB/s** | 514 MB/s | N/A | N/A |
| Ping RTT | 0.09 ms | 0.11 ms | ~1 ms | ~0.1 ms |
| JWT auth | Rust (0.01ms) | Python | N/A | N/A |
| Wire formats | JSON + MsgPack | JSON + MsgPack | JSON | JSON |
| Compression | zlib (Rust) | zlib + msgpack | per-msg deflate | per-msg deflate |
| Priority queue | 5-level (Rust) | 5-level | No | No |
| Circuit breaker | Yes | Yes | No | No |
| Offline queue | IndexedDB | IndexedDB | No | No |
| Multi-instance | Redis Pub/Sub | Redis Pub/Sub | Redis adapter | Manual |
| E2E encryption | AES-GCM-256 | AES-GCM-256 | No | No |
| Message signing | HMAC-SHA256 | HMAC-SHA256 | No | No |

---

## Performance Optimization History

| Phase | Throughput | Connection Latency | Improvement |
|-------|-----------|-------------------|-------------|
| Initial (unoptimized) | 34,000 msg/s | ~50 ms | Baseline |
| After Python optimization (9 fixes) | 106,000 msg/s | 23 ms | **3.1x** throughput |
| After Rust acceleration (v1.0) | 113,000 msg/s | 23 ms | **1.07x** throughput |
| After Rust JWT + transport (v1.2) | 113,000 msg/s | **0.53 ms** | **27x** latency, **5x** large msg |
| Multi-process (10 workers, M2) | **356,000 msg/s** sustained | 2.20 ms median | **3.1x** vs single client |
| Multi-process burst (M2) | **488,000 msg/s** | — | **~0.5M msg/s** |
| **EPYC 7502P (64 workers)** | **2,045,000 msg/s** sustained | 2.60 ms median | **5.7x** vs M2 10w |
| **EPYC 7502P (64w, MsgPack)** | **2,072,000 msg/s** sustained | — | **~2M msg/s** |

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
7. **Large message path**: 5x faster at 64KB (2.7 GB/s single client, 10.2 GB/s 10 workers)
8. **Inbound msgpack parsing**: Binary frames parsed in Rust via rmpv (zero Python overhead)

---

## Benchmark Methodology

### Tool

Custom Python benchmark using `websockets` library and `httpx` for authentication.

```bash
# Start the server
python -m server

# Run benchmarks (in another terminal)
python benchmarks/bench_single_client.py
python benchmarks/bench_multiprocess.py --workers 10
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
| Connection latency | 50 rounds (single), 10x5 rounds (multi) |
| Ping RTT | 500 rounds (single), 10x20 (multi) |
| Sequential throughput | 50,000 (single), 100,000 (multi) |
| Rapid connect | 50 cycles (single), 10x5 (multi) |
| Message sizes | 10,000-20,000 per size (single), 5,000 per size (multi) |
| Binary frames | 10,000 (single), 50,000 (multi) |
| Sustained load | 10s (single), 5s (multi) |

### Reproducibility

All benchmarks run on localhost to eliminate network variance.
Each test runs multiple rounds and reports percentile statistics.
Results are deterministic within ~5% variance across runs.

The 10-worker count was chosen as the optimal sweet spot for Apple M2:
fewer workers underutilize tokio parallelism, more workers add contention
(12+ workers showed higher stddev and lower per-worker throughput).
