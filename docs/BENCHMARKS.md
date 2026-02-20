# WSE Performance Benchmarks

## Test Environment

- **Hardware**: Apple M3 Max (16 cores)
- **OS**: macOS 15.3.2
- **Python**: 3.14
- **Rust**: 1.84+ (PyO3 0.23, maturin 1.8)
- **Server**: FastAPI + Granian (ASGI)
- **Network**: localhost (127.0.0.1)
- **Single connection**: Benchmarks measure single-client throughput
- **Compression**: Disabled for throughput tests (raw protocol performance)

---

## Rust-Accelerated Results

### Burst Throughput

| Mode | Messages | Time | Rate | Per Message |
|------|----------|------|------|-------------|
| **Rust binary (msgpack)** | 10,000 | 0.009 s | **1,100,000 msg/s** | 0.0009 ms |
| **Rust JSON** | 10,000 | 0.035 s | **285,000 msg/s** | 0.0035 ms |
| Pure Python JSON | 1,000 | 0.009 s | 106,000 msg/s | 0.009 ms |

### Sustained Throughput (60s)

| Mode | Rate | Latency (p50) | Latency (p99) |
|------|------|---------------|---------------|
| **Rust binary** | **285,000 msg/s** | 0.009 ms | 0.04 ms |
| **Rust JSON** | **180,000 msg/s** | 0.03 ms | 0.15 ms |
| Pure Python JSON | 85,000 msg/s | 0.09 ms | 0.4 ms |

### Rust Acceleration by Component

| Component | Pure Python | Rust | Speedup |
|-----------|------------|------|---------|
| Compression (1KB zlib) | 12 us | 1.8 us | **6.7x** |
| Event transform | 45 us | 3 us | **15x** |
| Sequence + dedup | 8 us | 0.3 us | **27x** |
| Filter match | 15 us | 1.2 us | **12.5x** |
| Priority queue (enqueue) | 5 us | 0.2 us | **25x** |
| Rate limiter (check) | 2 us | 0.05 us | **40x** |
| HMAC-SHA256 sign | 18 us | 0.8 us | **22x** |

---

## Pure Python Baseline Results

### Test 1: Connection Latency (20 rounds)

Time from WebSocket connect to receiving `server_ready` message (includes TCP handshake, HTTP upgrade, JWT auth, subscription setup, and snapshot delivery).

| Metric | Value |
|--------|-------|
| Mean | 23.12 ms |
| Median | 21.81 ms |
| p95 | 34.82 ms |
| Min | 18.08 ms |
| Max | 45.28 ms |

### Test 2: Ping/Pong RTT (100 rounds)

WebSocket-level ping/pong round-trip time.

| Metric | Value |
|--------|-------|
| Mean | 0.14 ms |
| Median | 0.11 ms |
| p95 | 0.19 ms |
| p99 | 0.38 ms |
| Min | 0.08 ms |
| Max | 1.82 ms |

### Test 3: Client-to-Server Throughput (1000 messages)

Raw message send rate from client to server (small JSON messages).

| Metric | Value |
|--------|-------|
| Messages | 1,000 |
| Time | 0.009 s |
| Rate | **106,180 msg/sec** |
| Per message | 0.009 ms |

### Test 4: Rapid Connect/Disconnect (10 sequential)

Sequential connect -> receive first message -> disconnect cycles.

| Metric | Value |
|--------|-------|
| Connections | 10/10 |
| Mean | 21.49 ms |
| Median | 20.29 ms |
| Min | 16.28 ms |
| Max | 28.24 ms |
| Total | 214 ms |

### Test 5: Message Size Impact (200 messages per size)

Throughput and bandwidth at different message sizes.

| Size | Rate | Bandwidth | Per Message |
|------|------|-----------|-------------|
| 93 B | 94,832 msg/s | 8.4 MB/s | 0.011 ms |
| 285 B | 107,508 msg/s | 29.2 MB/s | 0.009 ms |
| 1,053 B | 105,882 msg/s | 106.4 MB/s | 0.009 ms |
| 4,125 B | 82,248 msg/s | 323.5 MB/s | 0.012 ms |
| 16,413 B | 27,624 msg/s | 432.2 MB/s | 0.036 ms |
| 65,565 B | 8,229 msg/s | 514.3 MB/s | 0.122 ms |

Key observations:
- **No throughput cliff at 4 KB** (common with other WebSocket libraries)
- Linear bandwidth scaling up to 64 KB
- Peak bandwidth: 514 MB/s at 64 KB messages

---

## Comparison with Alternatives

| Feature | WSE (Rust) | WSE (Python) | Socket.IO | ws (Node.js) | Pusher |
|---------|-----------|-------------|-----------|--------------|--------|
| Throughput (msg/s) | **1.1M** | 106K | ~25K | ~50K | N/A |
| Connection latency | 23 ms | 23 ms | ~50 ms | ~10 ms | ~100 ms |
| Ping RTT | 0.11 ms | 0.11 ms | ~1 ms | ~0.1 ms | N/A |
| Compression | zlib (Rust) | zlib + msgpack | per-msg deflate | per-msg deflate | N/A |
| Priority queue | 5-level (Rust) | 5-level | No | No | No |
| Circuit breaker | Yes | Yes | No | No | No |
| Offline queue | IndexedDB | IndexedDB | No | No | No |
| Multi-instance | Redis Pub/Sub | Redis Pub/Sub | Redis adapter | Manual | Built-in |
| E2E encryption | AES-GCM-256 | AES-GCM-256 | No | No | No |
| Message signing | HMAC-SHA256 | HMAC-SHA256 | No | No | No |

---

## Performance Optimization History

| Phase | Throughput | Improvement |
|-------|-----------|-------------|
| Initial (unoptimized) | 34,000 msg/s | Baseline |
| After Python optimization (9 fixes) | 106,000 msg/s | **3.1x** |
| After Rust acceleration | 1,100,000 msg/s | **10.4x** (vs Python optimized) |

### Python Optimization Highlights

1. **orjson** (3-5x faster JSON): Replaced stdlib `json.dumps`/`json.loads`
2. **Log level downgrade**: Hot-path `INFO` to `DEBUG` (eliminated disk I/O per message)
3. **deque(maxlen)**: Replaced `list` + `pop(0)` with O(1) eviction
4. **OrderedDict dedup**: FIFO duplicate tracking without linear scan
5. **Priority queue reduction**: 10K to 1K capacity (5x memory savings per connection)

### Rust Acceleration Highlights

1. **flate2 compression**: 6.7x faster than Python zlib
2. **AHashSet dedup**: 27x faster duplicate detection
3. **BinaryHeap priority queue**: 25x faster message ordering
4. **Zero-copy transforms**: 15x faster event envelope construction
5. **Atomic rate limiter**: 40x faster token bucket

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
