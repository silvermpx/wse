# WSE Performance Benchmarks - Python Client

All results below were collected using Python benchmark scripts (`websockets` library).
Single-client tests use one async connection. Multi-process tests spawn one OS process per
connection with a tight synchronous `send()` loop - no asyncio overhead, true OS parallelism.

Server is always the production `RustWSEServer` (maturin-compiled binary, full Python wrapper,
JWT auth in handshake). No FastAPI or DB overhead.

---

## Hardware

Two environments tested:

| | Apple M2 | AMD EPYC 7502P |
|---|----------|----------------|
| Cores | 8 (4P + 4E) | 64 (128 threads) |
| RAM | 16 GB | 128 GB |
| OS | macOS 15.3.2 | Ubuntu 24.04 |
| Network | localhost | localhost |
| Python | 3.14 | 3.14 |

---

## Single Client (M2)

One WebSocket connection, sequential message sending.

### Connection Latency

Time from TCP connect to receiving `server_ready`. Includes TCP handshake, HTTP upgrade,
JWT validation (Rust), and connection registration. 50 rounds.

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
| **Sustained JSON (10s)** | **113,000 msg/s** | ~85,000 msg/s | 1.33x |
| **Sustained MsgPack (10s)** | **116,000 msg/s** | N/A | -- |
| **Concurrent (50 senders x 1K)** | **115,464 msg/s** | N/A | -- |
| **Binary 1KB** | **99,565 msg/s** | N/A | -- |

### Large Message Performance

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

## Multi-Process (M2, 10 Workers)

10 independent Python processes, each with its own WebSocket connection sending concurrently.

### Aggregate Throughput

| Test | JSON | MsgPack |
|------|------|---------|
| **Burst (100K msgs)** | **318,000 msg/s** | **311,000 msg/s** |
| **Sustained (5s)** | **356,000 msg/s** | **345,000 msg/s** |

### JSON Burst by Message Size

Small message (93 bytes) throughput approaches half a million messages per second:

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

### Sustained MsgPack (10 workers x 5s)

1.72M total messages, stddev 3.9%.

| Second | Rate |
|--------|------|
| 1 | 348,812 msg/s |
| 2 | 354,235 msg/s |
| 3 | 359,253 msg/s |
| 4 | 325,851 msg/s |
| 5 | 336,832 msg/s |
| **Average** | **344,997 msg/s** |

### Connection Latency (10 workers x 5 rounds)

| Metric | Value |
|--------|-------|
| Mean | 8.95 ms |
| Median | 2.20 ms |
| p95 | 35.02 ms |
| Min | 1.35 ms |

### Ping RTT (10 workers x 20 pings)

| Metric | Value |
|--------|-------|
| Mean | 0.33 ms |
| Median | 0.18 ms |
| p95 | 0.90 ms |
| Min | 0.10 ms |

---

## Multi-Process (EPYC 7502P, 64 Workers)

Same multi-process methodology, scaled to 64 threads (32 cores).

### Worker Scaling

| Workers | Sustained JSON | Sustained MsgPack | Burst (93B) | Per Worker | Stddev |
|---------|---------------|-------------------|-------------|------------|--------|
| **64** | **2,045K msg/s** | **2,072K msg/s** | 1,557K | 31,943 | 0.3% |
| 80 | 2,014K msg/s | 2,036K msg/s | 1,675K | 25,143 | 0.3% |
| 128 | 2,013K msg/s | 2,041K msg/s | 1,836K | 15,662 | 0.7% |

2M msg/s is the single-server ceiling with Python clients. 64 workers is optimal -
adding more workers just increases contention without improving throughput.

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

### M2 vs EPYC Comparison

| Metric | M2 (10w) | EPYC (64w) | Speedup |
|--------|---------|-----------|---------|
| Sustained JSON | 356K | **2,045K** | **5.7x** |
| Sustained MsgPack | 345K | **2,072K** | **6.0x** |
| Burst JSON | 488K | **1,836K** | **3.8x** |
| 64KB throughput | 10.2 GB/s | **16.0 GB/s** | **1.6x** |
| Connection latency | 2.20 ms | 2.60 ms | ~same |

---

## Brutal Concurrency (EPYC, v1.3.8)

Full concurrent stress test: 1 OS process per connection, tight synchronous `send()` loop.
194-byte JSON trading messages, 5 seconds per test. This is where we really pushed
the Python client to its limits.

**Server**: v1.3.8 with crossbeam-channel drain mode.

### drain_mode=OFF (Rust reads + parses + drops, no Python)

Raw Rust ingest ceiling - no queue, no GIL involvement.

#### Connection Storm

| Connections | Connected | Failed | Accept Rate | p50 | p95 | p99 |
|-------------|-----------|--------|-------------|-----|-----|-----|
| 100 | 100 | 0 | 3,113 conn/s | 23.3 ms | 27.3 ms | 28.7 ms |
| 500 | 500 | 0 | 2,441 conn/s | 49.3 ms | 65.1 ms | 71.0 ms |
| 1,000 | 1,000 | 0 | 2,328 conn/s | 47.2 ms | 59.3 ms | 66.8 ms |
| 5,000 | 5,000 | 0 | 2,071 conn/s | 52.3 ms | 102.9 ms | 150.9 ms |
| 10,000 | 10,000 | 0 | 1,688 conn/s | 52.7 ms | 249.8 ms | 288.5 ms |
| 15,000 | 15,000 | 0 | 1,307 conn/s | 53.1 ms | 322.3 ms | 604.9 ms |

Zero connection failures at any tier.

#### Ping/Pong Latency Under Load

| Connections | p50 | p95 | p99 | p99.9 | mean |
|-------------|-----|-----|-----|-------|------|
| 100 | 4.8 ms | 4.9 ms | 5.0 ms | 5.0 ms | 4.7 ms |
| 500 | 27.3 ms | 29.8 ms | 30.2 ms | 30.6 ms | 27.3 ms |
| 1,000 | 52.5 ms | 60.2 ms | 75.4 ms | 83.1 ms | 53.6 ms |
| 5,000 | 269.9 ms | 397.7 ms | 417.3 ms | 418.8 ms | 281.4 ms |
| 10,000 | 572.2 ms | 832.1 ms | 866.3 ms | 879.3 ms | 624.1 ms |
| 15,000 | 831.2 ms | 1,252.5 ms | 1,508.9 ms | 1,520.1 ms | 1,002.2 ms |

#### Throughput Saturation

| Connections | Total msg/s | Per-conn | MB/s | Errors |
|-------------|-------------|----------|------|--------|
| 100 | 2,328,879 | 23,289 | 430.9 | 0 |
| 500 | 3,818,479 | 7,637 | 706.5 | 0 |
| 1,000 | 6,924,403 | 6,924 | 1,281.1 | 0 |

Peak: **6.9M msg/s** at 1,000 connections. Zero errors.

#### Sustained Connection Hold (5s)

| Connections | Alive | Dropped | Pings OK |
|-------------|-------|---------|----------|
| 100 | 100/100 | 0 | 500 |
| 500 | 500/500 | 0 | 2,500 |
| 1,000 | 1,000/1,000 | 0 | 5,000 |

#### Serialization (1M iterations)

| Library | Encode (us) | Decode (us) | Enc ops/s | Dec ops/s | Bytes |
|---------|-------------|-------------|-----------|-----------|-------|
| json | 4.171 | 3.254 | 239,717 | 307,316 | 194 |
| orjson | 0.455 | 1.130 | 2,199,561 | 884,835 | 175 |
| msgpack | 1.457 | 1.543 | 686,439 | 648,176 | 143 |

orjson vs json: encode **9.2x**, decode **2.9x**. msgpack: **27% smaller** than JSON.

#### Message Size Impact (64 connections per size)

| Size | Actual | Total msg/s | MB/s | GB/s | Errors |
|------|--------|-------------|------|------|--------|
| 64B | 94B | 2,541,218 | 227.8 | 0.22 | 0 |
| 256B | 286B | 2,325,453 | 634.3 | 0.62 | 0 |
| 1KB | 1054B | 2,108,816 | 2,119.7 | 2.07 | 0 |
| 4KB | 4126B | 1,611,037 | 6,339.1 | 6.19 | 0 |
| 16KB | 16414B | 866,834 | 13,568.8 | 13.25 | 0 |
| 64KB | 65566B | 249,312 | 15,590.4 | 15.22 | 0 |

Peak bandwidth: **15.2 GB/s** at 64KB messages.

#### Wire Format Throughput (500 connections)

| Format | Payload | Total msg/s | Per-conn | MB/s | Errors |
|--------|---------|-------------|----------|------|--------|
| JSON (text) | 194B | 4,230,770 | 8,462 | 782.7 | 0 |
| msgpack (M:) | 145B | 4,289,127 | 8,578 | 593.1 | 0 |

msgpack: **1.4% faster**, **24% less bandwidth** at same throughput.

---

### drain_mode=ON (production path, Rust reads + queues + Python drains)

Production path: messages go through crossbeam-channel to Python drain thread.
v1.3.8 introduced crossbeam-channel, replacing the old Mutex<VecDeque> which was
the primary bottleneck in v1.3.6.

#### Connection Storm

| Connections | Connected | Failed | Accept Rate | p50 | p95 | p99 |
|-------------|-----------|--------|-------------|-----|-----|-----|
| 100 | 100 | 0 | 3,073 conn/s | 23.9 ms | 28.3 ms | 30.1 ms |
| 500 | 500 | 0 | 2,317 conn/s | 53.6 ms | 68.0 ms | 70.1 ms |
| 1,000 | 1,000 | 0 | 2,339 conn/s | 50.4 ms | 62.5 ms | 77.6 ms |
| 5,000 | 5,000 | 0 | 2,005 conn/s | 53.3 ms | 109.6 ms | 156.5 ms |
| 10,000 | 10,000 | 0 | 1,695 conn/s | 52.4 ms | 251.2 ms | 286.0 ms |
| 15,000 | 15,000 | 0 | 1,290 conn/s | 52.2 ms | 309.3 ms | 606.2 ms |

Identical to drain=OFF. Zero failures.

#### Ping/Pong Latency Under Load

| Connections | p50 | p95 | p99 | p99.9 | mean |
|-------------|-----|-----|-----|-------|------|
| 100 | 4.6 ms | 4.7 ms | 4.8 ms | 4.8 ms | 4.6 ms |
| 500 | 27.5 ms | 29.9 ms | 30.6 ms | 30.9 ms | 27.6 ms |
| 1,000 | 53.1 ms | 62.8 ms | 81.7 ms | 86.7 ms | 54.0 ms |
| 5,000 | 272.7 ms | 420.9 ms | 423.9 ms | 424.5 ms | 286.9 ms |
| 10,000 | 563.3 ms | 843.4 ms | 884.3 ms | 889.9 ms | 638.2 ms |
| 15,000 | 845.3 ms | 1,259.2 ms | 1,512.4 ms | 1,522.5 ms | 1,017.4 ms |

#### Throughput Saturation

| Connections | Total msg/s | Per-conn | MB/s | Errors |
|-------------|-------------|----------|------|--------|
| 100 | 2,285,649 | 22,856 | 422.9 | 0 |
| 500 | 3,831,507 | 7,663 | 708.9 | 0 |
| 1,000 | 6,925,602 | 6,926 | 1,281.3 | 0 |

drain=ON now matches drain=OFF at every tier. Zero errors.

##### crossbeam-channel Impact (v1.3.6 vs v1.3.8)

| Connections | v1.3.6 (Mutex) | v1.3.8 (crossbeam) | Improvement |
|-------------|----------------|---------------------|-------------|
| 100 | 178K msg/s | 2,285,649 msg/s | **12.8x** |
| 500 | 229K (324 killed) | 3,831,507 (0 errors) | **16.7x** |
| 1,000 | STALLED | 6,925,602 (0 errors) | **fixed** |

#### Sustained Connection Hold (5s)

| Connections | Alive | Dropped | Pings OK |
|-------------|-------|---------|----------|
| 100 | 100/100 | 0 | 500 |
| 500 | 500/500 | 0 | 2,500 |
| 1,000 | 1,000/1,000 | 0 | 5,000 |

#### Message Size Impact (64 connections per size)

| Size | Actual | Total msg/s | MB/s | GB/s | Errors |
|------|--------|-------------|------|------|--------|
| 64B | 94B | 2,402,910 | 215.4 | 0.21 | 0 |
| 256B | 286B | 2,303,210 | 628.2 | 0.61 | 0 |
| 1KB | 1054B | 2,067,295 | 2,078.0 | 2.03 | 0 |
| 4KB | 4126B | 1,598,165 | 6,288.6 | 6.14 | 0 |
| 16KB | 16414B | 866,977 | 13,571.3 | 13.25 | 0 |
| 64KB | 65566B | 234,277 | 14,649.0 | 14.31 | 0 |

Peak bandwidth: **14.3 GB/s** at 64KB. Within 6% of drain=OFF.

#### Wire Format Throughput (500 connections)

| Format | Payload | Total msg/s | Per-conn | MB/s | Errors |
|--------|---------|-------------|----------|------|--------|
| JSON (text) | 194B | 4,153,753 | 8,308 | 768.5 | 0 |
| msgpack (M:) | 145B | 4,229,030 | 8,458 | 584.8 | 0 |

---

### drain_mode ON vs OFF Summary

| Test | drain=OFF | drain=ON | Ratio |
|------|-----------|----------|-------|
| Connection storm (15K) | 1,307 conn/s | 1,290 conn/s | 99% |
| Latency p50 @ 1K conns | 52.5 ms | 53.1 ms | 99% |
| Throughput @ 100 conns | 2.3M msg/s | 2.3M msg/s | 100% |
| Throughput @ 500 conns | 3.8M msg/s | 3.8M msg/s | 100% |
| Throughput @ 1000 conns | 6.9M msg/s | 6.9M msg/s | 100% |
| Bandwidth @ 64KB | 15.2 GB/s | 14.3 GB/s | 94% |
| JSON wire @ 500 | 4.2M msg/s | 4.2M msg/s | 100% |
| msgpack wire @ 500 | 4.3M msg/s | 4.2M msg/s | 98% |

crossbeam-channel drain mode has zero measurable overhead. The channel is not a
bottleneck - the Python client is the limiting factor at these throughput levels.

---

## Python Client Limitations

The Python client tops out at ~6.9M msg/s with 1,000 connections. Beyond this, the
asyncio event loop and GIL become the ceiling, not the server. Max practical connections
is around 1,000 (1 OS process per connection on 64 threads). Async Python caps at ~1.5M
msg/s regardless of connection count.

To find the true server ceiling, we built a native Rust benchmark client (`wse-bench`).
Results: [BENCHMARKS_RUST_CLIENT.md](BENCHMARKS_RUST_CLIENT.md).

---

## Reproducing

```bash
# Start the benchmark server (standalone mode)
python benchmarks/bench_server.py

# Single-client benchmarks
python benchmarks/bench_wse.py

# Multi-process benchmarks (M2: 10 workers, EPYC: 64 workers)
python benchmarks/bench_wse_multiprocess.py --workers 10

# Brutal concurrency (EPYC)
python benchmarks/bench_brutal.py --token <JWT> --tiers 100,500,1000 --duration 5
```

---

*Tested February 2026. WSE v1.2 (single/multi-process), v1.3.8 (brutal concurrency).*
