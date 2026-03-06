# WSE Fan-out Benchmark Results

Server broadcasts messages to N subscribers. Measures delivery throughput,
message loss (sequence gaps), and end-to-end latency.

Two modes tested:
- **Single-Instance Broadcast** (Test 8) -- `server.broadcast_all()` directly to all connections
- **Multi-Instance Cluster** (Test 11) -- publish on Server A -> custom TCP protocol -> Server B -> N subscribers

## Hardware

AMD EPYC 7502P (32 cores, 64 threads), 128 GB RAM, Ubuntu 24.04.
Client and server on the same machine (loopback).

Server: `bench_fanout_server.py` with `RustWSEServer` -- production maturin binary,
drain_mode=ON, JWT auth enabled.

Client: `wse-bench` (Rust, tokio). Duration: 10 seconds per tier.

Message format: ~100-byte JSON (`{"t":"fanout_tick","p":{"seq":N,"ts_us":T,"s":"ES","px":5234.75,"q":2}}`).

## OS Tuning

```
ulimit -n 1000000
sysctl net.ipv4.ip_local_port_range="1024 65535"
sysctl fs.nr_open=1048576
```

For >64K connections: multi-IP via loopback aliases (127.0.0.1 through 127.0.0.9).

---

## Test 8: Single-Instance Broadcast

Server continuously broadcasts JSON messages. All connections receive every message.

| Subscribers | Published/s | Deliveries/s | Bandwidth | Gaps |
|-------------|-------------|-------------|-----------|------|
| 10 | 481K | 4.8M | 353 MB/s | 0 |
| 100 | 50K | **5.0M** | 369 MB/s | 0 |
| 500 | 8K | 4.2M | 309 MB/s | 0 |
| 1,000 | 4K | 3.8M | 279 MB/s | 0 |
| 2,000 | 2K | 3.3M | 239 MB/s | 0 |
| 5,000 | 473 | 2.4M | 174 MB/s | 0 |
| 10,000 | 201 | 2.0M | 148 MB/s | 0 |
| 20,000 | 84 | 1.7M | 123 MB/s | 0 |
| 50,000* | 83 | 2.7M | 254 MB/s | 0 |

*32,764 of 50,000 connected (ulimit). 100K and 200K tiers skipped (same ulimit limit).

**Peak: 5.0M deliveries/s at 100 subscribers (369 MB/s). Zero message loss at every tier.**

**Note on latency:** High p50 values (seconds, not milliseconds) are a benchmark artifact.
The publisher floods at maximum rate -- hundreds of thousands of messages per second --
which builds up a queue. Latency here = queue wait time + delivery time. In production
at typical rates (100-10K msg/s), delivery latency is sub-millisecond.

---

## Test 11: Multi-Instance Fan-out (Cluster Protocol)

Two separate server processes connected via WSE's built-in cluster protocol (direct TCP):

```
Server A (port 5006, cluster mode) -- publishes continuously
    |
    | Direct TCP (WSE cluster protocol)
    v
Server B (port 5007, cluster-subscribe mode) --> N WebSocket clients
```

| Subscribers | Published/s | Deliveries/s | Bandwidth | p50 | p95 | p99 | Gaps |
|-------------|-------------|-------------|-----------|-----|-----|-----|------|
| 10 | 52K | 517K | 48 MB/s | 5.1s | 8.0s | 8.8s | 0 |
| 100 | 19K | 1.9M | 181 MB/s | 8.0s | 15.5s | 16.3s | 0 |
| 500 | 4K | 2.2M | 204 MB/s | 19.7s | 27.6s | 28.3s | 0 |
| 1,000 | 2K | 2.0M | 183 MB/s | 34.4s | 39.9s | 40.5s | 0 |
| 2,000 | 945 | 1.9M | 176 MB/s | 46.5s | 52.2s | 52.7s | 0 |
| 5,000 | 369 | 1.8M | 172 MB/s | 58.9s | 64.8s | 65.2s | 0 |
| 10,000 | 338 | **3.4M** | 315 MB/s | 71.7s | 77.8s | 78.4s | 0 |
| 20,000 | 476 | **9.5M** | 887 MB/s | 86.2s | 92.7s | 93.2s | 0 |

**Peak: 9.5M deliveries/s at 20,000 subscribers (887 MB/s). Zero message loss at every tier.**

Verified over 3 consecutive runs with <2% variance.

At low subscriber counts (10-100), the TCP hop between servers is the bottleneck. At higher
counts, fan-out becomes the dominant cost, and cluster throughput approaches -- and exceeds --
standalone levels due to batching amortization.

---

## Comparison: Standalone vs Cluster

| Mode | Peak del/s | at N subs | Bandwidth | Gaps | Horizontal Scaling |
|------|-----------|----------|-----------|------|-------------------|
| Standalone | **5.0M** | 100 | 369 MB/s | 0 | No |
| Cluster | **9.5M** | 20K | 887 MB/s | 0 | Yes (N instances) |

At low subscriber counts (10-100), the TCP hop between servers is the bottleneck. At higher
counts, fan-out becomes the dominant cost, and cluster throughput approaches -- and exceeds --
standalone levels due to batching amortization.

### Memory at Scale

| Subscribers | Server RSS | Swap |
|-------------|-----------|------|
| 250,000 | ~65 GB | 0 |
| 500,000 | ~119 GB | ~90 GB |

500K connections consumed nearly all 128 GB RAM and spilled heavily into swap.
Production recommendation: cap at ~200K connections per process on 128 GB machines.

---

## v2.2.0 Results (March 2026)

Slow consumer protection (per-connection byte-based backpressure), fan-out hot path
optimizations, dependency updates (tokio 1.50). CPU pinning for isolation:
server on cores 0-15,32-47 (32 cores), benchmark client on remaining cores.

### Test 10: Single-Node Topic Fan-out (32 cores, pinned)

Topic-based fan-out via `broadcast_local()`. Server pinned to 32 cores.

| Subscribers | Published/s | Deliveries/s | Bandwidth | Gaps | Drops |
|-------------|-------------|-------------|-----------|------|-------|
| 10 | 254K | 2.5M | 257 MB/s | 0 | 0 |
| 100 | 47K | **4.7M** | 477 MB/s | 0 | 0 |
| 500 | 5K | 2.4M | 246 MB/s | 0 | 0 |
| 1,000 | 2K | 2.0M | 200 MB/s | 0 | 0 |
| 2,000 | 1K | 2.2M | 220 MB/s | 0 | 0 |
| 5,000 | 441 | 2.2M | 222 MB/s | 0 | 0 |
| 10,000 | 155 | 1.6M | 156 MB/s | 0 | 0 |
| 20,000 | 57 | 1.1M | 115 MB/s | 0 | 0 |
| 50,000 | 18 | 940K | 95 MB/s | 0 | 0 |
| 100,000 | 10 | 1.1M | 107 MB/s | 0 | 0 |

**Peak: 4.7M deliveries/s at 100 subscribers. Zero gaps, zero drops at all tiers including 100K.**

### Test 11: Cluster Fan-out 50/50 Split (32+32 cores, pinned)

Subscribers split evenly across two servers. Server A publishes and fans out to its
local N/2 subscribers, then replicates via cluster TCP to Server B which fans out to
its N/2 subscribers. True horizontal scaling test.

```
Server A (cores 0-15,32-47)          Server B (cores 16-31,48-63)
  - publisher                          - N/2 subscribers
  - N/2 subscribers
  |                                    ^
  +--- cluster TCP (direct) ---------->+
```

| Subscribers | Split A/B | Published/s | Deliveries/s | Bandwidth | Gaps | Drops |
|-------------|-----------|-------------|-------------|-----------|------|-------|
| 10 | 5/5 | 154K | 1.5M | 156 MB/s | 0 | 0 |
| 100 | 50/50 | 43K | 4.3M | 432 MB/s | 0 | 0 |
| 500 | 250/250 | 13K | **6.6M** | 662 MB/s | 0 | 0 |
| 1,000 | 500/500 | 6K | 6.4M | 642 MB/s | 0 | 0 |
| 2,000 | 1000/1000 | 3K | 5.6M | 559 MB/s | 0 | 0 |
| 5,000 | 2500/2500 | 815 | 4.1M | 412 MB/s | 0 | 0 |

**Peak: 6.6M deliveries/s at 500 subscribers (140% of single-node). Zero gaps, zero drops.**

### v2.2.0 Comparison: Single-Node vs Cluster

| Subs | Single-Node | Cluster 50/50 | Scaling |
|------|------------|--------------|---------|
| 10 | 2.5M | 1.5M | 0.6x |
| 100 | 4.7M | 4.3M | 0.9x |
| 500 | 2.4M | **6.6M** | **2.8x** |
| 1,000 | 2.0M | 6.4M | **3.2x** |
| 2,000 | 2.2M | 5.6M | **2.5x** |
| 5,000 | 2.2M | 4.1M | **1.9x** |

At 500-1000 subscribers, the cluster delivers 3x the throughput of a single node,
confirming true horizontal scaling. At low counts (10), the inter-node TCP hop
overhead dominates. At very high counts (5K+), per-connection write cost dominates
and the scaling advantage narrows.

---

## Reproducing

### Test 8: Single-Instance Broadcast

```bash
# Terminal 1: Start server
python benchmarks/bench_fanout_server.py --mode broadcast --max-connections 600000

# Terminal 2: Run benchmark
ulimit -n 1000000
./benchmarks/rust-bench/target/release/wse-bench --test fanout-broadcast \
  --host 127.0.0.1 --port 5006 --duration 10
```

### Test 11: Multi-Instance Fan-out (Cluster)

```bash
# Terminal 1: Server A (publisher)
python benchmarks/bench_fanout_server.py \
  --mode cluster --port 5006 --peers 127.0.0.1:5007

# Terminal 2: Server B (subscribers)
python benchmarks/bench_fanout_server.py \
  --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006

# Terminal 3: Run benchmark
./benchmarks/rust-bench/target/release/wse-bench --test fanout-cluster \
  --port 5006 --port2 5007 --duration 10
```

---

*Original results: February 28, 2026 (WSE v2.0.0). v2.2.0 results: March 6, 2026.*
