# WSE Fan-out Benchmark Results

Server broadcasts messages to N subscribers. Measures delivery throughput,
message loss (sequence gaps), and end-to-end latency.

Two modes tested:
- **Single-Instance Broadcast** (Test 8) — `server.broadcast_all()` directly to all connections, no Redis
- **Multi-Instance Redis Pub/Sub** (Test 9) — publish on Server A -> Redis -> Server B -> N subscribers

## Hardware

AMD EPYC 7502P (32 cores, 64 threads), 128 GB RAM, Ubuntu 24.04.
Client and server on the same machine (loopback). Redis 8.6.1 on localhost (io-threads 8)
for multi-instance tests.

Server: `bench_fanout_server.py` with `RustWSEServer` — production maturin binary,
drain_mode=ON, JWT auth enabled.

Client: `wse-bench --test fanout-broadcast` / `fanout-multi` (Rust, tokio).

## OS Tuning

```
ulimit -n 1000000
sysctl net.ipv4.ip_local_port_range="1024 65535"
sysctl fs.nr_open=1048576
```

For >64K connections: multi-IP via loopback aliases (127.0.0.1 through 127.0.0.9).

---

## Test 8: Single-Instance Broadcast

Server continuously broadcasts ~150-byte JSON messages. All connections receive every
message. The benchmark client tracks sequence numbers per connection and reports gaps.

The server's total write capacity (~1.2-2.1M WebSocket frame writes/s) is roughly
constant regardless of subscriber count. More subscribers = fewer unique messages
published per second, but the same total deliveries.

### Results (up to 500K connections)

| Subscribers | Published/s | Deliveries/s | Bandwidth | Gaps | Failed |
|-------------|-------------|-------------|-----------|------|--------|
| 10 | 206K | 2.1M | 295 MB/s | 0 | 0 |
| 100 | 14K | 1.4M | 192 MB/s | 0 | 0 |
| 500 | 3K | 1.4M | 188 MB/s | 0 | 0 |
| 1,000 | 1K | 1.4M | 185 MB/s | 0 | 0 |
| 5,000 | 256 | 1.3M | 172 MB/s | 0 | 0 |
| 10,000 | 120 | 1.2M | 163 MB/s | 0 | 0 |
| 50,000 | 25 | 1.3M | 172 MB/s | 0 | 0 |
| 100,000 | 17 | 1.7M | 234 MB/s | 0 | 0 |
| 200,000 | 7 | 1.5M | 199 MB/s | 0 | 0 |
| 250,000 | 6 | 1.7M | 223 MB/s | 0 | 0 |
| **500,000** | **2** | **1.4M** | **128 MB/s** | **0** | **0** |

Peak deliveries: **2.1M/s** at 10 subscribers. Sustained ~1.3-1.7M/s across all tiers.

**Zero message loss at every tier** — not a single sequence gap from 10 to 500,000
subscribers. The Rust broadcast path (`server.broadcast_all()`) writes to all connections
atomically with no fan-out queue drops.

### Latency (low subscriber counts)

| Subscribers | p50 | p95 | p99 | p99.9 |
|-------------|-----|-----|-----|-------|
| 10 | 0.005 ms | 0.010 ms | 0.013 ms | 0.038 ms |
| 100 | 0.052 ms | 0.133 ms | 0.154 ms | 0.193 ms |
| 500 | 0.266 ms | 0.624 ms | 0.718 ms | 0.827 ms |

At high subscriber counts (1K+), individual message delivery latency exceeds the
measurement window because each broadcast takes longer than the inter-message interval.
This is expected — the metric that matters at scale is total deliveries/s.

### Memory at Scale

| Subscribers | Server RSS | Swap |
|-------------|-----------|------|
| 250,000 | ~65 GB | 0 |
| 500,000 | ~119 GB | ~90 GB |

500K connections consumed nearly all 128 GB RAM and spilled heavily into swap.
Production recommendation: cap at ~200K connections per process on 128 GB machines.

---

## Test 9: Multi-Instance Fan-out (Redis)

Two separate server processes coordinated via Redis:

```
Server A (port 5006, pubsub mode)
    |
    | server.broadcast("bench_topic", msg)
    v
  Redis PUBLISH wse:bench_topic
    |
    | PSUBSCRIBE wse:*
    v
Server B (port 5007, subscribe mode) --> N WebSocket clients
```

Server A publishes continuously. Server B receives via Redis PSUBSCRIBE, then fans out
to all locally connected clients. The benchmark client connects only to Server B.

This tests true horizontal scaling: the publishing server has zero local subscribers,
all delivery happens on a separate instance via Redis coordination.

### Results (Redis 8.6, pipelined PUBLISH, io-threads 8)

| Subscribers (Server B) | Published/s | Deliveries/s | Bandwidth | Gaps |
|-------------------------|-------------|-------------|-----------|------|
| 10 | 45K | 448K | 41 MB/s | 0 |
| 100 | 9K | 906K | 83 MB/s | 0 |
| **500** | **2K** | **1.04M** | **96 MB/s** | **0** |
| 1,000 | 956 | 957K | 88 MB/s | 0 |
| 5,000 | 155 | 778K | 72 MB/s | 0 |

Peak deliveries: **1.04M/s** at 500 subscribers. Zero gaps at every tier.

Redis PUBLISH commands are batched via pipelining (up to 64 per round-trip) instead of
sequential one-at-a-time. Combined with Redis 8.6 io-threads, this gives +12-73%
improvement over sequential PUBLISH on Redis 7.

### Multi-Instance vs Single-Instance

| Metric | Single-Instance | Multi-Instance (Redis) |
|--------|----------------|----------------------|
| Peak deliveries/s | 2.1M | 1.04M |
| Max tested connections | 500K | 5K |
| Message loss | 0 | 0 |
| Bottleneck | WebSocket writes | WebSocket fan-out (Redis is not limiting) |
| Horizontal scaling | No | Yes (N instances) |

The multi-instance path is ~2x slower than direct broadcast in this benchmark because
all components (Server A, Server B, Redis, benchmark client) run on the same machine
and compete for CPU/memory. On separate machines, Server B would have dedicated
resources for fan-out and should approach single-instance broadcast throughput.

With N Server B instances on separate machines, capacity scales linearly:
2 instances = ~2M del/s, 3 instances = ~3M del/s.

---

## Reproducing

### Test 8: Single-Instance Broadcast

```bash
# Terminal 1: Start server
cd /root/wse && source venv/bin/activate
PYTHONPATH=/root/wse python benchmarks/bench_fanout_server.py --mode broadcast --max-connections 600000

# Terminal 2: Run benchmark
ulimit -n 1000000
cd /root/wse/benchmarks/rust-bench
./target/release/wse-bench --test fanout-broadcast \
  --host 127.0.0.1 --port 5006 \
  --secret "bench-secret-key-for-testing-only" \
  --tiers 10,100,500,1000,5000,10000,50000,100000,250000 \
  --duration 10
```

### Test 9: Multi-Instance Fan-out

```bash
# Terminal 1: Server A (publisher)
PYTHONPATH=/root/wse python benchmarks/bench_fanout_server.py \
  --mode pubsub --port 5006 --redis-url redis://localhost:6379

# Terminal 2: Server B (subscribers)
PYTHONPATH=/root/wse python benchmarks/bench_fanout_server.py \
  --mode subscribe --port 5007 --redis-url redis://localhost:6379

# Terminal 3: Run benchmark (connects to both)
./target/release/wse-bench --test fanout-multi \
  --host 127.0.0.1 --port 5006 --port2 5007 \
  --secret "bench-secret-key-for-testing-only" \
  --tiers 10,100,500,1000,2000,5000,10000,20000 \
  --duration 10
```

---

*Tested February 24, 2026. WSE v1.4.0, wse-bench v0.1.0.*
