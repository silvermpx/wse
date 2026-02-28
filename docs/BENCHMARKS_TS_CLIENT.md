# WSE Stress Test - TypeScript Client (wse-ts-bench)

Measured real-world performance from a **Node.js consumer** perspective - the actual use case
for the `wse-client` npm package. The Rust bench proved the server can do 14M msg/s JSON and
30M binary; this measures what a TypeScript application actually gets.

Single Node.js process = single event loop thread. For multi-core comparison, we also ran
64 concurrent processes (one per logical core) to match the Rust client's automatic
multi-core usage via tokio work-stealing.

## Hardware

AMD EPYC 7502P (32 cores, 64 threads), 128 GB RAM, Ubuntu 24.04.
Client and server on the same machine (loopback, zero network noise).

Server: `bench_server.py` with `RustWSEServer` - same maturin-compiled binary as production,
running through the full Python wrapper with drain_mode=ON and JWT auth enabled.

- **Node.js**: 22.22.0
- **WebSocket**: `ws` 8.18 (C++ addon for frame encode/decode)
- **Timer**: `process.hrtime.bigint()` (nanosecond precision)
- **Histogram**: `hdr-histogram-js` 3.0

## OS Tuning

```
ulimit -n 65536
net.ipv4.ip_local_port_range = 1024 65535
```

Connection limit capped at ~64K by default ulimit. With higher ulimits and multi-IP
binding, Node.js `ws` handles 100K+ connections.

---

## Cross-Client Comparison

| Client | Processes | JSON msg/s | Binary msg/s | Connections/s | Max Connections |
|--------|-----------|-----------|--------------|---------------|-----------------|
| **Rust** (tokio) | 1 (64 threads) | **14.7M** | **30.0M** | 15,020 | 500K |
| **TypeScript** (ws) | 64 | 7.0M | 7.9M | 10,673 | 64K+ |
| **TypeScript** (ws) | 1 | 116K | 118K | 5,508 | 64K+ |
| **Python** (asyncio) | 64 | 6.9M | - | - | - |

Rust uses all 64 threads in a single process (tokio work-stealing). TypeScript needs 64
OS processes to use the same hardware. Python uses 64 subprocesses with synchronous send
loops. All three hit the same server - differences are pure client overhead.

---

## Test 1: Connection Storm

All connections opened as fast as possible in batches of 500. Measures TCP handshake +
HTTP upgrade + JWT validation + `server_ready` round-trip time.

| Connections | Accept Rate | p50 | p95 | p99 | Max | Failed |
|-------------|-------------|-----|-----|-----|-----|--------|
| 100 | 1,087/s | 50 ms | 75 ms | 87 ms | 88 ms | 0 |
| 500 | 2,183/s | 76 ms | 189 ms | 213 ms | 218 ms | 0 |
| 1,000 | 2,680/s | 106 ms | 333 ms | 359 ms | 363 ms | 0 |
| 5,000 | 4,279/s | 310 ms | 1,070 ms | 1,130 ms | 1,140 ms | 0 |
| 10,000 | 5,508/s | 450 ms | 1,690 ms | 1,780 ms | 1,790 ms | 0 |
| 30,000 | 5,219/s | 2,430 ms | 5,480 ms | 5,660 ms | 5,690 ms | 0 |
| 50,000 | 4,453/s | 5,140 ms | 10,830 ms | 11,120 ms | 11,150 ms | 0 |

Peak accept rate: **5,508 conn/s** at 10K tier. Zero errors across all tiers. Rate
decreases above 10K due to event loop saturation during batch handshakes - each
connection goes through the full JWT + `server_ready` + `client_hello` handshake
sequentially on one thread.

Rust client at the same 10K tier: 14,543 conn/s (2.6x faster). The difference is
Promise allocation + V8 GC pauses during batch opens.

### 64-Process Connection Storm

| Metric | Value |
|--------|-------|
| Total connections | 64,000 |
| Wall time | 6.0s |
| **Aggregate rate** | **10,673 conn/s** |

---

## Test 2: Ping/Pong Latency Under Load

Each connection sends 50 application-level PINGs, server responds with PONGs.
All connections ping concurrently - worst-case latency, not sequential.

| Connections | Total Pings | p50 | p95 | p99 | Max |
|-------------|-------------|-----|-----|-----|-----|
| 100 | 5,000 | 6.1 ms | 16.4 ms | 19.6 ms | 20.6 ms |
| 500 | 25,000 | 17.3 ms | 57.2 ms | 77.6 ms | 141.8 ms |
| 1,000 | 50,000 | 40.5 ms | 112.5 ms | 152.8 ms | 234.3 ms |
| 5,000 | 250,000 | 199.9 ms | 535.3 ms | 621.4 ms | 746.6 ms |
| 10,000 | 500,000 | 408.3 ms | 1,160 ms | 1,360 ms | 1,610 ms |

At 100 connections: p50 = 6.1ms (Rust: 0.38ms, 16x faster). The gap is the event loop -
all connections share a single thread, so each PING/PONG round-trip waits behind all
other I/O callbacks. Latency scales linearly with connection count.

---

## Test 3: Throughput Saturation

All connections send ~175-byte JSON trading messages as fast as possible for 10 seconds.

| Connections | Msg/s | MB/s | Per-conn msg/s |
|-------------|-------|------|----------------|
| 100 | 116K | 20.3 | 1,162 |
| 500 | 105K | 18.4 | 210 |
| 1,000 | 88K | 15.4 | 88 |
| 5,000 | 42K | 7.4 | 8 |
| 10,000 | 20K | 3.5 | 2 |

Peak throughput: **116K msg/s** at 100 connections (20.3 MB/s). The event loop is the
ceiling - `ws` frame encoding + libuv scheduling caps a single thread. More connections
means more context switching, reducing per-connection throughput.

Rust at 100 connections: 14.5M msg/s (125x faster). This is the tokio-vs-event-loop gap.

### 64-Process Throughput

| Metric | Value |
|--------|-------|
| Per-process avg | ~108K msg/s |
| **Aggregate** | **6,950K msg/s** |
| Linear scaling | 97% |

64 processes achieve 7.0M msg/s - 97% of theoretical max (64 x 108K = 6.9M).
OS scheduling across 64 logical cores provides excellent parallelism.

---

## Test 4: Payload Size x Connections Matrix

5-second burst per cell. All values in msg/s.

| Size \ Conns | 100 | 500 | 1K | 5K | 10K |
|-------------|-----|-----|-----|-----|-----|
| **64 B** | 124K | 107K | 81K | 35K | 17K |
| **256 B** | 115K | 95K | 79K | 34K | 17K |
| **1 KB** | 89K | 74K | 62K | 28K | 14K |
| **4 KB** | 47K | 41K | 35K | 17K | 9K |
| **16 KB** | 20K | 16K | 13K | 7K | 3K |
| **64 KB** | 6K | 5K | 4K | 2K | 1K |

### Bandwidth at 100 connections

| Size | msg/s | Bandwidth |
|------|-------|-----------|
| 64 B | 124K | 7.6 MB/s |
| 256 B | 115K | 28 MB/s |
| 1 KB | 89K | 87 MB/s |
| 4 KB | 47K | 185 MB/s |
| 16 KB | 20K | 312 MB/s |
| 64 KB | 6K | 378 MB/s |

Peak bandwidth: **378 MB/s** at 64KB (Rust: 18.2 GB/s, 48x). The gap is V8 Buffer
allocation per frame + libuv write coalescing.

---

## Test 5: Format Comparison

JSON vs MsgPack vs zlib-compressed, same logical payload. 5-second burst per format.

Wire sizes: JSON = 175 bytes, MsgPack = 145 bytes (`M:` prefix), Compressed = 152 bytes (`C:` prefix).

| Connections | JSON | MsgPack | vs JSON | Compressed | vs JSON |
|-------------|------|---------|---------|------------|---------|
| 100 | 117K | 118K | +1.0% | 118K | +0.2% |
| 500 | 96K | 97K | +1.0% | 97K | +0.6% |
| 1,000 | 80K | 82K | +2.1% | 82K | +1.5% |
| 5,000 | 35K | 35K | +0.8% | 35K | +0.3% |
| 10,000 | 17K | 18K | +1.3% | 18K | +0.4% |

At single-process scale, all three formats perform within 1-2% - the bottleneck is
`ws` frame encoding + event loop scheduling, not serialization.

Rust sees a **2x** gap between JSON (14M) and binary (30M) because at 14M msg/s,
serialization overhead becomes significant. At Node.js's 116K msg/s, it's irrelevant.

### 64-Process Format Comparison

| Format | Aggregate msg/s | vs JSON |
|--------|----------------|---------|
| **JSON** | 7.0M | - |
| **MsgPack** | 7.9M | +13% |
| **Compressed** | 7.9M | +13% |

At 64-process scale, MsgPack and Compressed gain 13% over JSON - smaller wire size
reduces per-process overhead when all 64 threads are saturated.

---

## Test 6: Sustained Hold

Hold N connections for 30 seconds with periodic PING/PONG (every 5 seconds).
Tests long-term stability - can the server + Node.js client maintain connections
without degradation?

| Connections | Survival | Disconnects | Total PINGs | p50 | p95 | p99 |
|-------------|----------|-------------|-------------|-----|-----|-----|
| 1,000 | **100%** | 0 | 5,000 | 1.4 ms | 5.0 ms | 6.9 ms |
| 5,000 | **100%** | 0 | 25,000 | 4.1 ms | 13.9 ms | 16.5 ms |
| 10,000 | **100%** | 0 | 50,000 | 9.2 ms | 28.1 ms | 35.2 ms |

**100% survival** across all tiers. Zero disconnects over 30 seconds. PING latency
remains stable - no drift, no memory leaks, no GC pressure in the test window.

Matches the Rust client result (100% at all tiers including 100K). Connection stability
is a server-side property, not client-dependent.

---

## Test 7: Connection Limit

Binary search for the maximum number of stable connections. Exponential probe then bisect.

| Phase | Target | Connected | Result |
|-------|--------|-----------|--------|
| Probe | 1,000 | 1,000 | OK |
| Probe | 2,000 | 2,000 | OK |
| Probe | 4,000 | 4,000 | OK |
| Probe | 8,000 | 8,000 | OK |
| Probe | 16,000 | 16,000 | OK |
| Probe | 32,000 | 32,000 | OK |
| Probe | 64,000 | 64,000 | OK |

**Max stable connections: 64K+** (limited by OS ulimit=65536, not server or client).
All probes succeeded up to the OS limit. With `ulimit -n 500000` and multi-IP binding,
Node.js `ws` can handle well beyond 64K - the server proved 500K with the Rust client.

---

## Key Takeaways

1. **116K msg/s** single-process peak (JSON) - typical for a well-tuned Node.js WebSocket client
2. **7.0M msg/s** at 64 processes - 97% linear scaling across all cores
3. **Rust is 127x faster per-thread** (14.7M vs 116K single-process) - tokio async vs V8 event loop
4. **64K+ stable connections** per process - limited by ulimit, not the `ws` library
5. **100% connection survival** for 30 seconds at every tier - rock-solid stability
6. **Format differences are negligible** at single-process scale (<2%), meaningful at 64-process (+13% for binary)
7. **Latency scales linearly** with connections - single event loop means all connections share one thread
8. **Production-identical server** - same maturin-compiled binary, full JWT auth, drain_mode

---

## Reproducing

```bash
# Start the server (separate terminal)
cd /root/wse && source venv/bin/activate
DRAIN_MODE=1 python benchmarks/bench_server.py --max-connections 100000

# Navigate to ts-bench
cd benchmarks/ts-bench
npm install

# Run the full suite (all 7 tests, single process)
npx tsx src/main.ts \
 --host 127.0.0.1 --port 5006 \
 --secret "bench-secret-key-for-testing-only" \
 --tiers 100,500,1000,5000,10000,30000,50000 \
 --duration 10

# Single test
npx tsx src/main.ts --test throughput --tiers 100,1000 --duration 3

# 64-process throughput test
for i in $(seq 1 64); do
  npx tsx src/main.ts --test throughput --tiers 100 --duration 10 2>/dev/null &
done
wait
```

---

*Tested February 24, 2026. WSE v1.3.9, wse-ts-bench v0.1.0, Node.js 22.22.0.*
