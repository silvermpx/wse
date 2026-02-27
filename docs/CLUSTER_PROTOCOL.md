# Cluster Protocol

WSE uses a custom binary protocol for communication between cluster nodes. All inter-node traffic uses TCP with optional mTLS.

## Connection Establishment

1. Node A connects to Node B via TCP (or TLS if configured)
2. Node A sends a HELLO frame
3. Node B validates the HELLO (magic bytes, version, capabilities)
4. Node B responds with its own HELLO frame
5. Both nodes begin heartbeat exchange

If the HELLO handshake fails - wrong magic bytes, unsupported version, or malformed payload - the connection is closed immediately and the initiator retries with exponential backoff.

## Frame Format

All frames share a common 8-byte header:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 1 | type | Message type (0x01 - 0x0C) |
| 1 | 1 | flags | Bit 0: compressed (zstd), bits 1-7: reserved |
| 2 | 2 | topic_len | Topic field length (little-endian u16) |
| 4 | 4 | payload_len | Payload field length (little-endian u32) |
| 8 | N | topic | Topic string (UTF-8, N = topic_len) |
| 8+N | M | payload | Payload data (M = payload_len) |

Maximum frame size: 1,048,576 bytes (1 MB). Frames exceeding this limit are rejected and the connection is reset.

## Message Types

### MSG (0x01) - Application Message

Carries a topic-addressed message from one node to subscribers on another.

```
[header: type=0x01, flags, topic_len=N, payload_len=M]
[N bytes: topic string]
[M bytes: message payload (pre-framed WebSocket data)]
```

If FLAG_COMPRESSED (0x01) is set in the flags byte, the payload is zstd-compressed. The receiver decompresses before dispatching to local subscribers.

The payload contains pre-framed WebSocket data. This means the sending node has already formatted the message for WebSocket delivery, so the receiving node can forward it directly to connected clients without re-serialization.

### PING (0x02) / PONG (0x03) - Heartbeat

```
[header: type=0x02 or 0x03, all other fields zero]
```

Sent every 5 seconds. A peer that receives no data (including PONG responses) for 15 seconds is considered dead. On timeout, the connection is torn down and reconnection begins with exponential backoff.

### HELLO (0x04) - Handshake

```
[header: type=0x04, payload_len=N]
[4 bytes: magic "WSE\x00"]
[2 bytes: protocol version (little-endian u16, currently 1)]
[2 bytes: instance_id length (little-endian u16)]
[variable: instance_id (UUID string)]
[4 bytes: capabilities bitmask (little-endian u32)]
```

The magic bytes `WSE\x00` prevent accidental connections from non-WSE services. The protocol version field enables forward-compatible upgrades - both peers use the minimum of their advertised versions.

**Capabilities**:

| Bit | Name | Description |
|-----|------|-------------|
| 0 | CAP_INTEREST_ROUTING | SUB/UNSUB/RESYNC support |
| 1 | CAP_COMPRESSION | zstd compression support |
| 2 | CAP_PRESENCE | Presence sync support |

Negotiated capabilities are the bitwise AND of both peers. A feature is only active when both sides advertise support for it.

### SHUTDOWN (0x05) - Graceful Shutdown

```
[header: type=0x05, all other fields zero]
```

Sent when a node is shutting down cleanly. The receiving peer removes the sender from its peer table without triggering reconnection logic. After sending SHUTDOWN to all peers, the node cancels all peer tasks immediately.

### SUB (0x06) - Topic Subscribe

```
[header: type=0x06, topic_len=N]
[N bytes: topic pattern]
```

Sent when the first local client on a node subscribes to a topic. The receiving peer records this interest so it can route matching MSG frames to the sender.

Topic patterns support glob-style matching (e.g., `chat.*`, `prices.*.usd`).

### UNSUB (0x07) - Topic Unsubscribe

```
[header: type=0x07, topic_len=N]
[N bytes: topic pattern]
```

Sent when the last local client on a node unsubscribes from a topic. The receiving peer removes the interest entry. No further MSG frames for this topic are forwarded to the sender.

### RESYNC (0x08) - Topic List Resync

```
[header: type=0x08, payload_len=N]
[N bytes: newline-separated topic list]
```

Sent after a peer reconnects to re-establish the full interest table. The receiver replaces any stale interest state for the sender with the topics in this message. This prevents interest drift caused by missed SUB/UNSUB messages during disconnection.

### PeerAnnounce (0x09) - Address Gossip

```
[header: type=0x09, topic_len=N]
[N bytes: cluster address string (e.g., "10.0.0.4:9001")]
```

Sent by a node to announce its own cluster address to peers. Used during dynamic peer discovery - when a new node joins the mesh, existing nodes gossip its address so all nodes can establish direct connections.

### PeerList (0x0A) - Known Peers

```
[header: type=0x0A, payload_len=N]
[2 bytes: peer_count (little-endian u16)]
[For each peer: 2 bytes addr_len (little-endian u16) + addr_len bytes address string]
```

Response to a PeerAnnounce or sent periodically during discovery. Contains the full list of known peer addresses, allowing a newly joined node to connect to all existing members of the mesh.

### PresenceUpdate (0x0B) - Presence Delta

```
[header: type=0x0B, topic_len=N, payload_len=M]
[N bytes: topic string]
[1 byte: action (0=join, 1=leave, 2=update)]
[8 bytes: updated_at timestamp (little-endian u64, milliseconds since epoch)]
[2 bytes: user_id_len (little-endian u16)]
[user_id_len bytes: user_id string]
[remaining bytes: presence data JSON]
```

Sent on every presence join, leave, or data update. The receiving peer merges the update using last-write-wins conflict resolution based on the `updated_at` timestamp. Only sent to peers that have negotiated `CAP_PRESENCE`.

Action values:

| Action | Meaning |
|--------|---------|
| 0 | Join - user became present in topic |
| 1 | Leave - user left topic |
| 2 | Update - user's presence data changed |

### PresenceFull (0x0C) - Full Presence Sync

```
[header: type=0x0C, topic_len=0, payload_len=M]
[M bytes: JSON state]
```

Sent when a new peer connects and completes the HELLO handshake. The payload is the full local presence state serialized as JSON:

```json
{
  "chat-1": {
    "alice": {"data": {"status": "online"}, "updated_at": 1708441800000},
    "bob": {"data": {"status": "away"}, "updated_at": 1708441795000}
  }
}
```

The receiver merges this state into its own presence tables, creating entries for remote users. Only entries with local connections (not re-synced remote entries) are included in the outgoing full sync to prevent infinite amplification.

## Interest-Based Routing

Without interest routing, every MSG is forwarded to every peer regardless of whether that peer has subscribers for the topic. This works but wastes bandwidth in clusters where topics are partitioned across nodes.

With CAP_INTEREST_ROUTING negotiated, nodes only forward messages to peers that have declared interest in the topic:

1. Client on Node A subscribes to `"prices"`
2. Node A sends SUB(`"prices"`) to all peers
3. Node B records that Node A is interested in `"prices"`
4. When Node B broadcasts to `"prices"`, it forwards the MSG to Node A
5. When the last subscriber on Node A leaves `"prices"`, Node A sends UNSUB(`"prices"`)

Subscription tracking uses reference counting. The first local subscriber for a topic triggers a SUB to peers; the last unsubscribe triggers an UNSUB. This prevents duplicate SUB/UNSUB messages when multiple local clients share the same topic.

**Reconnection behavior**: after a peer reconnects and completes the HELLO handshake, it sends a RESYNC containing its full topic list. The receiving node replaces any stale interest state for that peer. Before RESYNC is received, the safe default is to forward all MSG frames to that peer (preventing message loss during the resync window).

**Known trade-off**: a SUB message may arrive at a peer after a MSG for that topic has already been sent. This is an accepted at-most-once delivery semantic - the alternative (buffering all messages until SUB confirmation) adds latency and complexity without meaningful benefit for real-time workloads.

## Compression

MSG frames with payloads above 256 bytes may be zstd-compressed when both peers have negotiated CAP_COMPRESSION. The FLAG_COMPRESSED bit (bit 0 of the flags byte) indicates a compressed payload.

Compression is opportunistic: the sender compresses the payload and only uses the compressed form if it is smaller than the original. If compression does not reduce size, the uncompressed payload is sent with FLAG_COMPRESSED cleared.

**Decompression bomb protection**: the receiver rejects any frame where the decompressed-to-compressed size ratio exceeds 100:1, and rejects compressed payloads smaller than 8 bytes (insufficient for a valid zstd frame). Maximum decompressed size is capped at 1 MB (MAX_FRAME_SIZE). This prevents a malicious or corrupted peer from sending a small compressed payload that expands into an arbitrarily large buffer.

## mTLS

Optional mutual TLS provides authentication and encryption for inter-node traffic. When enabled, both sides of every peer connection verify each other's certificate against a shared CA.

| Parameter | Value |
|-----------|-------|
| TLS library | rustls + tokio-rustls |
| Curve | P-256 (ECDSA) |
| Client verification | WebPkiClientVerifier (mutual) |
| Certificate model | Single cert/key pair for both server and client roles |

**Configuration**: pass `ca_cert`, `node_cert`, and `node_key` file paths to `connect_cluster()`. All three must be PEM-encoded. The node certificate is used for both accepting inbound connections (server role) and initiating outbound connections (client role).

When TLS is configured, all peer connections are upgraded to TLS immediately after TCP connect, before the HELLO handshake. Plaintext connections are not accepted when TLS is enabled.

## Dynamic Peer Discovery

WSE supports two modes of cluster formation:

**Static**: pass a list of peer addresses to `connect_cluster()`. Each node connects to every listed peer.

**Dynamic**: nodes discover each other through address gossip. A node joining the cluster connects to one or more seed nodes, sends a PeerAnnounce with its own address, and receives a PeerList of known members. The node then connects to any peers it does not already have connections to.

Gossip is lightweight - PeerAnnounce and PeerList are only exchanged during join and on periodic intervals, not on every message.

## Reliability

| Mechanism | Configuration |
|-----------|--------------|
| Heartbeat | PING every 5s, timeout 15s |
| Circuit breaker | 10-failure threshold, 60s reset |
| Reconnect | Exponential backoff: 1s initial, 1.5x multiplier, 60s max, with jitter |
| Graceful shutdown | SHUTDOWN frame sent to all peers, then immediate cancel |
| Dead letter queue | 1,000 entries, FIFO eviction |
| Write batching | 64 KB batch limit per flush |
| Backpressure | Bounded per-peer channel (10,000 capacity), messages dropped on overflow |

### Circuit Breaker

Each peer connection has an independent circuit breaker. After 10 consecutive failures (connection refused, handshake timeout, write error), the breaker opens and the node stops attempting to reconnect for 60 seconds. After the reset interval, a single probe connection is attempted. If it succeeds, the breaker closes and normal operation resumes.

### Dead Letter Queue

Messages that cannot be delivered to any peer (all connections down, all channels full) are placed in a per-node dead letter queue. The DLQ holds up to 1,000 entries with FIFO eviction - oldest entries are discarded when the queue is full. DLQ contents are exposed through the health snapshot API for monitoring and debugging.

### Write Batching

The writer task for each peer connection uses a drain loop: it blocks on the first message, then drains all immediately available messages from the channel into a single write buffer (up to 64 KB). This coalesces multiple small messages into fewer syscalls, reducing per-message overhead at high throughput.

## Topology

WSE clusters use a full-mesh topology. Every node maintains a direct TCP connection to every other node. This provides the lowest possible latency (single hop) and simplest routing (no forwarding, no leader election).

Full mesh scales well for the target deployment size of 2 to 20 nodes. For larger clusters, interest-based routing significantly reduces per-node bandwidth by filtering messages at the source.
