# Cluster Protocol

WSE uses a custom binary protocol for communication between cluster nodes. All inter-node traffic uses TCP with optional mTLS.

## Connection Establishment

1. Node A connects to Node B via TCP (or TLS if configured)
2. Node A sends a HELLO frame
3. Node B validates the HELLO (magic bytes, version, capabilities)
4. Node B responds with its own HELLO frame
5. Both nodes begin heartbeat exchange

If the HELLO handshake fails - wrong magic bytes, unsupported version, or malformed payload - the connection is closed immediately and the initiator retries with exponential backoff.

**One link per pair.** Any node may dial any other, so a node started later joins through an older seed like any other. A node keeps one live link per peer instance. When a second link to the same instance appears (both nodes dialed each other at the same moment, or a peer is known under two addresses), both nodes keep the one whose **dialer** has the lexicographically lower `instance_id` and close the other, so they always agree without coordination. A peer that dials again while the receiver still holds its earlier link has given up on that one (a half-open connection), so the new link replaces it. A node that finds its own address in the peer list stops dialing it. The losing dialer keeps retrying with backoff, so the pair reconnects at once if the winning link goes away.

## Frame Format

On the wire every frame follows a 4-byte big-endian length (u32), the length of the frame after it. A frame starts with an 8-byte header:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 1 | type | Message type (0x01 - 0x0D) |
| 1 | 1 | flags | Per type: MSG bit 0 compressed (zstd), bit 1 recovery trailer; RESYNC bit 0 continuation; PresenceFull bits 0-2 sync round; otherwise 0 |
| 2 | 2 | topic_len | Topic field length (little-endian u16) |
| 4 | 4 | payload_len | Payload field length (little-endian u32) |
| 8 | N | topic | Topic string (UTF-8, N = topic_len) |
| 8+N | M | payload | Payload data (M = payload_len) |

Maximum frame size: 1,048,576 bytes (1 MB). A frame over this limit ends the link; a node never sends one (a publish too large for it reaches local subscribers only).

## Message Types

### MSG (0x01) - Application Message

Carries a topic-addressed message from one node to subscribers on another.

```
[header: type=0x01, flags, topic_len=N, payload_len=M]
[N bytes: topic string]
[M bytes: message payload]
```

If FLAG_COMPRESSED (0x01) is set in the flags byte, the payload is zstd-compressed. The receiver decompresses before dispatching to local subscribers.

The payload is the message as the sending node's own subscribers got it: JSON text, with its recovery stamp when recovery is on. The receiving node builds the WebSocket frame once for all of its subscribers (and msgpack or sealed frames for those that need them).

If FLAG_HAS_RECOVERY (0x02) is set, a 12-byte **recovery trailer** is appended *after* the payload (and is **not** counted in `payload_len`): a little-endian `u32` origin epoch followed by a little-endian `u64` origin offset. The receiving node stores the payload in a foreign recovery buffer keyed by `(topic, origin_epoch)`, so a client that reconnects to a different node can still recover with its original `(epoch, offset)` — provided that node had interest in (and therefore received) the topic's messages.

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
| 3 | CAP_RECOVERY | MSG frames carry the origin's recovery position |

Negotiated capabilities are the bitwise AND of both peers. A feature is only active when both sides advertise support for it.

### SHUTDOWN (0x05) - Graceful Shutdown

```
[header: type=0x05, all other fields zero]
```

Sent when a node is shutting down cleanly. The receiver ends the link at once instead of waiting for the heartbeat timeout, and drops the sender's interest and presence. A peer that dials the node keeps dialing it with backoff, so it is back as soon as the node restarts (a peer learned through gossip is given up after 10 failed attempts). After sending SHUTDOWN to all peers, the node cancels all peer tasks immediately.

### SUB (0x06) - Topic Subscribe

```
[header: type=0x06, topic_len=N]
[N bytes: topic pattern]
```

Sent when the first local client on a node subscribes to a topic. The receiving peer records this interest so it can route matching MSG frames to the sender.

A topic with `*` or `?` is a pattern: the peer forwards every topic it matches. A peer holds at most 1,000,000 topics of each node, names of at most 256 bytes (the longest a node subscribes); anything past that is ignored.

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

The sender's whole interest. The receiver replaces what it held for the sender with these topics. A list over the frame limit is split: the first frame (flags 0) replaces, the following ones (flags 0x01) extend, and only within the same link.

The cluster manager sends it when a link comes up, queued in the same order as every SUB and UNSUB after it, so a subscription made while the link came up reaches the peer after the snapshot, never before it. It sends it again on every link every 60 seconds (`CLUSTER_RESYNC_INTERVAL_SECS`), whole or not at all, so a SUB or UNSUB dropped on a full link heals.

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

Sent by both sides when a link comes up. Contains the known peer addresses, so a newly joined node connects to every member of the mesh.

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
[header: type=0x0C, flags, topic_len=0, payload_len=M]
[M bytes: JSON state]
```

The sender's whole local presence, as a sync round: one frame or several, flags `0x04` on each, `0x01` on the first and `0x02` on the last. The receiver merges every frame as it comes, and at the end of the round drops whatever it held for the sender that the round did not hold, so a leave lost on the way leaves no ghost. The cluster manager sends a round when a link comes up, in order with every PresenceUpdate after it, and again, on the next 60-second tick, to a peer whose link was too full for a presence update. Frames without the flags (from older nodes) are merged as they come.

The payload is presence state as JSON:

```json
{
  "chat-1": {
    "alice": {"data": {"status": "online"}, "updated_at": 1708441800000},
    "bob": {"data": {"status": "away"}, "updated_at": 1708441795000}
  }
}
```

The receiver merges this state into its own presence tables, creating **per-origin** sentinel entries (`__remote__<origin>\x1F<user_id>`, one per origin+user) for remote users, so one peer's leave or disconnect can't erase a user still present via another peer. Only entries with local connections (not remote ones) are sent, so state never echoes around the mesh.

### DRAIN (0x0D) - Lame Duck

```
[header: type=0x0D, all other fields zero]
```

Sent when a node begins graceful drain. On receipt, a peer clears the draining
node's interest entries so it stops routing **new** messages there, but keeps the
TCP connection open so in-flight delivery can finish (lame-duck). See
[Drain Notification](#drain-notification).

## Interest-Based Routing

Without interest routing, every MSG is forwarded to every peer regardless of whether that peer has subscribers for the topic. This works but wastes bandwidth in clusters where topics are partitioned across nodes.

With CAP_INTEREST_ROUTING negotiated, nodes only forward messages to peers that have declared interest in the topic:

1. Client on Node A subscribes to `"prices"`
2. Node A sends SUB(`"prices"`) to all peers
3. Node B records that Node A is interested in `"prices"`
4. When Node B broadcasts to `"prices"`, it forwards the MSG to Node A
5. When the last subscriber on Node A leaves `"prices"`, Node A sends UNSUB(`"prices"`)

Subscription tracking uses reference counting. The first local subscriber for a topic triggers a SUB to peers; the last unsubscribe triggers an UNSUB. This prevents duplicate SUB/UNSUB messages when multiple local clients share the same topic.

**When a link comes up**, each side sends a RESYNC with its full topic list, and the receiving node replaces any stale interest for that peer. Until the first RESYNC arrives, the safe default is to forward every MSG to that peer (no message is lost meanwhile).

**Known trade-off**: a SUB message may arrive at a peer after a MSG for that topic has already been sent. This is an accepted at-most-once delivery semantic - the alternative (buffering all messages until SUB confirmation) adds latency and complexity without meaningful benefit for real-time workloads.

## Compression

MSG frames with payloads above 256 bytes may be zstd-compressed when both peers have negotiated CAP_COMPRESSION. The FLAG_COMPRESSED bit (bit 0 of the flags byte) indicates a compressed payload.

Compression is opportunistic: the sender compresses the payload and only uses the compressed form if it is smaller than the original. If compression does not reduce size, the uncompressed payload is sent with FLAG_COMPRESSED cleared.

**Decompression bomb protection**: decompressed output is capped at 1 MB (MAX_FRAME_SIZE) via `zstd::bulk::decompress`. This prevents a malicious or corrupted peer from sending a small compressed payload that expands into an arbitrarily large buffer.

## mTLS

Optional mutual TLS provides authentication and encryption for inter-node traffic. When enabled, both sides of every peer connection verify each other's certificate against a shared CA.

| Parameter | Value |
|-----------|-------|
| TLS library | rustls + tokio-rustls |
| Curve | P-256 (ECDSA) |
| Client verification | WebPkiClientVerifier (mutual) |
| Certificate model | Single cert/key pair for both server and client roles |

**Configuration**: pass `tls_ca`, `tls_cert`, and `tls_key` file paths to `connect_cluster()`. All three must be PEM-encoded. The node certificate is used for both accepting inbound connections (server role) and initiating outbound connections (client role).

When TLS is configured, all peer connections are upgraded to TLS immediately after TCP connect, before the HELLO handshake. Plaintext connections are not accepted when TLS is enabled.

## Dynamic Peer Discovery

WSE supports two modes of cluster formation:

**Static**: pass a list of peer addresses to `connect_cluster()`. Each node connects to every listed peer.

**Dynamic**: nodes discover each other through address gossip. A node joining the cluster connects to one or more seed nodes, sends a PeerAnnounce with its own address, and receives a PeerList of known members. The node then connects to any peers it does not already have connections to.

Gossip is lightweight: PeerAnnounce and PeerList go out when links come up, and a node relays a PeerAnnounce to its other peers only for an address it did not know yet.

## Reliability

| Mechanism | Configuration |
|-----------|--------------|
| Heartbeat | PING every 5s, timeout 15s |
| Circuit breaker | 10-failure threshold, 60s reset |
| Reconnect | Exponential backoff: 1s initial, 1.5x multiplier, 60s max, with jitter |
| Graceful shutdown | SHUTDOWN frame sent to all peers, then immediate cancel |
| Dead letter queue | 1,000 entries, FIFO eviction |
| Write batching | 64 KB batch limit per flush |
| Backpressure | Bounded per-peer outbound channel (10,000 frames), messages dropped on overflow. The inbound MSG dispatch queue per peer is likewise bounded (10,000); overflow is shed (counted in `messages_dropped`) so the reader stays responsive to heartbeats, and shed messages remain recoverable via recovery buffers. Interest and presence updates reach the manager through bounded queues (65,536); a peer that sends them faster than the manager keeps up waits, slowed down at its socket |

### Circuit Breaker

Each peer connection has an independent circuit breaker. After 10 consecutive failures (connection refused, handshake timeout, write error), the breaker opens and the node stops attempting to reconnect for 60 seconds. After the reset interval, up to 3 probe connections are attempted in half-open state. If 3 consecutive probes succeed, the breaker closes and normal operation resumes.

### Dead Letter Queue

Publishes and presence updates for a peer whose link has just closed (its channel is gone) go to a per-node dead letter queue; ones dropped on a full channel are only counted (`messages_dropped`). The DLQ holds up to 1,000 entries with FIFO eviction - oldest entries are discarded when the queue is full. `get_cluster_dlq_entries()` takes them, and `health_snapshot()` reports `cluster_dlq_size`.

### Write Batching

The writer task for each peer connection uses a drain loop: it blocks on the first message, then drains all immediately available messages from the channel into a single write buffer (up to 64 KB). This coalesces multiple small messages into fewer syscalls, reducing per-message overhead at high throughput.

## Topology

WSE clusters use a full-mesh topology. Every node maintains a direct TCP connection to every other node. This provides the lowest possible latency (single hop) and simplest routing (no forwarding, no leader election).

Full mesh scales well for the target deployment size of 2 to 20 nodes. For larger clusters, interest-based routing significantly reduces per-node bandwidth by filtering messages at the source.

## Topology API

The `cluster_info()` method exposes the current cluster topology from the perspective of the local node:

```python
peers = server.cluster_info()
# [
#     {"address": "10.0.0.2:9999", "instance_id": "a1b2c3...", "capabilities": 15, "connected_at": 1731470000123},
#     {"address": "10.0.0.3:41822", "instance_id": "d4e5f6...", "capabilities": 15, "connected_at": 1731470000456},
# ]
```

One entry per live link:

| Field | Description |
|-------|-------------|
| `address` | The address this node dialed, or, for a link the peer dialed, the socket address it came from |
| `instance_id` | Unique identifier assigned at peer startup (UUID) |
| `capabilities` | The capabilities negotiated with this peer (bitmask) |
| `connected_at` | When the link came up (milliseconds since the epoch) |

Use `cluster_info()` for monitoring dashboards, verifying that gossip discovery has converged, and debugging connectivity issues between nodes.

## Drain Notification

When a node initiates graceful drain via `drain()`, the internal `ClusterCommand::Drain` is sent on the wire as a **DRAIN frame (0x0D)** to all connected peers before closing client connections. On receipt, each peer:

1. Clears the draining node's interest entries so it stops routing **new** messages to it
2. Keeps the TCP connection open so any in-flight delivery can finish (lame-duck)
3. Redirects interest-routed traffic to remaining nodes

The drain notification is a best-effort signal. If the draining node loses connectivity before the DRAIN frame is delivered, peers detect the failure through the standard heartbeat timeout (15s) and remove the node automatically.
