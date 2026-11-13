# WSE Security

## Authentication

### Authentication Methods by Client Type

WSE supports three authentication methods, chosen based on client type:

| Client Type | Method | How |
|-------------|--------|-----|
| **Browser (TS/React)** | HTTP-only Cookie | Cookie with `httpOnly + secure + sameSite=Lax` (default name: `access_token`, configurable via `jwt_cookie_name`) |
| **Backend (Python)** | Cookie + Authorization header | Sends both; server reads whichever is available |
| **API / CLI** | Authorization header | `Authorization: Bearer <JWT>` |

**Why cookies for browsers (OWASP recommended):**
- Browsers **cannot** set custom headers (`Authorization`) during the WebSocket upgrade handshake - the `new WebSocket(url)` API has no header parameter
- HTTP-only cookies are immune to XSS (JavaScript cannot read them)
- `sameSite=Lax` prevents CSRF on cross-origin requests
- `secure` ensures HTTPS-only transmission
- Cookies are automatically attached to the WebSocket upgrade request by the browser

**Why NOT query parameters:**
- Query strings leak in server access logs, browser history, and referrer headers
- WSE does NOT recommend `?token=<JWT>` for production use

### Server-Side Token Extraction Order

The Rust server reads authentication credentials in this order:

1. **HTTP-only cookie**: from the `Cookie` header (default name: `access_token`, configurable via `jwt_cookie_name` constructor parameter)
2. **Authorization header**: `Authorization: Bearer <JWT>` (fallback - covers backend clients)

Both paths use the configured JWT algorithm (HS256 by default, RS256 and ES256 also supported).

### JWT Validation

JWT validation runs entirely in Rust during the WebSocket handshake. Zero Python involvement, zero GIL acquisition.

```python
# HS256 (default -- symmetric, shared secret)
server = RustWSEServer(
    host="0.0.0.0",
    port=5006,
    max_connections=10000,
    jwt_secret=b"replace-with-a-strong-secret-key!",
    jwt_issuer="your-app",           # optional: validate iss claim
    jwt_audience="your-api",         # optional: validate aud claim
    jwt_cookie_name="access_token",  # optional: cookie name (default: "access_token")
    jwt_previous_secret=None,        # optional: previous secret for key rotation
    jwt_key_id=None,                 # optional: expected kid header claim
)

# Any standard algorithm: HS256/384/512, RS256/384/512, PS256/384/512,
# ES256/384 and EdDSA (Ed25519). The server checks tokens with exactly the
# configured one; a token's own "alg" header never chooses it.

# RS256 (asymmetric -- external auth service signs with RSA private key)
server = RustWSEServer(
    host="0.0.0.0",
    port=5006,
    jwt_secret=open("public_key.pem", "rb").read(),      # RSA public key (PEM)
    jwt_algorithm="RS256",
    # jwt_private_key=open("private_key.pem", "rb").read(), # only if server needs to encode tokens
    jwt_issuer="auth-service",
    jwt_audience="my-api",
)

# ES256 (asymmetric -- external auth service signs with EC P-256 private key)
server = RustWSEServer(
    host="0.0.0.0",
    port=5006,
    jwt_secret=open("ec_public.pem", "rb").read(),       # EC P-256 public key (PEM)
    jwt_algorithm="ES256",
    # jwt_private_key=open("ec_private.pem", "rb").read(),  # only if server needs to encode tokens
    jwt_issuer="auth-service",
    jwt_audience="my-api",
)
```

The Rust server extracts the JWT from cookies (configured via `jwt_cookie_name`) or the `Authorization: Bearer <token>` header and validates:

1. Token size limit (8KB max, prevents DoS)
2. Algorithm enforcement (configured algorithm only, not derived from token header -- prevents algorithm confusion attacks)
3. `kid` claim (if `jwt_key_id` configured, checked before signature)
4. Signature verification via jsonwebtoken crate (HS256: HMAC-SHA256, RS256: RSA PKCS#1v1.5, ES256: ECDSA P-256)
5. `exp` claim (required, 30s clock skew tolerance per RFC 8725)
6. `nbf` claim (if present, 30s clock skew tolerance per RFC 7519)
7. `iss` claim (if `jwt_issuer` configured)
8. `aud` claim (if `jwt_audience` configured, supports string or array per RFC 7519)

**Supported algorithms:**

| Algorithm | Type | `jwt_secret` | `jwt_private_key` |
|-----------|------|-------------|-------------------|
| HS256 (default) | Symmetric | Shared secret (>= 32 bytes) | Not needed |
| RS256 | Asymmetric | RSA public key (PEM) | RSA private key (PEM, for encoding) |
| ES256 | Asymmetric | EC P-256 public key (PEM) | EC P-256 private key (PEM, for encoding) |

**Key requirements:** HS256 secret must be >= 32 bytes (RFC 7518). RS256/ES256 keys must be valid PEM. Tokens without `exp` are rejected.

**Key rotation:** Set `jwt_previous_secret` to the old key when rotating. For HS256: old shared secret. For RS256/ES256: old public key PEM. Tokens signed with either key are accepted during the transition window. Remove `jwt_previous_secret` after all old tokens have expired.

On success, the server sends `server_ready` directly from Rust and pushes an `AuthConnect` event to the drain queue with the validated `user_id`.

**Performance:** 0.01ms per JWT decode. Connection latency: 0.53ms median.

**Token requirements:**
```json
{
  "sub": "<user-id>",
  "exp": 1708441800,
  "iat": 1708440000,
  "iss": "your-app",
  "aud": "your-api"
}
```

The `sub` claim is returned as `user_id` in the `auth_connect` drain event. `iss` and `aud` are validated only if configured in the constructor.

When `jwt_secret` is `None`, authentication is disabled and all connections receive a `connect` event instead of `auth_connect`.

On authentication failure:
- Server sends `error` message with code `AUTH_FAILED` or `AUTH_REQUIRED`
- Connection closed with close code `4401`

### Client Authentication Examples

**Browser (TS/React client):**
```typescript
// Cookie is set by your login endpoint (name must match jwt_cookie_name):
// Set-Cookie: access_token=<JWT>; HttpOnly; Secure; SameSite=Lax; Max-Age=900
// The browser attaches it automatically on WebSocket upgrade
const ws = new WebSocket('wss://host:port/wse');
```

**Python client (backend):**
```python
from wse_client import connect

async with connect("ws://localhost:5006/wse", token="<JWT>") as client:
    # Sends JWT as both Cookie and Authorization header
    await client.subscribe(["events"])
```

**API client (curl/httpie):**
```bash
# Authorization header only
wscat -c "ws://localhost:5006/wse" -H "Authorization: Bearer <JWT>"
```

## Transport Security

### TLS

Always use `wss://` (WebSocket Secure) in production. WSE does not enforce TLS at the application layer - configure your reverse proxy (nginx, Caddy) or load balancer.

### Origin Validation

A page on another site, even a sibling subdomain that browsers treat as
same-site, can open a WebSocket to your server with the victim's cookies
(Cross-Site WebSocket Hijacking, CSWSH). Pass the origins your pages are
served from, and the server answers an upgrade from any other origin with
HTTP 403:

```python
server = RustWSEServer(
    "0.0.0.0", 5007,
    jwt_secret=secret,
    allowed_origins=["https://app.example.com", "https://admin.example.com"],
)
```

Origins are compared as `scheme://host[:port]`, case-insensitively, with the
scheme's default port optional. Upgrades without an `Origin` header (clients
other than browsers) are not affected. The default `None` checks nothing.

The same check can live in your reverse proxy instead:

```nginx
# nginx: reject WebSocket upgrades from unknown origins
map $http_origin $origin_allowed {
    default 0;
    "https://app.example.com" 1;
    "https://admin.example.com" 1;
}
server {
    location /wse {
        if ($origin_allowed = 0) { return 403; }
        # ... proxy_pass etc.
    }
}
```

JWT authentication provides the primary access control. Origin validation adds defense-in-depth against CSWSH.

## Message Security

### Message Signing (Integrity)

WSE uses selective message signing: only critical operations are signed, not data updates or status messages.

**Signing process (server):**
1. Serialize payload to JSON with sorted keys
2. Compute SHA-256 hash of serialized payload
3. Create JWT with `hash`, `signed_at`, and random `nonce`
4. Attach as `sig` field on the message

```json
{
  "c": "U",
  "t": "action_completed",
  "p": {"item_id": "abc123", "status": "done"},
  "sig": "eyJhbGciOiJIUzI1NiJ9.eyJoYXNoIjoiYWJjZGVmLi4uIiwic2lnbmVkX2F0IjoxNzA4NDQxODAwfQ.xxxxx",
  "v": 1
}
```

**Verification (client):**
1. Decode JWT signature
2. Recompute SHA-256 hash of received payload
3. Compare hashes
4. Check nonce hasn't been seen before (replay prevention)

Configure which event types require signing based on your application's security requirements. Typical candidates include state-changing operations, financial transactions, and configuration changes.

### End-to-End Encryption

WSE supports optional E2E encryption using Web Crypto API:

**Algorithms:**
| Purpose | Algorithm |
|---------|-----------|
| Encryption | AES-GCM-256 |
| Key Exchange | ECDH P-256 |
| Key Derivation | HKDF-SHA256 |
| Signing | HMAC-SHA256 |

**Key exchange flow:**
1. Client generates ECDH P-256 key pair
2. Server generates ECDH P-256 key pair
3. Public keys exchanged via `encryption_request`/`encryption_response`
4. Both derive shared secret via ECDH
5. Shared secret -> HKDF -> AES-GCM-256 encryption key

**Wire format (encrypted):**
```
E:<IV (12 bytes)><AES-GCM ciphertext>
```

**Server-side encryption (Rust):**
- ECDH P-256 key pair generated per connection via `p256` crate
- Session key derivation via HKDF-SHA256 (no salt, info: `wse-encryption/aes-gcm-key`)
- AES-GCM-256 encrypt/decrypt via `aes-gcm` crate
- Per-connection key lifecycle: generate on handshake, derive on client key exchange, clear on disconnect
- Recovery replay: buffered messages are stored **unframed**; each is framed — or encrypted then framed — per connection at replay time, so a plaintext frame is never replayed to an E2E-encrypted connection

**Client-side encryption (`security.ts`):**
- AES-GCM-256 with a fresh random IV per message; the recent-IV set is checked **before** each encryption (redraw on collision, and refuse to encrypt rather than reuse an IV)
- Nonce cache for replay attack prevention (5-minute window, 10K max)
- Automatic key rotation (configurable interval, default: 1 hour)
- Constant-time string comparison for security checks
- Cleanup on destroy (keys, timers, caches cleared from memory)

**Enable encryption:**

```typescript
// TypeScript/React client
const { } = useWSE({
  security: { encryptionEnabled: true },
});
```

```python
# Python client
from wse_client import AsyncWSEClient

# Encryption is negotiated during the handshake when crypto extras are installed
async with AsyncWSEClient("ws://localhost:5006/wse", token="<jwt>") as client:
    await client.subscribe(["secure-topic"])
```

The key exchange happens automatically during the `client_hello`/`server_hello` handshake. The `SecurityManager` handles ECDH key exchange, AES-GCM encryption/decryption, and key rotation. Incoming `E:`-prefixed binary frames are decrypted transparently by the message processor.

## Cluster Security

### Mutual TLS (mTLS)

All cluster peer connections support mutual TLS for authentication and encryption.

Configuration:
```python
server.connect_cluster(
    peers=["10.0.0.2:9999"],
    tls_ca="/etc/wse/ca.pem",
    tls_cert="/etc/wse/node.pem",
    tls_key="/etc/wse/node.key",
    cluster_port=9999,
)
```

Details:
- TLS library: rustls + tokio-rustls
- Certificate curves: P-256 (NIST)
- Client verification: WebPkiClientVerifier (both sides present and verify certificates)
- Single certificate/key pair for both server and client roles
- TLS version: 1.3 by default (1.2 fallback)
- Without TLS config, cluster connections use plaintext TCP (trusted networks only)

Certificate generation example:
```bash
# Certificate authority shared by every node
openssl ecparam -genkey -name prime256v1 -out ca.key
openssl req -new -x509 -sha256 -key ca.key -out ca.pem -days 365 -subj "/CN=WSE CA"

# Node certificate. A peer checks the address it dials against the
# certificate's subjectAltName (the CN is not used), so list every DNS name
# and IP address other nodes use to reach this one.
printf 'subjectAltName = DNS:wse-node-1, IP:10.0.0.11\nextendedKeyUsage = serverAuth, clientAuth\n' > node.ext
openssl ecparam -genkey -name prime256v1 -out node.key
openssl req -new -key node.key -out node.csr -subj "/CN=wse-node-1"
openssl x509 -req -sha256 -in node.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out node.pem -days 365 -extfile node.ext
```

The same commands work with OpenSSL and with LibreSSL (the `openssl` on macOS). Keep `-sha256`: LibreSSL otherwise signs with SHA-1, which rustls rejects, and without `-extfile` it issues a version 1 certificate, which rustls rejects as well. A certificate without a matching subjectAltName fails every handshake with "certificate not valid for name".

## Crypto Library

One crypto library serves everything that encrypts or signs: cluster mTLS,
JWT validation, end-to-end encryption and the `rust_*` crypto helpers. It is
chosen when the extension is built:

| Build feature | Library | Used by |
|---|---|---|
| `aws-lc` (default) | aws-lc-rs | the wheels on PyPI |
| `fips` | aws-lc-rs on its FIPS 140-3 validated module | builds from source |

A FIPS build needs CMake and Go, and is made from the source distribution:

```bash
MATURIN_PEP517_ARGS="--features fips" pip install wse-server --no-binary wse-server
```

`health_snapshot()["crypto"]` and the Prometheus metric
`wse_build_info{crypto="..."}` report which library a running server uses.
The cluster's TLS gets its provider from that library explicitly; WSE never
installs a process-wide rustls provider, so the application's own TLS
settings are left alone.

## Connection Security

### Rate Limiting

The messages each client sends go through a per-connection token bucket
before the server parses them (`rate_limit_capacity`, `rate_limit_refill`):
- **Capacity**: 100,000 messages
- **Refill rate**: 10,000 messages/second
- **Exceeded**: the message is dropped; the client receives an error with code `RATE_LIMITED` (at most once a second)
- **Warning**: sent at 20% remaining capacity
- **Abuse**: a client that keeps sending into the empty bucket for 10 seconds is closed with code 4429

Callback mode additionally bounds the callbacks in flight per connection
(256). In both modes the client messages waiting for the application are
bounded by `max_inbound_queue_size` (count) and `max_inbound_queue_bytes`
(size); connect, disconnect and presence events are never dropped.

### Client-Side

The frontend `RateLimiter` prevents excessive sends.

### Idle and Unresponsive Clients

- The server pings every `ping_interval` seconds (25 by default)
- A connection that has sent no frame for `idle_timeout` seconds (60 by default) is closed with code 4408; protocol pings count as activity
- A client that does not read: its outbound queue is capped at `max_outbound_queue_bytes`, and a queue still over it on the next ping tick closes the connection with code 1013
- Every server-initiated close gives the client 5 seconds to answer; then the socket is closed and the connection's tasks end, whether the client reads or not
- Accepted sockets use TCP keepalive, so peers that vanished without closing are found by the kernel as well

### Handshake Protection

- 10-second timeout on WebSocket upgrade handshake prevents slow loris attacks
- Connections that stall during HTTP upgrade are terminated before consuming server resources
- At most 512 connections may sit in the pre-handshake state at once (`MAX_PENDING_HANDSHAKES`); further accepts are dropped before the upgrade, so a flood of half-open sockets can't exhaust tasks/file descriptors before the connection limit applies

### Connection Limits

- `max_connections` caps total concurrent WebSocket connections; a full server answers the upgrade with HTTP 503 and `Retry-After: 5`
- `max_subscriptions_per_connection` caps topics per connection (0 = unlimited). Prevents topic explosion DoS where a single client creates millions of unique topics
- Inbound queue bounded at 131,072 events
- Per-connection deduplication: 50,000-entry AHashSet with FIFO eviction

## Topic ACL

Per-connection topic access control using allow/deny glob patterns. Enforces which topics a connection can subscribe to, providing multi-tenant isolation and role-based access at the subscription layer.

```python
# Set ACL before subscribing
server.set_topic_acl(conn_id, allow=["tenant:acme:*"], deny=["tenant:acme:internal:*"])
server.subscribe_connection(conn_id, ["tenant:acme:prices"])   # allowed
server.subscribe_connection(conn_id, ["tenant:acme:internal:logs"])  # denied
server.subscribe_connection(conn_id, ["tenant:other:prices"])  # denied (not in allow list)
```

**Evaluation order:**

1. If deny list is set and the topic matches any deny pattern, subscription is rejected
2. If allow list is set and the topic does not match any allow pattern, subscription is rejected
3. Otherwise, subscription is permitted

Deny always takes precedence over allow. Patterns support `*` (match any characters) and `?` (match single character).

**Security considerations:**

- ACLs are enforced at subscribe time **and on recovery replay**: `subscribe_with_recovery` filters replayed topics through the same ACL, fail-closed — an unauthorized topic is never replayed and its position is never probed (no existence/count leak). Call `set_topic_acl` before `subscribe_connection`.
- ACLs are per-connection, not per-user. Different connections from the same user can have different ACLs.
- Without ACLs, all topics are accessible (backward compatible).
- Combine with JWT `sub` claim for user-level authorization: read the user's role from the JWT payload in your drain loop, then set appropriate ACLs.

## Wire-Level Security

### Cluster Authentication

- **With TLS**: mutual TLS (mTLS) provides both encryption and authentication. Cluster connections are only accepted on the dedicated `cluster_port`
- **Without TLS**: cluster connections on the main WebSocket port log a security warning. For production, always enable cluster TLS or use a dedicated cluster port on a private network
- Plaintext cluster connections are accepted only when cluster is initialized and TLS is not configured

### Cluster Frame Protection

- Maximum frame size: 1,048,576 bytes (1 MB)
- zstd decompression output capped at 1 MB (MAX_FRAME_SIZE)
- Protocol version validation in handshake
- Unknown message types silently ignored (forward compatibility)

### Decompression Bomb Protection

- **Zlib**: output bounded to 10 MB. Decompression is streamed in 8 KB chunks with size check after each read. Rejects payloads that decompress beyond the limit before allocating full output.
- **Zstd (cluster)**: output bounded to 1 MB (MAX_FRAME_SIZE). Uses `zstd::bulk::decompress` with explicit capacity limit.
- **Regex cache**: FIFO eviction via IndexMap (insertion-ordered). Capped at 1024 entries. Prevents cache poisoning where an attacker sends unique patterns to evict useful compiled regexes.

### Client Frame Protection

- WebSocket frame size limited to 1 MB (`max_message_size`)
- JSON parsing with serde_json (no eval, no injection)
- User ID from JWT escaped via serde_json in server_ready
- Binary frames parsed as msgpack or raw bytes (no code execution)
- Inbound msgpack frames nested deeper than 64 levels (`MAX_MSGPACK_DEPTH`) are rejected before decoding, so a crafted deeply-nested payload can't stack-overflow the decoder
- Callback mode bounds in-flight `on_message` callbacks at 256 per connection; excess inbound work is shed (counted in `rate_limited_total`) rather than spawning unbounded blocking tasks

## Circuit Breaker

**Cluster circuit breaker** (per-peer):

| Parameter | Value |
|-----------|-------|
| Failure threshold | 10 |
| Reset timeout | 60 seconds |
| Half-open probe calls | 3 |

**Client-side circuit breaker** (TypeScript/Python SDKs):

| Parameter | Value |
|-----------|-------|
| Failure threshold | 5 |
| Reset timeout | 60 seconds |

States:
- **CLOSED**: Normal operation
- **OPEN**: Errors exceeded threshold, connection degraded
- **HALF_OPEN**: Testing recovery with limited calls

When circuit breaker opens:
1. Connection state changes to `DEGRADED`
2. Client notified via `connection_state_change`
3. After reset timeout, transitions to `HALF_OPEN`
4. On success, closes; on failure, reopens

## Replay Attack Prevention

Frontend nonce cache:
- **Max size**: 10,000 entries
- **TTL**: 5 minutes
- **Cleanup**: Every 60 seconds
- Signed messages include timestamp + nonce
- Messages older than TTL are rejected

## Best Practices

1. **Always use TLS** (`wss://`) in production
2. **Configure origin validation** (`allowed_origins`, or your reverse proxy) to prevent CSWSH
3. **Use asymmetric algorithms (RS256/ES256)** when tokens are issued by an external auth service
4. **Rotate JWT keys** periodically using `jwt_previous_secret`
5. **Enable signing** for critical operations
6. **Enable encryption** for sensitive data (PII, credentials)
7. **Enable mTLS** for cluster peer connections in production
8. **Monitor circuit breaker** state for connection health
9. **Use HTTP-only cookies** for web clients (prevents XSS token theft)
10. **Use separate CA** for cluster certificates (isolate trust domains)
