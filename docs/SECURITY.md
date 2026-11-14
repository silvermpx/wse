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
    # jwt_private_key=open("private_key.pem", "rb").read(), # only for server.issue_token()
    jwt_issuer="auth-service",
    jwt_audience="my-api",
)

# ES256 (asymmetric -- external auth service signs with EC P-256 private key)
server = RustWSEServer(
    host="0.0.0.0",
    port=5006,
    jwt_secret=open("ec_public.pem", "rb").read(),       # EC P-256 public key (PEM)
    jwt_algorithm="ES256",
    # jwt_private_key=open("ec_private.pem", "rb").read(),  # only for server.issue_token()
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

**Issuing tokens:** `server.issue_token(sub, ttl=900, claims=None)` signs a token with the server's own configuration (algorithm, key, `jwt_key_id`, issuer and audience), for a login endpoint in the same process. The HMAC algorithms sign with `jwt_secret`; the asymmetric ones need `jwt_private_key`, kept in memory that is wiped when the server is dropped.

**Key requirements:** HS256 secret must be >= 32 bytes (RFC 7518). RS256/ES256 keys must be valid PEM. Tokens without `exp` are rejected.

**Expiry of open connections:** a connection is closed with code 4403 when the token it opened with expires (its `exp` plus the 30-second skew). The clients reconnect with a fresh token from their token provider; without one they stop. Keep tokens short-lived: an open WebSocket is authorized exactly as long as its token.

**Key rotation:** Set `jwt_previous_secret` to the old key when rotating. For HS256: old shared secret. For RS256/ES256: old public key PEM. Tokens signed with either key are accepted during the transition window. Remove `jwt_previous_secret` after all old tokens have expired. A running server takes new keys with `server.reload_jwt_keys(new_key, jwt_previous_secret=old_key)`, no restart needed; open connections keep running until their tokens expire.

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

## End-to-End Encryption

TLS protects a connection up to whatever terminates it: a load balancer, a reverse proxy or a CDN in front of the server sees every message in clear. E2E encryption seals each frame for the server process itself.

### Setting it up

1. Give the server an identity key, an ECDSA P-256 private key in PEM or DER (PKCS#8):

   ```bash
   openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out e2e-identity.pem
   ```

   ```python
   server = RustWSEServer(
       "0.0.0.0", 5007,
       jwt_secret=...,
       e2e_identity_key=open("e2e-identity.pem", "rb").read(),
   )
   server.e2e_identity_public_key()  # base64: what the clients pin
   ```

2. Pin its public key in the clients:

   ```python
   client = AsyncWSEClient(url, token=token, encryption=True, e2e_server_keys=[SERVER_KEY])
   ```

   ```typescript
   useWSE(token, topics, { security: { encryptionEnabled: true, serverKeys: [SERVER_KEY] } });
   ```

   A client that cannot pin a key, during development for instance, may accept any server with `e2e_unauthenticated=True` (Python) or `security.unauthenticated: true` (TypeScript). The session is then safe from anyone who only listens, not from anyone who can answer in the server's place. With encryption on, a client refuses to start without one of the two.

**Rotating the identity key:** clients pin a list. Ship clients that pin the old and the new key, switch the server to the new one, then drop the old key from the clients.

### What a session guarantees

| | |
|-|-|
| Server authenticity | The server signs the handshake (ES256 over the whole exchange: the suites offered, both sides' ephemeral keys, the client's random, the connection id and the key id); the client checks the signature against the keys it pins |
| Forward secrecy | Every connection uses fresh ECDH P-256 keys on both sides; the identity key only signs, so losing it later exposes no recorded session |
| Confidentiality and integrity | AES-256-GCM, with keys from HKDF-SHA256 over the ECDH secret salted with the transcript hash: one key schedule per direction |
| Order | Each frame carries its direction's epoch and sequence number, authenticated with it, and only the next frame opens: a replayed, reordered, dropped, reflected or forged frame closes the connection (4802) |
| Key lifetime | Each direction moves to a new key after 2^20 frames or 10 minutes; past 2^53 frames the connection closes (4803) |
| Coverage | After `server_hello` every frame is sealed, both ways: application messages, pings and PONGs, subscription requests and replies, presence events, recovery replays, messages relayed from other cluster nodes. A plaintext data frame inside a session closes the connection (4802) |

It does not hide the size and timing of frames (the server does not compress for E2E connections, so sizes say nothing about content), and the server process sees the plaintext: E2E protects the path between client and server, not the server.

[PROTOCOL.md](PROTOCOL.md#end-to-end-encryption) specifies the handshake, the transcript and the frames. `tests/vectors/e2e.json`, made with an independent implementation, holds test vectors the server and both clients check.

### Close codes

| Code | Meaning |
|------|---------|
| 4800 | No E2E suite in common, or a first-design client on a server with `e2e_legacy=False` |
| 4802 | A frame out of order, forged, or in plaintext inside a session |
| 4803 | The frame counter is used up |

A client that refuses the server's identity or one of its frames ends the connection itself, stops reconnecting and reports an encryption error. Nothing is sent in plaintext instead.

### The first design (2.x clients)

`e2e_legacy=True`, the default, keeps serving 2.x clients, and 3.x clients reach 2.x servers with `e2e_legacy=True` (Python) or `security.legacyE2e: true` (TypeScript). That design is ECDH P-256 per connection, HKDF-SHA256 (no salt, info `wse-encryption/aes-gcm-key`) and one AES-256-GCM key for both directions with a random 12-byte nonce per message (`E:` + nonce + ciphertext + tag). It has no server identity, no sequence numbers and no rekeying: whoever can answer the handshake can read and change the session, and frames can be replayed. Set `e2e_legacy=False` once every client is on 3.x.

## Signed Messages

TLS and the E2E sessions protect a message on its way, but they leave no proof: afterwards nobody can show a third party that a client sent a given order. A signed message carries that proof. It is signed with the sender's own private key, which only the sender holds, over the exact text that travels, and whoever keeps the text, the signature and the key id can verify later who sent what.

Use it for the messages that may be disputed: orders, cancellations, fills, balance changes. Every message signed costs a few tens of microseconds on each side (ECDSA P-256: about 11 µs to sign, 37 µs to verify), so it is not for every tick of a price feed.

### Setting it up

1. Give the server a signing key, an ECDSA P-256 private key in PEM or DER (PKCS#8), apart from its E2E identity key:

   ```bash
   openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out signing.pem
   ```

   ```python
   server = RustWSEServer("0.0.0.0", 5007, jwt_secret=..., signing_key=open("signing.pem", "rb").read())
   server.signing_public_key()  # base64: what the clients pin
   ```

2. Each client makes its own key pair and keeps the private key on the device: `generate_signing_key()` (Python) or `generateSigningKey()` (TypeScript, a WebCrypto key that cannot be exported; IndexedDB stores it as is).

3. Bind the client's public key to the user, one of two ways:

   - **In the token.** The application's backend puts the public key in the JWT the client connects with, as RFC 7800's confirmation claim: `"cnf": {"jwk": {"kty": "EC", "crv": "P-256", "x": ..., "y": ...}}` (`public_key_jwk()` / `SigningKey.jwk` give it).
   - **By the application.** `server.set_client_key(conn_id, public_key)` binds a key to a connection whatever way the application checked it: its own key registry, an admin, or a passkey (WebAuthn) assertion over the new key, verified with a standard WebAuthn library. That last one proves a person approved the key.

   Non-repudiation is only as strong as this binding: whoever can bind a key to the user can sign as the user.

4. The clients pin the server's signing key (`server_signing_keys` / `security.serverSigningKeys`) and send with `send(..., signed=True)` / `sendSigned()`.

### What it guarantees

| | |
|-|-|
| Origin | Only the holder of the private key can produce the signature; the server checks it against the key bound to the connection |
| Integrity | The signature covers the body byte for byte; one changed byte fails it |
| One server, one time, once | The body names the server's key (`aud`), carries a time the server accepts only within `signature_window`, and an `n` the server takes once per key within that window |
| Proof for later | The application gets the body, signature and key id with every signed message; keeping them, with the key binding, is what lets anyone verify the message years later |
| Receipt | The server answers every signed message it takes with a signed receipt: the client can show what the server received and when |

The server remembers `n` within the window on its own node only. A message replayed to another node of a cluster within the window is taken there too: the application deduplicates by `n` where it executes the order (an order id it already has is a no-op).

The recovery stamp (`tp` / `e` / `o`) is outside the signature: it is the server's delivery position, protected by TLS and E2E. A topic message's signed body names its topic, and clients drop one that comes on another.

**Rotating the server's key:** clients pin a list. Pin the old and the new key, switch the server, then drop the old one; keep old public keys wherever signatures must still be verified.

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

**Pattern subscriptions:** a subscription to a name with `*` or `?` receives the matching topics, and for a connection with an ACL only those the ACL allows: with the ACL above, a subscription to `tenant:acme:*` gets `tenant:acme:prices` but never `tenant:acme:internal:logs`. A connection holds at most 16 pattern subscriptions; every publish is matched against the patterns, never against all topics. Topic names are at most 256 bytes, without control characters; `subscribe_connection` returns the topics it took.

**Security considerations:**

- ACLs are enforced at subscribe time, **on every delivery to a pattern subscription** and **on recovery replay**: `subscribe_with_recovery` filters replayed topics through the same ACL, fail-closed — an unauthorized topic is never replayed and its position is never probed (no existence/count leak). Call `set_topic_acl` before `subscribe_connection`.
- ACLs are per-connection, not per-user. Different connections from the same user can have different ACLs.
- Without ACLs, all topics are accessible (backward compatible).
- Combine with JWT `sub` claim for user-level authorization: read the user's role from the JWT payload in your drain loop, then set appropriate ACLs.

## Wire-Level Security

### Cluster Authentication

- **With TLS**: mutual TLS (mTLS) provides both encryption and authentication on the dedicated `cluster_port`
- **Without TLS**: links are plaintext and unauthenticated: anyone who reaches the cluster port can join as a peer, read every topic and publish to any. Keep `cluster_port` on a private network (`cluster_host` binds it to one interface)
- The WebSocket port takes cluster links only with `connect_cluster(..., single_port=True)`, which shares that port with clients: use it only where the WebSocket port, too, is reachable from the cluster's own network alone. Otherwise cluster frames arriving there are refused
- At most 64 inbound links may wait for their HELLO at once, 4 from one address, and at most 256 peer sessions are open

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

## Best Practices

1. **Always use TLS** (`wss://`) in production
2. **Configure origin validation** (`allowed_origins`, or your reverse proxy) to prevent CSWSH
3. **Use asymmetric algorithms (RS256/ES256)** when tokens are issued by an external auth service
4. **Rotate JWT keys** periodically using `jwt_previous_secret`
5. **Enable E2E encryption with a pinned server identity** when something between clients and the server (a TLS-terminating proxy, a CDN) must not read the messages
6. **Enable mTLS** for cluster peer connections in production
7. **Monitor circuit breaker** state for connection health
8. **Use HTTP-only cookies** for web clients (prevents XSS token theft)
9. **Use separate CA** for cluster certificates (isolate trust domains)
