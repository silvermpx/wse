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

Both paths use HS256 (HMAC-SHA256) JWT validation.

### JWT Validation

JWT validation runs entirely in Rust during the WebSocket handshake. Zero Python involvement, zero GIL acquisition.

```python
server = RustWSEServer(
    host="0.0.0.0",
    port=5006,
    max_connections=10000,
    jwt_secret=b"replace-with-a-strong-secret-key!",
    jwt_issuer="your-app",      # optional: validate iss claim
    jwt_audience="your-api",    # optional: validate aud claim
    jwt_cookie_name="access_token",  # optional: cookie name for JWT (default: "access_token")
)
```

The Rust server extracts the JWT cookie (configured via `jwt_cookie_name`, default `access_token`) from the HTTP upgrade headers and validates:
1. HS256 signature (constant-time comparison)
2. `exp` claim (reject expired tokens, boundary check per RFC 7519)
3. `iss` claim (if `jwt_issuer` configured)
4. `aud` claim (if `jwt_audience` configured)

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
- Server sends `error` message with code `AUTH_FAILED`
- Connection closed with code `4401`

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

WSE validates the `Origin` header to prevent Cross-Site WebSocket Hijacking (CSWSH). Configure allowed origins via the `ALLOWED_ORIGINS` environment variable:

```bash
# Comma-separated list of allowed origins
ALLOWED_ORIGINS=https://app.example.com,https://admin.example.com
```

When `ALLOWED_ORIGINS` is set, connections from unlisted origins are rejected with close code `4403`. When not set (development mode), all origins are allowed.

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
  "v": 1,
  "t": "action_completed",
  "p": {"item_id": "abc123", "status": "done"},
  "sig": "eyJhbGciOiJIUzI1NiJ9.eyJoYXNoIjoiYWJjZGVmLi4uIiwic2lnbmVkX2F0IjoxNzA4NDQxODAwfQ.xxxxx"
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
- Session key derivation via HKDF-SHA256 (salt: `wse-encryption`, info: `aes-gcm-key`)
- AES-GCM-256 encrypt/decrypt via `aes-gcm` crate
- Per-connection key lifecycle: generate on handshake, derive on client key exchange, clear on disconnect

**Client-side encryption (`security.ts`):**
- AES-GCM-256 with unique IVs per message (IV reuse prevention)
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
openssl ecparam -genkey -name prime256v1 -out ca.key
openssl req -new -x509 -key ca.key -out ca.pem -days 365 -subj "/CN=WSE CA"

openssl ecparam -genkey -name prime256v1 -out node.key
openssl req -new -key node.key -out node.csr -subj "/CN=wse-node-1"
openssl x509 -req -in node.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out node.pem -days 365
```

## Connection Security

### Rate Limiting

Per-connection token bucket rate limiter:
- **Capacity**: 100,000 tokens
- **Refill rate**: 10,000 tokens/second
- **Exceeded**: client receives error with code `RATE_LIMITED`
- **Warning**: sent at 20% remaining capacity

### Client-Side

The frontend `RateLimiter` prevents excessive sends.

### Zombie Detection

- Server pings every 25 seconds
- Connections with no activity for 60 seconds are force-closed

### Connection Limits

- `max_connections` caps total concurrent WebSocket connections
- Inbound queue bounded at 131,072 events
- Per-connection deduplication: 50,000-entry AHashSet with FIFO eviction

## Wire-Level Security

### Cluster Frame Protection

- Maximum frame size: 1,048,576 bytes (1 MB)
- zstd decompression output capped at 1 MB (MAX_FRAME_SIZE)
- Protocol version validation in handshake
- Unknown message types silently ignored (forward compatibility)

### Client Frame Protection

- WebSocket frame size limited to 1 MB
- JSON parsing with serde_json (no eval, no injection)
- User ID from JWT escaped via serde_json in server_ready
- Binary frames parsed as msgpack or raw bytes (no code execution)

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
2. **Configure ALLOWED_ORIGINS** to prevent CSWSH
3. **Rotate JWT secrets** periodically
4. **Enable signing** for critical operations
5. **Enable encryption** for sensitive data (PII, credentials)
6. **Enable mTLS** for cluster peer connections in production
7. **Monitor circuit breaker** state for connection health
8. **Use HTTP-only cookies** for web clients (prevents XSS token theft)
9. **Use separate CA** for cluster certificates (isolate trust domains)
