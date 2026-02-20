# WSE Security

## Authentication

### JWT Token Authentication

WSE authenticates connections via JWT tokens. The server checks multiple sources in order:

1. **Query parameter**: `?token=<JWT>` (preferred for WebSocket)
2. **Authorization header**: `Authorization: Bearer <JWT>`
3. **HTTP-only cookie**: `access_token` cookie
4. **Scope cookies**: Starlette `scope.cookies` (fallback)

Token requirements:
```json
{
  "sub": "<user-uuid>",
  "exp": 1708441800,
  "iat": 1708440000
}
```

On authentication failure:
- Server sends `error` message with code `AUTH_FAILED`
- Connection closed with code `4401`

### Refresh Token Grace Period

If the access token expired within the last 60 seconds, WSE attempts to use the `refresh_token` cookie as a short grace-period fallback. This prevents unnecessary reconnections during token refresh cycles while limiting the window of exposure. Refresh tokens older than this grace period are rejected.

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
  "v": 2,
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
| Password KDF | PBKDF2 (600,000 iterations) |

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

**Backend encryption:**
- Uses Fernet (symmetric) from the security layer
- Channel-specific tokens with 1-hour expiry
- Key rotation support via environment variables

**Frontend encryption (`security.ts`):**
- AES-GCM-256 with unique IVs per message (IV reuse prevention)
- Nonce cache for replay attack prevention (5-minute window, 10K max)
- Automatic key rotation (configurable interval, default: 1 hour)
- Constant-time string comparison for security checks
- Cleanup on destroy (keys, timers, caches cleared from memory)

**Enable encryption:**
```typescript
// Frontend -- useWSE hook initializes SecurityManager automatically
const { } = useWSE({
  security: { encryptionEnabled: true },
});

// Backend (connect URL)
ws://host:port/wse?token=<JWT>&encryption=true
```

The `SecurityManager` singleton handles ECDH key exchange, AES-GCM encryption/decryption, and key rotation. The `MessageProcessor` automatically decrypts incoming `E:`-prefixed binary frames using `securityManager.decryptFromTransport()`.

## Rate Limiting

### Server-Side

Token bucket rate limiter per connection:
- **Capacity**: 1,000 tokens
- **Refill rate**: 100 tokens/second
- **Burst**: Up to 1,000 messages instantly, then 100/sec sustained

When rate limit is exceeded:
1. Message is dropped
2. Client receives `rate_limit_warning` message
3. `retry_after` field indicates when to resume

### Client-Side

The frontend `RateLimiter` prevents excessive sends.

## Circuit Breaker

Per-connection circuit breaker (Google SRE Chapter 22 pattern):

| Parameter | Value |
|-----------|-------|
| Failure threshold | 10 |
| Success threshold | 3 |
| Reset timeout | 30 seconds |
| Half-open max calls | 5 |
| Window size | 50 |
| Failure rate threshold | 30% |

States:
- **CLOSED**: Normal operation
- **OPEN**: Errors exceeded threshold, connection degraded
- **HALF_OPEN**: Testing recovery with limited calls

When circuit breaker opens:
1. Connection state changes to `DEGRADED`
2. Client notified via `connection_state_change`
3. After reset timeout, transitions to `HALF_OPEN`
4. On success, closes; on failure, reopens

## Message Size Limits

- **Maximum message size**: 1 MB (1,048,576 bytes)
- Messages exceeding this limit are rejected with `MESSAGE_TOO_LARGE` error
- Protocol error counter incremented

## Deduplication

- Server: `deque(maxlen=1000)` for seen message IDs
- Client: `OrderedDict` with FIFO eviction (5,000 entries)
- Duplicate messages are silently dropped (metric incremented)

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
6. **Monitor circuit breaker** state for connection health
7. **Set up DLQ monitoring** for failed message delivery
8. **Use HTTP-only cookies** for web clients (prevents XSS token theft)
