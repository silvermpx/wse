# =============================================================================
# WSE Python Client -- Protocol Constants
# =============================================================================
#
# Values match client/constants.ts and docs/PROTOCOL.md.
# =============================================================================

from ._version import __version__

PROTOCOL_VERSION = 1
# Sent in client_hello and the connect URL. Derived from the package version so
# the two can never drift apart (it used to be a hand-kept copy, "2.0.7").
CLIENT_VERSION = __version__

# -- Timing (seconds) --------------------------------------------------------

HEARTBEAT_INTERVAL = 15.0
IDLE_TIMEOUT = 40.0
CONNECTION_TIMEOUT = 10.0
HEALTH_CHECK_INTERVAL = 30.0
METRICS_INTERVAL = 60.0
# How long connect() waits for the handshake (server_ready / server_hello, and
# the key exchange when encryption is on) before deciding.
SERVER_READY_TIMEOUT = 10.0
# How long a send waits for the E2E key exchange of a fresh connection.
KEY_EXCHANGE_TIMEOUT = 10.0

# -- Reconnection -------------------------------------------------------------

RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
RECONNECT_MAX_ATTEMPTS = -1  # -1 = infinite (matches TS client)
RECONNECT_FACTOR = 1.5
RECONNECT_ABSOLUTE_CAP = 300.0  # 5 minutes
# With a token_provider, an auth rejection (4401/4403/1008) is retried with a
# fresh token; this many rejections in a row end the client with WSEAuthError.
AUTH_MAX_CONSECUTIVE_FAILURES = 3

# -- Circuit breaker -----------------------------------------------------------

CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60.0
CIRCUIT_BREAKER_SUCCESS_THRESHOLD = 3

# -- Rate limiter --------------------------------------------------------------

# Outbound limiter defaults (AsyncWSEClient(rate_limit=..., rate_limit_burst=...)).
RATE_LIMIT_CAPACITY = 1000
RATE_LIMIT_REFILL_RATE = 100
RATE_LIMIT_REFILL_INTERVAL = 1.0

# -- Sequencing ----------------------------------------------------------------

SEQUENCE_WINDOW_SIZE = 10_000
MAX_OUT_OF_ORDER = 100
DUPLICATE_MAX_AGE = 300.0  # 5 minutes

# -- Recovery ------------------------------------------------------------------

# How long a topic waits for a recovery replay to fill an offset gap before the
# client gives up, reports a recovery_gap event and delivers what it holds.
RECOVERY_TIMEOUT = 3.0
# Messages held per topic while a gap is being recovered; past this the client
# gives up waiting at once.
RECOVERY_BUFFER_MAX = 1000
# Recovery requests in a row that got no reply at all before the client stops
# asking on this connection (the application does not answer them).
RECOVERY_MAX_UNANSWERED = 2

# -- Compression ---------------------------------------------------------------

COMPRESSION_THRESHOLD = 1024  # bytes
COMPRESSION_LEVEL = 6

# -- Messages ------------------------------------------------------------------

# Legacy 1 MiB limit, kept for imports. The client default is below.
MAX_MESSAGE_SIZE = 1_048_576
# Default inbound limit: the largest frame accepted from the server and the
# largest size a compressed frame may inflate to. AsyncWSEClient(max_message_size=)
DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024

# -- Wire prefixes -------------------------------------------------------------

PREFIX_COMPRESSED = b"C:"
PREFIX_MSGPACK = b"M:"
PREFIX_ENCRYPTED = b"E:"
PREFIX_SYSTEM = "WSE"
PREFIX_SNAPSHOT = "S"
PREFIX_UPDATE = "U"

# -- Zlib magic bytes ----------------------------------------------------------

ZLIB_MAGIC = 0x78
ZLIB_METHODS = (0x01, 0x5E, 0x9C, 0xDA)

# -- Quality thresholds --------------------------------------------------------

QUALITY_EXCELLENT_LATENCY = 50.0  # ms
QUALITY_EXCELLENT_JITTER = 25.0
QUALITY_EXCELLENT_LOSS = 0.1  # % (matches TS EXCELLENT_LOSS)

QUALITY_GOOD_LATENCY = 150.0
QUALITY_GOOD_JITTER = 50.0
QUALITY_GOOD_LOSS = 1.0  # % (matches TS GOOD_LOSS)

QUALITY_FAIR_LATENCY = 300.0
QUALITY_FAIR_JITTER = 100.0
QUALITY_FAIR_LOSS = 3.0  # % (matches TS FAIR_LOSS)

# -- Batching ------------------------------------------------------------------

BATCH_SIZE = 10
BATCH_TIMEOUT = 0.1  # seconds

# -- Client hello --------------------------------------------------------------

CLIENT_HELLO_MAX_RETRIES = 10
CLIENT_HELLO_RETRY_DELAY = 1.0  # seconds

# -- WebSocket close codes -----------------------------------------------------

WS_CLOSE_NORMAL = 1000
WS_CLOSE_GOING_AWAY = 1001
WS_CLOSE_NO_STATUS = 1005
WS_CLOSE_ABNORMAL = 1006
WS_CLOSE_POLICY_VIOLATION = 1008
WS_CLOSE_SERVER_ERROR = 1011
WS_CLOSE_DRAIN = 4300
WS_CLOSE_AUTH_FAILED = 4401
WS_CLOSE_AUTH_EXPIRED = 4403
WS_CLOSE_RATE_LIMITED = 4429
WS_CLOSE_SUBSCRIPTION_FAILED = 4430
WS_CLOSE_INVALID_MESSAGE = 4400
WS_CLOSE_SERVER_OVERLOAD = 4503

# Server-sent close codes that end the client instead of reconnecting: the
# server rejected the credentials (a token_provider gets a refresh first).
# Every other server close (1000, 1001, 1005, 1011, 4300 drain, 4429, ...) and
# every network loss is reconnected with backoff.
AUTH_CLOSE_CODES = frozenset(
    {WS_CLOSE_AUTH_FAILED, WS_CLOSE_AUTH_EXPIRED, WS_CLOSE_POLICY_VIOLATION}
)

# -- Send retry ----------------------------------------------------------------

SEND_MAX_RETRIES = 5
SEND_RETRY_BASE_DELAY = 0.1  # seconds (100ms)
SEND_RETRY_MAX_DELAY = 2.0  # seconds

# -- Snapshot retry ------------------------------------------------------------

SNAPSHOT_MAX_RETRIES = 3
SNAPSHOT_RETRY_DELAY = 1.0  # seconds
# An identical snapshot request (same topics) inside this window is not resent.
SNAPSHOT_DEDUP_WINDOW = 5.0  # seconds
