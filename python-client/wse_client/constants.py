# =============================================================================
# WSE Python Client -- Protocol Constants
# =============================================================================
#
# Values match client/constants.ts and docs/PROTOCOL.md.
# =============================================================================

PROTOCOL_VERSION = 1
CLIENT_VERSION = "1.3.0"

# -- Timing (seconds) --------------------------------------------------------

HEARTBEAT_INTERVAL = 15.0
IDLE_TIMEOUT = 40.0
CONNECTION_TIMEOUT = 10.0
HEALTH_CHECK_INTERVAL = 30.0
METRICS_INTERVAL = 60.0

# -- Reconnection -------------------------------------------------------------

RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
RECONNECT_MAX_ATTEMPTS = -1  # -1 = infinite (matches TS client)
RECONNECT_FACTOR = 1.5
RECONNECT_ABSOLUTE_CAP = 300.0  # 5 minutes

# -- Circuit breaker -----------------------------------------------------------

CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60.0
CIRCUIT_BREAKER_SUCCESS_THRESHOLD = 3

# -- Rate limiter --------------------------------------------------------------

RATE_LIMIT_CAPACITY = 1000
RATE_LIMIT_REFILL_RATE = 100
RATE_LIMIT_REFILL_INTERVAL = 1.0

# -- Sequencing ----------------------------------------------------------------

SEQUENCE_WINDOW_SIZE = 1_000
MAX_OUT_OF_ORDER = 100
DUPLICATE_MAX_AGE = 300.0  # 5 minutes

# -- Compression ---------------------------------------------------------------

COMPRESSION_THRESHOLD = 1024  # bytes
COMPRESSION_LEVEL = 6

# -- Messages ------------------------------------------------------------------

MAX_MESSAGE_SIZE = 1_048_576  # 1 MB

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

QUALITY_EXCELLENT_LATENCY = 50.0   # ms
QUALITY_EXCELLENT_JITTER = 25.0
QUALITY_EXCELLENT_LOSS = 0.1       # % (matches TS EXCELLENT_LOSS)

QUALITY_GOOD_LATENCY = 150.0
QUALITY_GOOD_JITTER = 50.0
QUALITY_GOOD_LOSS = 1.0            # % (matches TS GOOD_LOSS)

QUALITY_FAIR_LATENCY = 300.0
QUALITY_FAIR_JITTER = 100.0
QUALITY_FAIR_LOSS = 3.0            # % (matches TS FAIR_LOSS)

# -- Batching ------------------------------------------------------------------

BATCH_SIZE = 10
BATCH_TIMEOUT = 0.1  # seconds

# -- Client hello --------------------------------------------------------------

CLIENT_HELLO_MAX_RETRIES = 10
CLIENT_HELLO_RETRY_DELAY = 1.0  # seconds

# -- WebSocket close codes -----------------------------------------------------

WS_CLOSE_NORMAL = 1000
WS_CLOSE_GOING_AWAY = 1001
WS_CLOSE_POLICY_VIOLATION = 1008
WS_CLOSE_SERVER_ERROR = 1011
WS_CLOSE_AUTH_FAILED = 4401
WS_CLOSE_AUTH_EXPIRED = 4403
WS_CLOSE_RATE_LIMITED = 4429
WS_CLOSE_SUBSCRIPTION_FAILED = 4430
WS_CLOSE_INVALID_MESSAGE = 4400
WS_CLOSE_SERVER_OVERLOAD = 4503

# -- Send retry ----------------------------------------------------------------

SEND_MAX_RETRIES = 5
SEND_RETRY_BASE_DELAY = 0.1  # seconds (100ms)
SEND_RETRY_MAX_DELAY = 2.0   # seconds

# -- Snapshot retry ------------------------------------------------------------

SNAPSHOT_MAX_RETRIES = 3
SNAPSHOT_RETRY_DELAY = 1.0  # seconds
