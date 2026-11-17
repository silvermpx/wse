// The client for Java, on the JVM and on Android. Kotlin's suspend functions
// come as futures, the events one at a time with the calling thread waiting
// for them, and the bindings' unsigned numbers, which Java does not see, as
// int and long. Kotlin code uses connectClient, the suspend functions and
// WseEvents.asFlow instead.
package dev.wse.ffi

import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeoutException
import java.util.function.Supplier
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull

/**
 * The client for Java. A call that waits returns a future, and cancelling the future gives the
 * call up; the client's other methods (`status`, `subscribe`, `trySend`, `pause`, ...) are called
 * on it as they are.
 *
 * ```java
 * WseClient client = Wse.connect(Wse.config("wss://example.com/wse").token(token).topics("prices").build()).join();
 * Wse.send(client, "order", "{\"id\":1}").join();
 * WseEventReader events = Wse.events(client);
 * for (WseItem item; (item = events.next()) != null; ) {
 *     if (item instanceof WseItem.Message m) System.out.println(m.getMessage().getKind());
 * }
 * ```
 */
object Wse {
    // The calls behind the futures: one failing fails its own future only.
    private val calls = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /** A configuration for the server at [url] (ws://host:port/wse or wss://...). */
    @JvmStatic
    fun config(url: String): WseConfigBuilder = WseConfigBuilder(url)

    /** The recover-or-resync delivery of [consumer], the one consumer that commits, by a stable name. */
    @JvmStatic
    fun delivery(consumer: String): WseDeliveryBuilder = WseDeliveryBuilder(consumer)

    /**
     * The outbox, where every message waits until the server answers for it: in memory by
     * default, or in a file ([WseOfflineBuilder.path]) that outlives the app.
     */
    @JvmStatic
    fun offline(): WseOfflineBuilder = WseOfflineBuilder()

    /** How a message goes ([sendWith]): its own id, how long it may wait, the topics it requires. */
    @JvmStatic
    fun sendOptions(): WseSendOptionsBuilder = WseSendOptionsBuilder()

    /** How many messages wait in the outbox for the server's answer. */
    @JvmStatic
    fun kept(client: WseClient): Int = client.kept().coerceAtMost(Int.MAX_VALUE.toUInt()).toInt()

    /**
     * Tokens from Java: [supplier] is asked before each connection attempt and returns the token's
     * future. A future that fails is a [WseTokenException.Failed] with its message.
     */
    @JvmStatic
    fun tokens(supplier: Supplier<out CompletionStage<String>>): WseTokenSource = FutureTokens(supplier)

    /** Connects; the future completes once the server took the connection. */
    @JvmStatic
    @JvmOverloads
    fun connect(config: WseConfig, tokens: WseTokenSource? = null): CompletableFuture<WseClient> =
        calls.future { connectClient(config, tokens) }

    /**
     * Starts; the future completes at once, the connection made in the background (a failed
     * first attempt made again as any later one). What is sent meanwhile waits in the outbox.
     */
    @JvmStatic
    @JvmOverloads
    fun start(config: WseConfig, tokens: WseTokenSource? = null): CompletableFuture<WseClient> =
        calls.future { startClient(config, tokens) }

    /**
     * Sends `{"t": kind, "p": json}`, waiting for room; a message already queued still goes. It
     * waits in the outbox until the server answers for it.
     */
    @JvmStatic
    fun send(client: WseClient, kind: String, json: String): CompletableFuture<Void?> =
        calls.future {
            client.send(kind, json)
            null
        }

    /** [send] with [options] ([sendOptions]); the future gives its id, which its outcome names. */
    @JvmStatic
    fun sendWith(client: WseClient, kind: String, json: String, options: WseSendOptions): CompletableFuture<String> =
        calls.future { client.sendWith(kind, json, options) }

    /**
     * Sends once each of [topics] is continuous on the connection, waiting for room in the
     * queue; until then it waits, in order with the others that wait.
     */
    @JvmStatic
    fun sendRequiring(client: WseClient, kind: String, json: String, topics: List<String>): CompletableFuture<Void?> =
        calls.future {
            client.sendRequiring(kind, json, topics)
            null
        }

    /** Sends on the connection that is ready now, or not at all, waiting for room in the queue. */
    @JvmStatic
    fun sendLive(client: WseClient, kind: String, json: String): CompletableFuture<Void?> =
        calls.future {
            client.sendLive(kind, json)
            null
        }

    /** Sends a signed message on this connection, or not at all; the future gives the nonce it went with. */
    @JvmStatic
    @JvmOverloads
    fun sendSigned(client: WseClient, kind: String, json: String, nonce: String? = null): CompletableFuture<String> =
        calls.future { client.sendSigned(kind, json, nonce) }

    /** Sends what is queued, for at most [deadline], and closes. */
    @JvmStatic
    fun disconnect(client: WseClient, deadline: Duration): CompletableFuture<Void?> =
        calls.future {
            client.disconnect(millis(deadline))
            null
        }

    /** The client's events, one at a time. */
    @JvmStatic
    fun events(client: WseClient): WseEventReader = WseEventReader(client.events())

    /** The consumer applied [message]; see `WseClient.commit` in Kotlin. */
    @JvmStatic
    fun commit(client: WseClient, message: WseMessage) = client.commit(message)

    /** The application brought the topics of [resync] up to date: what they held is handed on. */
    @JvmStatic
    fun resynced(client: WseClient, resync: WseItem.Resync) = client.resynced(resync.round)

    /** A new ECDSA P-256 private key (PKCS#8, DER) for signed messages. */
    @JvmStatic
    fun generateSigningKey(): ByteArray = dev.wse.ffi.generateSigningKey()

    /** The public key of [key] (base64), for the server to check this client's signed messages against. */
    @JvmStatic
    @Throws(WseException::class)
    fun signingPublicKey(key: ByteArray): String = dev.wse.ffi.signingPublicKey(key)

    /** When the next attempt is made, in milliseconds. */
    @JvmStatic
    fun retryInMillis(disconnected: WseItem.Disconnected): Long = disconnected.retryInMs.toSignedLong()

    /** Attempts made since the last connection the server took. */
    @JvmStatic
    fun attempt(disconnected: WseItem.Disconnected): Long = disconnected.attempt.toLong()

    /** The close code, or -1 when the connection was cut or never made. */
    @JvmStatic
    fun closeCode(disconnected: WseItem.Disconnected): Int = disconnected.code?.toInt() ?: -1

    // The numbers Kotlin has unsigned, for Java. An offset is a u64: its 64 bits come as a long,
    // which Long.toUnsignedString and Long.compareUnsigned read whole.

    /** The epoch of [message]'s stream; null for a message of no topic. */
    @JvmStatic
    fun epoch(message: WseMessage): Long? = message.epoch?.toLong()

    /** [message]'s offset in its stream (unsigned, in a long); null for a message of no topic. */
    @JvmStatic
    fun offset(message: WseMessage): Long? = message.offset?.toLong()

    /** How urgent [message]'s sender said it is (0 to 255); null when it did not say. */
    @JvmStatic
    fun priority(message: WseMessage): Int? = message.priority?.toInt()

    /** The user generation [message] came in; see [commit]. */
    @JvmStatic
    fun generation(message: WseMessage): Long = message.generation.toLong()

    /** The epoch of the stream of [position] (a checkpoint's, a recovery's). */
    @JvmStatic
    fun epoch(position: WseStreamPosition): Long = position.epoch.toLong()

    /** The offset of [position] (unsigned, in a long). */
    @JvmStatic
    fun offset(position: WseStreamPosition): Long = position.offset.toLong()

    /** How many messages the recovery of a topic replayed. */
    @JvmStatic
    fun count(recovery: WseTopicRecovery): Long = recovery.count.toLong()

    /** The epoch of the stream [gap] is in. */
    @JvmStatic
    fun epoch(gap: WseItem.Gap): Long = gap.epoch.toLong()

    /** The first offset [gap] missed (unsigned, in a long); null when not known. */
    @JvmStatic
    fun first(gap: WseItem.Gap): Long? = gap.first?.toLong()

    /** The last offset [gap] missed (unsigned, in a long); null when not known. */
    @JvmStatic
    fun last(gap: WseItem.Gap): Long? = gap.last?.toLong()

    /** The round of [resync], as [resynced] takes it. */
    @JvmStatic
    fun round(resync: WseItem.Resync): Long = resync.round.toLong()
}

/**
 * A client's events for Java: one item at a time, the calling thread waiting for it. An item goes
 * to one reader, as with [WseEvents]: a second reading at the same time gets [WseException.Busy].
 */
class WseEventReader internal constructor(private val events: WseEvents) {
    /** The next item, waiting as long as it takes; null once the client closed. */
    @Throws(WseException::class, InterruptedException::class)
    fun next(): WseItem? = runBlocking { events.next() }

    /**
     * The next item if one comes within [timeout]; null once the client closed. Nothing in time is
     * a [TimeoutException], and the next call goes on from there: nothing is lost.
     */
    @Throws(WseException::class, InterruptedException::class, TimeoutException::class)
    fun next(timeout: Duration): WseItem? {
        val next = runBlocking { withTimeoutOrNull(timeout.toMillis()) { Next(events.next()) } }
        return (next ?: throw TimeoutException("no event within $timeout")).item
    }

    private class Next(val item: WseItem?)
}

/** A [WseConfig] for Java, field by field (Kotlin names the fields in `WseConfig(...)` instead). */
class WseConfigBuilder internal constructor(private val url: String) {
    private var token: String? = null
    private var topics = emptyList<String>()
    private var msgpack = false
    private val headers = mutableListOf<WseHeader>()
    private var sendQueue = 0u
    private var reconnect = true
    private var reconnectDelay = 0uL
    private var reconnectMaxDelay = 0uL
    private var reconnectFactor = 0.0
    private var reconnectJitter = true
    private var reconnectMaxAttempts = 0u
    private var connectTimeout = 0uL
    private var readyTimeout = 0uL
    private var idleTimeout = 0uL
    private var recoveryTimeout = 0uL
    private var serverIdentityKeys = emptyList<String>()
    private var signingKey: ByteArray? = null
    private var serverSigningKeys = emptyList<String>()
    private var trust: WseTrust? = null
    private var insecure = false
    private var delivery: WseDelivery? = null
    private var offline: WseOffline? = null

    /** The token for the handshake's Authorization header (a token source gives each attempt's instead). */
    fun token(token: String?): WseConfigBuilder = apply { this.token = token }

    /** The topics to hold. */
    fun topics(vararg topics: String): WseConfigBuilder = apply { this.topics = topics.toList() }

    /** The topics to hold. */
    fun topics(topics: List<String>): WseConfigBuilder = apply { this.topics = topics.toList() }

    /** MessagePack instead of JSON on the wire. */
    fun msgpack(msgpack: Boolean): WseConfigBuilder = apply { this.msgpack = msgpack }

    /** A header for the handshake. */
    fun header(name: String, value: String): WseConfigBuilder = apply { headers += WseHeader(name, value) }

    /** Messages waiting to be sent at most (0: 1024). */
    fun sendQueue(messages: Int): WseConfigBuilder = apply { sendQueue = count(messages) }

    /** Whether a lost connection is made again (true by default). */
    fun reconnect(reconnect: Boolean): WseConfigBuilder = apply { this.reconnect = reconnect }

    /** The wait before the first reconnect attempt (zero: a second). */
    fun reconnectDelay(delay: Duration): WseConfigBuilder = apply { reconnectDelay = millis(delay) }

    /** The longest wait between reconnect attempts (zero: 30 s). */
    fun reconnectMaxDelay(delay: Duration): WseConfigBuilder = apply { reconnectMaxDelay = millis(delay) }

    /** Each attempt waits this many times longer than the one before (zero: 1.5). */
    fun reconnectFactor(factor: Double): WseConfigBuilder = apply { reconnectFactor = factor }

    /** Spread the waits by ±10%, so clients of a restarted server do not all come back at once (true by default). */
    fun reconnectJitter(jitter: Boolean): WseConfigBuilder = apply { reconnectJitter = jitter }

    /** Attempts in a row before giving up (zero: never). */
    fun reconnectMaxAttempts(attempts: Int): WseConfigBuilder = apply { reconnectMaxAttempts = count(attempts) }

    /** For the socket, TLS and the upgrade (zero: 10 s). */
    fun connectTimeout(timeout: Duration): WseConfigBuilder = apply { connectTimeout = millis(timeout) }

    /** For the server to confirm the connection after the upgrade (zero: 10 s). */
    fun readyTimeout(timeout: Duration): WseConfigBuilder = apply { readyTimeout = millis(timeout) }

    /** Silence from the server after which the connection is taken for dead and made again (zero: 40 s). */
    fun idleTimeout(timeout: Duration): WseConfigBuilder = apply { idleTimeout = millis(timeout) }

    /** How long a topic waits for the server to replay what it missed before it reports a gap (zero: 3 s). */
    fun recoveryTimeout(timeout: Duration): WseConfigBuilder = apply { recoveryTimeout = millis(timeout) }

    /** End-to-end encryption, pinned to these server identity keys (base64). */
    fun serverIdentityKeys(vararg keys: String): WseConfigBuilder = apply { serverIdentityKeys = keys.toList() }

    /** End-to-end encryption, pinned to these server identity keys (base64). */
    fun serverIdentityKeys(keys: List<String>): WseConfigBuilder = apply { serverIdentityKeys = keys.toList() }

    /** This client's ECDSA P-256 private key (PKCS#8, PEM or DER), for signed messages. */
    fun signingKey(key: ByteArray?): WseConfigBuilder = apply { signingKey = key }

    /** The server's signing keys (base64) its signed messages are checked against. */
    fun serverSigningKeys(vararg keys: String): WseConfigBuilder = apply { serverSigningKeys = keys.toList() }

    /** The server's signing keys (base64) its signed messages are checked against. */
    fun serverSigningKeys(keys: List<String>): WseConfigBuilder = apply { serverSigningKeys = keys.toList() }

    /** The certificates a wss:// server's chain must lead to; null: the system's verifier. */
    fun trust(trust: WseTrust?): WseConfigBuilder = apply { this.trust = trust }

    /** Allows ws://, which sends everything in the clear: for development. */
    fun insecure(insecure: Boolean): WseConfigBuilder = apply { this.insecure = insecure }

    /** The recover-or-resync delivery ([Wse.delivery]); null: best effort. */
    fun delivery(delivery: WseDelivery?): WseConfigBuilder = apply { this.delivery = delivery }

    /** The outbox ([Wse.offline]); null: in memory, with its defaults. */
    fun offline(offline: WseOffline?): WseConfigBuilder = apply { this.offline = offline }

    fun build(): WseConfig =
        WseConfig(
            url = url,
            token = token,
            topics = topics,
            msgpack = msgpack,
            headers = headers.toList(),
            sendQueue = sendQueue,
            reconnect = reconnect,
            reconnectBaseDelayMs = reconnectDelay,
            reconnectMaxDelayMs = reconnectMaxDelay,
            reconnectFactor = reconnectFactor,
            reconnectJitter = reconnectJitter,
            reconnectMaxAttempts = reconnectMaxAttempts,
            connectTimeoutMs = connectTimeout,
            readyTimeoutMs = readyTimeout,
            idleTimeoutMs = idleTimeout,
            recoveryTimeoutMs = recoveryTimeout,
            serverIdentityKeys = serverIdentityKeys,
            signingKey = signingKey,
            serverSigningKeys = serverSigningKeys,
            trust = trust,
            insecure = insecure,
            delivery = delivery,
            offline = offline,
        )
}

/** A [WseOffline] for Java, field by field. */
class WseOfflineBuilder internal constructor() {
    private var disabled = false
    private var path: String? = null
    private var maxMessages = 0u
    private var maxBytes = 0uL
    private var maxAge = 0uL

    /** No outbox: a send with no connection fails, as before 3.2. */
    fun disabled(): WseOfflineBuilder = apply { disabled = true }

    /** The file the messages wait in, so that they outlive the app (in its own storage); null: memory. */
    fun path(path: String?): WseOfflineBuilder = apply { this.path = path }

    /** How many wait at most, those not answered yet included (0: 100,000). */
    fun maxMessages(messages: Int): WseOfflineBuilder = apply { maxMessages = count(messages) }

    /** How many bytes of messages wait at most (0: 16 MiB). */
    fun maxBytes(bytes: Long): WseOfflineBuilder = apply { maxBytes = bytes.coerceAtLeast(0).toULong() }

    /** How long one waits to be written first (zero: an hour); one not written by then is dropped. */
    fun maxAge(age: Duration): WseOfflineBuilder = apply { maxAge = millis(age) }

    fun build(): WseOffline =
        WseOffline(
            disabled = disabled,
            path = path,
            maxMessages = maxMessages,
            maxBytes = maxBytes,
            maxAgeMs = maxAge,
        )
}

/** A [WseSendOptions] for Java, field by field. */
class WseSendOptionsBuilder internal constructor() {
    private var id: String? = null
    private var ttl = 0uL
    private var requires: List<String> = emptyList()
    private var correlationId: String? = null
    private var priority: UByte = 0u
    private var live = false

    /** Its id: the app's own, or a random UUID; its outcome names it. */
    fun id(id: String?): WseSendOptionsBuilder = apply { this.id = id }

    /** How long it may wait to be written first (zero: the outbox's `maxAge`). */
    fun ttl(ttl: Duration): WseSendOptionsBuilder = apply { this.ttl = millis(ttl) }

    /** Topics that must each be continuous when it goes. */
    fun requires(topics: List<String>): WseSendOptionsBuilder = apply { requires = topics.toList() }

    /** The id the reply names (`cid`): the app's request and its response. */
    fun correlationId(id: String?): WseSendOptionsBuilder = apply { correlationId = id }

    /**
     * How urgent the app says it is (`pri`: 10 critical, 8 high, 5 normal, 3 low, 1 background;
     * 0 none). For the server's app: the outbox keeps the order.
     */
    fun priority(priority: Int): WseSendOptionsBuilder = apply {
        require(priority in 0..255) { "a priority is 0 to 255, not $priority" }
        this.priority = priority.toUByte()
    }

    /** On the connection that is ready now, or not at all: never kept, never sent later. */
    fun live(live: Boolean): WseSendOptionsBuilder = apply { this.live = live }

    fun build(): WseSendOptions = WseSendOptions(
        id = id,
        ttlMs = ttl,
        requires = requires,
        correlationId = correlationId,
        priority = priority,
        live = live,
    )
}

/** A [WseDelivery] for Java, field by field. */
class WseDeliveryBuilder internal constructor(private val consumer: String) {
    private var schema: String? = null
    private var realm: String? = null
    private var checkpoint: String? = null
    private var maxHeld = 0u

    /** The application's data schema: a checkpoint of another is refused. */
    fun schema(schema: String?): WseDeliveryBuilder = apply { this.schema = schema }

    /** The server's realm (by default its URL): a checkpoint of another is refused. */
    fun realm(realm: String?): WseDeliveryBuilder = apply { this.realm = realm }

    /** The checkpoint the application saved last (`client.checkpoint()`). */
    fun checkpoint(checkpoint: String?): WseDeliveryBuilder = apply { this.checkpoint = checkpoint }

    /** Messages a topic holds while it resyncs (0: 10,000); more starts the resync over. */
    fun maxHeld(messages: Int): WseDeliveryBuilder = apply { maxHeld = count(messages) }

    fun build(): WseDelivery = WseDelivery(consumer, schema, realm, checkpoint, maxHeld)
}

private class FutureTokens(private val supplier: Supplier<out CompletionStage<String>>) : WseTokenSource {
    override suspend fun token(): String =
        try {
            supplier.get().await()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            throw WseTokenException.Failed(error.message ?: error.toString())
        }
}

private fun millis(duration: Duration): ULong = duration.toMillis().coerceAtLeast(0).toULong()

private fun count(value: Int): UInt {
    require(value >= 0) { "a count is not negative: $value" }
    return value.toUInt()
}

private fun ULong.toSignedLong(): Long = coerceAtMost(Long.MAX_VALUE.toULong()).toLong()
