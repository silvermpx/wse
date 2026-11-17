// The Kotlin side of the client: suspend calls a cancelled coroutine stops,
// and the events as a Flow that reads a batch when the last one is used up.
package dev.wse.ffi

import java.lang.ref.WeakReference
import java.util.WeakHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector

/** Connects, and returns once the server took the connection. Cancelling the coroutine gives up the attempt. */
suspend fun connectClient(config: WseConfig, tokens: WseTokenSource? = null): WseClient =
    connect(config, tokens, WseCancel())

/**
 * The events of a client. A batch is acknowledged only once all of it was handed out, so a
 * collector cancelled in between gets the rest from the same object later, and nothing comes twice.
 * One reader at a time: a second reading at the same time gets [WseException.Busy].
 */
class WseEvents internal constructor(
    private val client: WseClient,
    private val maxItems: UInt = 256u,
    private val maxBytes: UInt = 1u shl 20,
) {
    private var batch: WseBatch? = null
    private var index = 0
    private var acknowledged: ULong = 0u
    private var ended = false
    private val reading = AtomicBoolean(false)

    /** Runs as an item is about to be handed over (tests cancel the collector there). */
    internal var handingOver: ((WseItem) -> Unit)? = null

    /** The next item, or null once the client closed. */
    suspend fun next(): WseItem? = reading {
        val item = peek() ?: return@reading null
        index++
        item
    }

    /** Runs `read` as the one reader, or throws [WseException.Busy]. */
    private inline fun <T> reading(read: () -> T): T {
        if (!reading.compareAndSet(false, true)) throw WseException.Busy()
        try {
            return read()
        } finally {
            reading.set(false)
        }
    }

    /** The next item, not taken yet, or null once the client closed. */
    private suspend fun peek(): WseItem? {
        while (true) {
            batch?.let { current ->
                if (index < current.items.size) return current.items[index]
                acknowledged = current.id
            }
            if (ended) return null
            val next = client.nextBatch(acknowledged, maxItems, maxBytes, WseCancel())
            if (next.kind == WseBatchKind.END) {
                ended = true
                batch = null
                return null
            }
            batch = next
            index = 0
        }
    }

    /**
     * As a Flow: each collection goes on from where the last one stopped. An item is taken once its
     * collector returned from it, or stopped the collection while it still ran (`first()`,
     * `take()`, its own failure). One whose collection was cancelled before that comes again to the
     * next reader: a collector cancelled while it handled an item may see it twice.
     */
    fun asFlow(): Flow<WseItem> = Items()

    // Not the flow builder: its emit checks for cancellation before the collector runs.
    private inner class Items : Flow<WseItem> {
        override suspend fun collect(collector: FlowCollector<WseItem>) = reading {
            while (true) {
                val item = peek() ?: return@reading
                // Cancelled before the hand-over: the item stays for the next reader.
                currentCoroutineContext().ensureActive()
                handingOver?.invoke(item)
                try {
                    collector.emit(item)
                } catch (stopped: Throwable) {
                    // Stopped while running, the collector had it. Cancelled, it may not have
                    // (an operator in between checks before it hands on): it comes again.
                    if (currentCoroutineContext().isActive) index++
                    throw stopped
                }
                index++
            }
        }
    }
}

/** Each client's reader while one is held. */
private val readers = WeakHashMap<WseClient, WeakReference<WseEvents>>()

/**
 * What the server sends, in order, until the client closes. Every call reads the client's one
 * stream (with the limits of the first, while it is held): an item goes to one reader, and a second
 * reading at the same time gets [WseException.Busy].
 */
/**
 * The consumer applied [message] (one of a topic): the checkpoint moves on through what was applied
 * without a hole. A message handed out before the events showed another user counts for nobody.
 */
fun WseClient.commit(message: WseMessage) {
    val topic = message.topic ?: return
    val epoch = message.epoch ?: return
    val offset = message.offset ?: return
    commit(topic, epoch, offset, message.generation)
}

fun WseClient.events(maxItems: UInt = 256u, maxBytes: UInt = 1u shl 20): WseEvents =
    synchronized(readers) {
        readers[this]?.get() ?: WseEvents(this, maxItems, maxBytes).also { readers[this] = WeakReference(it) }
    }

/**
 * Sends `{"t": kind, "p": json}`, waiting for room in the queue. Cancelling the
 * coroutine gives up the wait; a message already queued still goes.
 */
suspend fun WseClient.send(kind: String, json: String) = send(kind, json, WseCancel())

/**
 * Sends once each of [topics] is continuous on the connection (see `topicStates()`),
 * waiting for room in the queue; until then it waits, in order with the others that
 * wait. Cancelling the coroutine gives up the wait for room; a message already queued
 * still goes.
 */
suspend fun WseClient.sendRequiring(kind: String, json: String, topics: List<String>) =
    sendRequiring(kind, json, topics, WseCancel())

/** Sends on the connection that is ready now, or not at all, waiting for room in the queue. */
suspend fun WseClient.sendLive(kind: String, json: String) = sendLive(kind, json, WseCancel())

/** Sends a signed message on this connection, or not at all; the nonce it went with. */
suspend fun WseClient.sendSigned(kind: String, json: String, nonce: String? = null): String =
    sendSigned(kind, json, nonce, WseCancel())
