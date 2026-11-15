// The Kotlin side of the client: suspend calls a cancelled coroutine stops,
// and the events as a Flow that reads a batch when the last one is used up.
package dev.wse.ffi

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/** Connects, and returns once the server took the connection. Cancelling the coroutine gives up the attempt. */
suspend fun connectClient(config: WseConfig, tokens: WseTokenSource? = null): WseClient =
    connect(config, tokens, WseCancel())

/**
 * The events of a client. A batch is acknowledged only once all of it was handed out, so a
 * collector cancelled in between gets the rest from the same object later, and nothing comes twice.
 * One reader at a time: a second gets [WseException.Busy].
 */
class WseEvents(
    private val client: WseClient,
    private val maxItems: UInt = 256u,
    private val maxBytes: UInt = 1u shl 20,
) {
    private var batch: WseBatch? = null
    private var index = 0
    private var acknowledged: ULong = 0u
    private var ended = false

    /** The next item, or null once the client closed. */
    suspend fun next(): WseItem? {
        while (true) {
            batch?.let { current ->
                if (index < current.items.size) return current.items[index++]
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

    /** As a Flow: each collection goes on from where the last one stopped. */
    fun asFlow(): Flow<WseItem> = flow {
        while (true) emit(next() ?: return@flow)
    }
}

/** What the server sends, in order, until the client closes. */
fun WseClient.events(maxItems: UInt = 256u, maxBytes: UInt = 1u shl 20): WseEvents =
    WseEvents(this, maxItems, maxBytes)
