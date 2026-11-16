// Against the Rust client's example server: run-tests.sh starts it and passes
// its URL in WSE_TEST_URL.
package dev.wse.ffi

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class WseClientTest {
    private val url = System.getenv("WSE_TEST_URL") ?: "ws://127.0.0.1:5007/wse"

    @Test
    fun ticksComeInOrderAndEchoAnswers(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, topics = listOf("ticks")))
        assertEquals(WseStatus.CONNECTED, client.status())
        client.send("echo", """{"text":"hallo"}""")
        val events = client.events()
        val ticks = mutableListOf<ULong>()
        var echoed: String? = null
        while (ticks.size < 3 || echoed == null) {
            val item = events.next() ?: break
            if (item is WseItem.Message) {
                if (item.message.kind == "echoed") echoed = item.message.json
                if (item.message.topic == "ticks") item.message.offset?.let { ticks.add(it) }
            }
        }
        assertEquals("""{"text":"hallo"}""", echoed)
        assertEquals(ticks.sorted(), ticks)
        client.disconnect(1_000u)
    }

    @Test
    fun aCollectorStoppedInTheMiddleLosesNothing(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, topics = listOf("ticks")))
        val events = client.events()
        val first = events.asFlow().first()
        val second = events.asFlow().first()
        assertTrue(first != second)
        client.disconnect(1_000u)
        // After the end, collections end too.
        events.asFlow().collect { }
    }

    @Test
    fun aCancelledConnectGivesUp(): Unit = runBlocking {
        val started = System.nanoTime()
        assertFailsWith<TimeoutCancellationException> {
            withTimeout(200) { connectClient(WseConfig(url = "ws://10.255.255.1:9/wse", connectTimeoutMs = 30_000u)) }
        }
        assertTrue(System.nanoTime() - started < 5_000_000_000)
    }

    @Test
    fun aLiveMessageGoesOnTheReadyConnection(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url))
        client.trySendLive("valve", "{\"open\":true}")
        client.sendLive("valve", "{\"open\":false}")
        client.disconnect(1_000u)
        assertFailsWith<WseException.Closed> { client.trySendLive("valve", "{}") }
    }

    @Test
    fun aPayloadMustBeJson(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url))
        assertFailsWith<WseException.Encode> { client.trySend("x", "not json") }
        client.disconnect(1_000u)
    }
}
