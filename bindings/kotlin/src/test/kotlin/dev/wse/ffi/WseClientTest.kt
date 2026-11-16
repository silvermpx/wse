// Against the Rust client's example server: run-tests.sh starts it and passes
// its URL in WSE_TEST_URL.
package dev.wse.ffi

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class WseClientTest {
    private val url = System.getenv("WSE_TEST_URL") ?: "ws://127.0.0.1:5007/wse"

    @Test
    fun wsGoesInTheClearOnlyWhenAsked(): Unit = runBlocking {
        assertFailsWith<WseException.Config> { connectClient(WseConfig(url = url)) }
    }

    @Test
    fun aPrivateCaIsTrustedWhereItIsNamed(): Unit = runBlocking {
        val tlsUrl = checkNotNull(System.getenv("WSE_TEST_TLS_URL")) { "run-tests.sh passes the wss:// door" }
        val ca = java.io.File(System.getenv("WSE_TEST_CA")).readBytes()
        val client = connectClient(WseConfig(url = tlsUrl, trust = WseTrust.Roots(listOf(ca))))
        assertEquals(WseStatus.CONNECTED, client.status())
        client.disconnect(1_000u)
        // The system's verifier and the Mozilla roots do not know that CA.
        for (trust in listOf(WseTrust.Platform, WseTrust.Mozilla)) {
            val error = assertFailsWith<WseException.Connect> {
                connectClient(WseConfig(url = tlsUrl, reconnect = false, trust = trust))
            }
            assertTrue("certificate" in error.reason, "$trust: ${error.reason}")
        }
    }

    @Test
    fun ticksComeInOrderAndEchoAnswers(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, topics = listOf("ticks"), insecure = true))
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
        val client = connectClient(WseConfig(url = url, topics = listOf("ticks"), insecure = true))
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
            withTimeout(200) { connectClient(WseConfig(url = "ws://10.255.255.1:9/wse", connectTimeoutMs = 30_000u, insecure = true)) }
        }
        assertTrue(System.nanoTime() - started < 5_000_000_000)
    }

    @Test
    fun aCollectorCancelledAtAnItemLosesNotTheNext(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, topics = listOf("ticks"), insecure = true))
        val events = client.events()
        // Several ticks wait in one batch.
        delay(700)
        var first: ULong? = null
        val collector = launch {
            events.asFlow().collect { item ->
                if (item is WseItem.Message && item.message.topic == "ticks") {
                    first = item.message.offset
                    // The collector's job ends here; the handler returns as usual.
                    coroutineContext.job.cancel()
                }
            }
        }
        collector.join()
        var next: ULong? = null
        while (next == null) {
            val item = events.next() ?: break
            if (item is WseItem.Message && item.message.topic == "ticks") next = item.message.offset
        }
        assertEquals(first!! + 1u, next)
        client.disconnect(1_000u)
    }

    @Test
    fun everyCallReadsOneStreamOneReaderAtATime(): Unit = runBlocking {
        // No topics: past its first items the server stays quiet.
        val client = connectClient(WseConfig(url = url, insecure = true))
        val events = client.events()
        assertSame(events, client.events())
        val waiting = launch { while (events.next() != null) {} }
        delay(300)
        assertFailsWith<WseException.Busy> { client.events().next() }
        waiting.cancel()
        waiting.join()
        client.send("echo", "{\"text\":\"after\"}")
        var echoed: String? = null
        while (echoed == null) {
            val item = events.next() ?: break
            if (item is WseItem.Message && item.message.kind == "echoed") echoed = item.message.json
        }
        assertEquals("{\"text\":\"after\"}", echoed)
        client.disconnect(1_000u)
    }

    @Test
    fun aCancelledSendDoesNotGoOut(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, insecure = true))
        val cancel = WseCancel()
        cancel.cancel()
        assertFailsWith<WseException.Cancelled> { client.send("order", "{}", cancel) }
        client.send("after", "{}")
        client.disconnect(1_000u)
    }

    @Test
    fun aLiveMessageGoesOnTheReadyConnection(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, insecure = true))
        client.trySendLive("valve", "{\"open\":true}")
        client.sendLive("valve", "{\"open\":false}")
        client.disconnect(1_000u)
        assertFailsWith<WseException.Closed> { client.trySendLive("valve", "{}") }
    }

    @Test
    fun aPayloadMustBeJson(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, insecure = true))
        assertFailsWith<WseException.Encode> { client.trySend("x", "not json") }
        client.disconnect(1_000u)
    }
}
