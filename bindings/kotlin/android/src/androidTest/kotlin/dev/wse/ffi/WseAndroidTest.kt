// On an Android device or emulator, against the Rust client's example server
// on the host: run-emulator-tests.sh passes its URL as the wseUrl argument.
package dev.wse.ffi

import androidx.lifecycle.ProcessLifecycleOwner
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class WseAndroidTest {
    private val url: String =
        InstrumentationRegistry.getArguments().getString("wseUrl") ?: "ws://10.0.2.2:5007/wse"

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
    fun theProcessInTheBackgroundPausesTheClient(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url))
        val lifecycle = WseAppLifecycle(client, graceMillis = 0)
        val owner = ProcessLifecycleOwner.get()
        onMain { lifecycle.onStop(owner) }
        waitFor { client.status() == WseStatus.PAUSED }
        onMain { lifecycle.onStart(owner) }
        waitFor { client.status() == WseStatus.CONNECTED }
        client.disconnect(1_000u)
    }

    @Test
    fun theNetworkPausesTheClientAndRenewsItsConnection(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url))
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val monitor = WseNetworkMonitor(context, client, settleMillis = 0)
        onMain {
            monitor.update("wifi")
            monitor.update(null)
        }
        waitFor { client.status() == WseStatus.PAUSED }
        onMain { monitor.update("wifi") }
        waitFor { client.status() == WseStatus.CONNECTED }
        // Over another network: the connection is made again.
        onMain { monitor.update("cellular") }
        val events = client.events()
        var renewed = false
        while (!renewed) {
            val item = withTimeout(5_000) { events.next() } ?: break
            renewed = item is WseItem.Disconnected && item.reason == "reconnect asked"
        }
        assertTrue(renewed)
        client.disconnect(1_000u)
    }

    private fun onMain(run: () -> Unit) = InstrumentationRegistry.getInstrumentation().runOnMainSync(run)

    private suspend fun waitFor(done: () -> Boolean) {
        withTimeout(5_000) {
            while (!done()) delay(20)
        }
    }

    @Test
    fun aPayloadMustBeJson(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url))
        assertFailsWith<WseException.Encode> { client.trySend("x", "not json") }
        client.disconnect(1_000u)
    }
}
