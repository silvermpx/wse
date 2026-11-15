// On an Android device or emulator, against the Rust client's example server
// on the host: run-emulator-tests.sh passes its URL as the wseUrl argument.
package dev.wse.ffi

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class WseAndroidTest {
    private val url: String =
        InstrumentationRegistry.getArguments().getString("wseUrl") ?: "ws://10.0.2.2:5007/wse"

    @Test
    fun ticksComeInOrderAndEchoAnswers() = runBlocking {
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
    fun aPayloadMustBeJson() = runBlocking {
        val client = connectClient(WseConfig(url = url))
        assertFailsWith<WseException.Encode> { client.trySend("x", "not json") }
        client.disconnect(1_000u)
    }
}
