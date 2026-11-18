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

    /** The next item of the type asked for. */
    private suspend inline fun <reified T : WseItem> next(events: WseEvents): T {
        while (true) {
            val item = events.next() ?: error("the events ended before a ${T::class.simpleName}")
            if (item is T) return item
        }
    }

    /** The next message of `kind`. */
    private suspend fun message(events: WseEvents, kind: String): WseMessage {
        while (true) {
            val item = events.next() ?: error("the events ended before $kind")
            if (item is WseItem.Message && item.message.kind == kind) return item.message
        }
    }

    @Test
    fun anEncryptedSessionAndSignedMessagesOnTheDevice(): Unit = runBlocking {
        val arguments = InstrumentationRegistry.getArguments()
        val identity = checkNotNull(arguments.getString("wseIdentity")) { "run-emulator-tests.sh passes the key" }
        val signing = checkNotNull(arguments.getString("wseSigning")) { "run-emulator-tests.sh passes the key" }
        val sealed = connectClient(WseConfig(url = url, serverIdentityKeys = listOf(identity), insecure = true))
        sealed.send("echo", """{"sealed":true}""")
        assertEquals("""{"sealed":true}""", message(sealed.events(), "echoed").json)
        sealed.disconnect(1_000u)

        val key = generateSigningKey()
        val client = connectClient(
            WseConfig(url = url, signingKey = key, serverSigningKeys = listOf(signing), insecure = true),
        )
        val events = client.events()
        client.send("bind_key", """{"public_key":"${signingPublicKey(key)}"}""")
        assertEquals("""{"bound":true}""", message(events, "key_bound").json)
        client.sendSigned("order", """{"side":"buy"}""", "android-1")
        val receipt = message(events, "receipt")
        assertTrue(""""n":"android-1"""" in receipt.json.orEmpty(), receipt.json)
        assertEquals(true, receipt.signature?.verified)
        client.disconnect(1_000u)
    }

    @Test
    fun aPrivateCaIsTrustedWhereItIsNamed(): Unit = runBlocking {
        val arguments = InstrumentationRegistry.getArguments()
        val tlsUrl = checkNotNull(arguments.getString("wseTlsUrl")) { "run-emulator-tests.sh passes the wss:// door" }
        val ca = android.util.Base64.decode(arguments.getString("wseCa"), android.util.Base64.DEFAULT)
        val client = connectClient(WseConfig(url = tlsUrl, trust = WseTrust.Roots(listOf(ca))))
        assertEquals(WseStatus.CONNECTED, client.status())
        client.disconnect(1_000u)
        // Android's verifier (set up as the app started) knows that CA only where the device
        // trusts it (run-emulator-tests.sh adds it to the CAs the owner added, which this app
        // trusts); the Mozilla roots never do.
        val deviceTrusts = arguments.getString("wseCaTrusted") == "1"
        if (deviceTrusts) {
            val trusted = connectClient(WseConfig(url = tlsUrl, reconnect = false, trust = WseTrust.Platform))
            assertEquals(WseStatus.CONNECTED, trusted.status())
            trusted.disconnect(1_000u)
        }
        val unknown = if (deviceTrusts) listOf(WseTrust.Mozilla) else listOf(WseTrust.Platform, WseTrust.Mozilla)
        for (trust in unknown) {
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
    fun theProcessInTheBackgroundPausesTheClient(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, insecure = true))
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
        val client = connectClient(WseConfig(url = url, insecure = true))
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

    @Test
    fun theDevicesNetworkGoingAndComingPausesAndBringsBackTheClient(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, insecure = true))
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val monitor = WseNetworkMonitor(context, client)
        onMain { monitor.start() }
        try {
            // The device's own network goes: airplane mode, as a user turns it on.
            shell("cmd connectivity airplane-mode enable")
            waitFor(30_000) { client.status() == WseStatus.PAUSED }
            shell("cmd connectivity airplane-mode disable")
            waitFor(60_000) { client.status() == WseStatus.CONNECTED }
        } finally {
            shell("cmd connectivity airplane-mode disable")
            onMain { monitor.stop() }
            client.disconnect(1_000u)
        }
    }

    private fun shell(command: String) {
        InstrumentationRegistry.getInstrumentation().uiAutomation.executeShellCommand(command).close()
    }

    private fun onMain(run: () -> Unit) = InstrumentationRegistry.getInstrumentation().runOnMainSync(run)

    private suspend fun waitFor(millis: Long = 5_000, done: () -> Boolean) {
        withTimeout(millis) {
            while (!done()) delay(20)
        }
    }

    @Test
    fun aPayloadMustBeJson(): Unit = runBlocking {
        val client = connectClient(WseConfig(url = url, insecure = true))
        assertFailsWith<WseException.Encode> { client.trySend("x", "not json") }
        client.disconnect(1_000u)
    }

    /**
     * The outbox in a file of the app's own, its locks taken in the app's
     * sandbox: what is sent with no connection is in the file, another client
     * of the file keeps its own in memory, and it goes once the connection is back.
     */
    @Test
    fun anOutboxInTheAppsFilesIsTheClientsAlone(): Unit = runBlocking {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val file = java.io.File(context.cacheDir, "wse-outbox-test")
        java.io.File(context.cacheDir, "wse-outbox-test.lock").delete()
        file.delete()
        val config = WseConfig(url = url, insecure = true, offline = WseOffline(path = file.path))
        val client = connectClient(config)
        val events = client.events()
        client.pause("test")
        while (client.status() == WseStatus.CONNECTED) delay(20)
        client.send("echo", """{"n":1}""")
        assertEquals(1u, client.kept())
        // In the file only when it opened, both its locks taken.
        waitFor(10_000) { file.length() > 0 }

        val other = connectClient(config)
        val refused = withTimeout(10_000) { next<WseItem.StorageFailed>(other.events()) }
        assertEquals("open", refused.operation)
        assertTrue("another client's" in refused.message, refused.message)
        other.disconnect(1_000u)

        client.resume("test")
        assertEquals("""{"n":1}""", message(events, "echoed").json)
        client.disconnect(1_000u)
        file.delete()
    }
}
