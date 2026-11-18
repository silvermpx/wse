// The Kotlin package against the conformance server (crates/wse-conformance)
// on Android, in an emulator, which reaches the server as 10.0.2.2:
// run-emulator-tests.sh starts it and passes its control channel's URL as
// the wseConformance argument. The driver is ../src/conformance's.
package dev.wse.ffi

import android.os.Build
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlin.test.assertEquals
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.junit.Test
import org.junit.runner.RunWith

/** The control channel on OkHttp's WebSocket (Android has no java.net.http). */
private class OkHttpSocket(url: String) : ControlSocket {
    private val inbox = Channel<String>(Channel.UNLIMITED)
    private val opened = CompletableDeferred<Unit>()
    private val socket = OkHttpClient().newWebSocket(
        Request.Builder().url(url).build(),
        object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                opened.complete(Unit)
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                inbox.trySend(text)
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                opened.completeExceptionally(t)
                inbox.close(t)
            }
        },
    )

    override suspend fun send(text: String) {
        opened.await()
        check(socket.send(text)) { "the control channel is closed" }
    }

    override suspend fun receive(): String = inbox.receive()

    override fun close() {
        socket.close(1000, null)
    }
}

@RunWith(AndroidJUnit4::class)
class ConformanceAndroidTest {
    @Test
    fun theKotlinPackagePassesEveryCaseItCanOnAndroid(): Unit = runBlocking {
        val url = InstrumentationRegistry.getArguments().getString("wseConformance") ?: return@runBlocking
        val agent = Agent(
            agent = "kotlin-android",
            runtime = "android ${Build.VERSION.RELEASE} (API ${Build.VERSION.SDK_INT})",
            os = "Android ${Build.VERSION.RELEASE}",
            arch = Build.SUPPORTED_ABIS.firstOrNull().orEmpty(),
        )
        assertEquals(emptyList(), runCases(OkHttpSocket(url), agent))
    }
}
