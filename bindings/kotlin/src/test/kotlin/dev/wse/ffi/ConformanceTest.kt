// The Kotlin package against the conformance server (crates/wse-conformance),
// on the JVM. Runs when WSE_CONFORMANCE holds the server's control URL
// (run-tests.sh starts it); the driver is src/conformance's.
package dev.wse.ffi

import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.util.concurrent.CompletionStage
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking

/** The control channel on the JDK's WebSocket. */
private class JdkSocket private constructor(private val socket: WebSocket, private val inbox: Channel<String>) :
    ControlSocket {
    override suspend fun send(text: String) {
        socket.sendText(text, true).await()
    }

    override suspend fun receive(): String = inbox.receive()

    override fun close() {
        socket.sendClose(WebSocket.NORMAL_CLOSURE, "")
    }

    companion object {
        suspend fun open(url: String): JdkSocket {
            val inbox = Channel<String>(Channel.UNLIMITED)
            val listener = object : WebSocket.Listener {
                private val text = StringBuilder()

                override fun onText(socket: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
                    text.append(data)
                    if (last) {
                        inbox.trySend(text.toString())
                        text.setLength(0)
                    }
                    socket.request(1)
                    return null
                }
            }
            val socket = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(URI(url), listener).await()
            return JdkSocket(socket, inbox)
        }
    }
}

class ConformanceTest {
    @Test
    fun theKotlinPackagePassesEveryCaseItCan(): Unit = runBlocking {
        val url = System.getenv("WSE_CONFORMANCE")
        if (url == null) {
            check(System.getenv("WSE_CONFORMANCE_REQUIRED") != "1") {
                "WSE_CONFORMANCE_REQUIRED is set, but WSE_CONFORMANCE is not"
            }
            return@runBlocking
        }
        val agent = Agent(
            agent = "kotlin",
            runtime = "jvm ${System.getProperty("java.version")}",
            os = System.getProperty("os.name"),
            arch = System.getProperty("os.arch"),
        )
        assertEquals(emptyList(), runCases(JdkSocket.open(url), agent))
    }
}
