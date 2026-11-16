// Connects to the server the intent names, waits for a tick, checks that
// Android's certificate verifier turns down the wss:// door of a CA it does
// not know, and logs the outcome (tag WseConsumer): "ok", or what went wrong.
// It shows no text.
package dev.wse.consumer

import android.app.Activity
import android.os.Bundle
import android.util.Log
import dev.wse.ffi.WseConfig
import dev.wse.ffi.WseException
import dev.wse.ffi.WseItem
import dev.wse.ffi.connectClient
import dev.wse.ffi.events
import dev.wse.ffi.send
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout

class MainActivity : Activity() {
    private val scope = CoroutineScope(Dispatchers.IO)

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val url = intent.getStringExtra("url") ?: return finish()
        val tlsUrl = intent.getStringExtra("tls_url") ?: return finish()
        scope.launch {
            val outcome = try {
                withTimeout(30_000) {
                    val refused = try {
                        connectClient(WseConfig(url = tlsUrl, reconnect = false)).disconnect(1_000u)
                        "connected to a CA nobody trusts"
                    } catch (error: WseException.Connect) {
                        if ("certificate" in error.reason) null else "the verifier said ${error.reason}"
                    }
                    if (refused != null) return@withTimeout refused
                    val client = connectClient(WseConfig(url = url, topics = listOf("ticks"), insecure = true))
                    client.send("echo", """{"text":"r8"}""")
                    client.trySendLive("echo", """{"text":"live"}""")
                    val events = client.events()
                    var heard = false
                    while (!heard) {
                        val item = events.next() ?: break
                        heard = item is WseItem.Message && item.message.topic == "ticks"
                    }
                    client.disconnect(1_000u)
                    if (heard) "ok" else "the events ended without a tick"
                }
            } catch (error: Throwable) {
                "failed: $error"
            }
            Log.i("WseConsumer", outcome)
        }
    }

    override fun onDestroy() {
        scope.cancel()
        super.onDestroy()
    }
}
