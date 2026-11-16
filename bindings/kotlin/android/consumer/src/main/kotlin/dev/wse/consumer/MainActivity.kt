// Checks the release AAR in an app built with R8, against the server the
// intent names, and logs the outcome (tag WseConsumer): "ok", or what went
// wrong. Android's certificate verifier turns down the wss:// door of a CA
// it does not know; a client hears a tick; an end-to-end encrypted session
// with the pinned server echoes; a signed message gets a signed receipt. It
// shows no text.
package dev.wse.consumer

import android.app.Activity
import android.os.Bundle
import android.util.Log
import dev.wse.ffi.WseClient
import dev.wse.ffi.WseConfig
import dev.wse.ffi.WseException
import dev.wse.ffi.WseItem
import dev.wse.ffi.WseMessage
import dev.wse.ffi.connectClient
import dev.wse.ffi.events
import dev.wse.ffi.generateSigningKey
import dev.wse.ffi.send
import dev.wse.ffi.sendSigned
import dev.wse.ffi.signingPublicKey
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
        val identity = intent.getStringExtra("identity") ?: return finish()
        val signing = intent.getStringExtra("signing") ?: return finish()
        scope.launch {
            val outcome = try {
                withTimeout(60_000) { check(url, tlsUrl, identity, signing) }
            } catch (error: Throwable) {
                "failed: $error"
            }
            Log.i("WseConsumer", outcome)
        }
    }

    private suspend fun check(url: String, tlsUrl: String, identity: String, signing: String): String {
        try {
            connectClient(WseConfig(url = tlsUrl, reconnect = false)).disconnect(1_000u)
            return "connected to a CA nobody trusts"
        } catch (error: WseException.Connect) {
            if ("certificate" !in error.reason) return "the verifier said ${error.reason}"
        }

        val client = connectClient(WseConfig(url = url, topics = listOf("ticks"), insecure = true))
        client.send("echo", """{"text":"r8"}""")
        client.trySendLive("echo", """{"text":"live"}""")
        val events = client.events()
        while (true) {
            val item = events.next() ?: return "the events ended without a tick"
            if (item is WseItem.Message && item.message.topic == "ticks") break
        }
        client.disconnect(1_000u)

        val sealed = connectClient(WseConfig(url = url, serverIdentityKeys = listOf(identity), insecure = true))
        sealed.send("echo", """{"sealed":true}""")
        val echoed = message(sealed, "echoed") ?: return "no echo on the encrypted session"
        sealed.disconnect(1_000u)
        if (echoed.json != """{"sealed":true}""") return "the encrypted echo said ${echoed.json}"

        val key = generateSigningKey()
        val signer = connectClient(
            WseConfig(url = url, signingKey = key, serverSigningKeys = listOf(signing), insecure = true),
        )
        signer.send("bind_key", """{"public_key":"${signingPublicKey(key)}"}""")
        message(signer, "key_bound") ?: return "the key was not bound"
        signer.sendSigned("order", """{"side":"buy"}""", "r8-1")
        val receipt = message(signer, "receipt") ?: return "no receipt"
        signer.disconnect(1_000u)
        if (receipt.signature?.verified != true) return "the receipt's signature was not verified"
        return "ok"
    }

    /** The next message of `kind`, or null once the events end. */
    private suspend fun message(client: WseClient, kind: String): WseMessage? {
        val events = client.events()
        while (true) {
            val item = events.next() ?: return null
            if (item is WseItem.Message && item.message.kind == kind) return item.message
        }
    }

    override fun onDestroy() {
        scope.cancel()
        super.onDestroy()
    }
}
