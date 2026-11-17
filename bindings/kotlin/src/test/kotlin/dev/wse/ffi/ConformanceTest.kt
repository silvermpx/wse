// The Kotlin package's driver for the conformance server
// (crates/wse-conformance): it runs each case it can, step by step, and
// records what its client saw and did; the server judges. Runs when
// WSE_CONFORMANCE holds the server's control URL (run-tests.sh starts it).
package dev.wse.ffi

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.util.concurrent.CompletionStage
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull

/** What the Kotlin package has, by the catalog's names. */
private val FEATURES = listOf("outbox", "start", "live", "msgpack", "e2e", "signing", "recover_or_resync")

private fun obj(vararg fields: Pair<String, Any?>): JsonObject = JsonObject().apply {
    for ((name, value) in fields) add(name, json(value))
}

private fun json(value: Any?): JsonElement = when (value) {
    null -> JsonNull.INSTANCE
    is JsonElement -> value
    is String -> com.google.gson.JsonPrimitive(value)
    is Boolean -> com.google.gson.JsonPrimitive(value)
    is Number -> com.google.gson.JsonPrimitive(value)
    is ULong -> com.google.gson.JsonPrimitive(value.toLong())
    is UInt -> com.google.gson.JsonPrimitive(value.toLong())
    is Iterable<*> -> JsonArray().apply { value.forEach { add(json(it)) } }
    else -> error("no JSON for $value")
}

private fun JsonObject.text(name: String): String? = get(name)?.takeUnless { it.isJsonNull }?.asString

/** The control channel: requests and their answers, by id. */
private class Control private constructor(private val socket: WebSocket, private val inbox: Channel<String>) {
    private var next = 0

    suspend fun call(op: String, body: JsonObject = JsonObject()): JsonObject {
        val id = ++next
        body.addProperty("id", id)
        body.addProperty("op", op)
        socket.sendText(body.toString(), true).await()
        while (true) {
            val answer = JsonParser.parseString(inbox.receive()).asJsonObject
            if (answer["id"].asInt != id) continue
            if (answer["ok"]?.asBoolean == true) return answer
            error("$op: ${answer["error"]}")
        }
    }

    fun close() {
        socket.sendClose(WebSocket.NORMAL_CLOSURE, "")
    }

    companion object {
        suspend fun open(url: String): Control {
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
            return Control(socket, inbox)
        }
    }
}

/** One case: the client, what it saw, and the steps' doings. */
private class Run(private val begun: JsonObject, private val scope: CoroutineScope) {
    val trace = mutableListOf<JsonObject>()
    private var client: WseClient? = null
    private var reader: Job? = null
    private val items = Channel<WseItem>(Channel.UNLIMITED)
    private var ready = false
    private var rr = false
    private val published = mutableMapOf<String, MutableList<Long>>()
    /** Resync rounds waiting for a step to say the application resynced. */
    private val resyncs = mutableListOf<Pair<ULong, List<String>>>()
    private var key: ByteArray? = null
    private var bound = false
    private val began = System.nanoTime()

    private fun record(event: JsonObject) {
        event.addProperty("t", (System.nanoTime() - began) / 1_000_000)
        trace += event
    }

    suspend fun go(control: Control) {
        val steps = begun.getAsJsonArray("steps")
        for ((at, element) in steps.withIndex()) {
            val step = element.asJsonObject
            drain()
            if (step.has("server")) {
                record(obj("ev" to "step", "server" to step["server"]))
                try {
                    val done = control.call("act", obj("step" to step))
                    if (step.text("server") == "publish") {
                        published.getOrPut(done.text("topic").orEmpty()) { mutableListOf() } +=
                            done.getAsJsonArray("n").map { it.asLong }
                    }
                } catch (failure: Exception) {
                    record(obj("ev" to "error", "message" to "step $at: ${failure.message}"))
                    break
                }
                continue
            }
            try {
                if (!clientStep(step)) {
                    record(obj("ev" to "timeout", "step" to at, "waiting" to step))
                    break
                }
            } catch (failure: Exception) {
                record(obj("ev" to "error", "message" to "step $at: ${failure.message}"))
                break
            }
        }
        close()
    }

    /** A step of the client's: false when what it waits for did not come. */
    private suspend fun clientStep(step: JsonObject): Boolean {
        when (val kind = step.text("client")) {
            "connect" -> connect(step)
            "wait" -> return until(step, step["timeout_ms"]?.asLong ?: 10_000)
            "send" -> send(step)
            "pause" -> client().pause("test")
            "resume" -> client().resume("test")
            "resynced" -> {
                val topics = step.getAsJsonArray("topics").map { it.asString }
                record(obj("ev" to "resynced", "topics" to topics))
                val (done, rest) = resyncs.partition { (_, of) -> of.any { it in topics } }
                resyncs.clear()
                resyncs += rest
                for ((round, _) in done) client().resynced(round)
            }
            "sleep" -> {
                val until = System.nanoTime() + (step["ms"]?.asLong ?: 0) * 1_000_000
                while (System.nanoTime() < until) pump(until)
            }
            "close" -> close()
            else -> error("no such client step: $kind")
        }
        return true
    }

    private fun client(): WseClient = client ?: error("no client")

    private suspend fun connect(step: JsonObject) {
        // A client made again: the last one goes, as a dropped handle's does.
        if (client != null) close()
        val user = step.text("user") ?: "ann"
        rr = step["recover_or_resync"]?.asBoolean == true
        var signingKey: ByteArray? = null
        if (step["signing"]?.asBoolean == true) {
            signingKey = generateSigningKey()
            key = signingKey
        }
        val config = WseConfig(
            url = begun.text("url").orEmpty(),
            token = begun.getAsJsonObject("tokens").text(user),
            topics = step.getAsJsonArray("topics")?.map { it.asString } ?: emptyList(),
            msgpack = step.text("format") == "msgpack",
            reconnectBaseDelayMs = 50u,
            reconnectMaxDelayMs = 300u,
            reconnectJitter = false,
            serverIdentityKeys = if (step["e2e"]?.asBoolean == true) listOfNotNull(begun.text("identity_key")) else emptyList(),
            signingKey = signingKey,
            serverSigningKeys = if (signingKey != null) listOfNotNull(begun.text("signing_key")) else emptyList(),
            insecure = true,
            delivery = if (rr) WseDelivery(consumer = "conformance") else null,
        )
        val made = if (step["start"]?.asBoolean == true) startClient(config) else connectClient(config)
        client = made
        val events = made.events()
        reader = scope.launch {
            while (true) items.send(events.next() ?: break)
        }
    }

    private suspend fun send(step: JsonObject) {
        val kind = step.text("type").orEmpty()
        val count = step["count"]?.asInt ?: 1
        for (k in 1..count) {
            val id = step.text("id") ?: "$kind-$k"
            val payload = step["payload"]?.takeUnless { it.isJsonNull }?.toString() ?: obj("n" to k).toString()
            if (step["signed"]?.asBoolean == true) {
                if (!bound) {
                    val bind = runCatching {
                        client().sendWith(
                            "bind_key",
                            obj("public_key" to signingPublicKey(key ?: error("no signing key"))).toString(),
                            WseSendOptions(id = "bind-key"),
                        )
                    }
                    record(obj("ev" to "sent", "id" to "bind-key", "type" to "bind_key", "live" to false, "ok" to bind.isSuccess))
                    bind.getOrThrow()
                    val bound = until(obj("for" to "message", "type" to "key_bound"), 10_000)
                    check(bound) { "the server never bound the key" }
                    this.bound = true
                }
                val sent = runCatching { client().sendSigned(kind, payload, id) }
                record(obj("ev" to "sent", "id" to id, "type" to kind, "signed" to true, "live" to false, "ok" to sent.isSuccess))
                continue
            }
            val live = step["live"]?.asBoolean == true
            val options = WseSendOptions(
                id = id,
                correlationId = step.text("correlation_id"),
                priority = (step["priority"]?.asInt ?: 0).toUByte(),
                live = live,
            )
            val sent = runCatching {
                if (live) client().trySendWith(kind, payload, options) else client().sendWith(kind, payload, options)
            }
            record(obj("ev" to "sent", "id" to id, "type" to kind, "live" to live, "ok" to sent.isSuccess))
        }
    }

    /** Takes what the client has for the application now. */
    private fun drain() {
        while (true) take(items.tryReceive().getOrNull() ?: return)
    }

    /** Takes what comes until `deadline` (nanoTime), or the next item. */
    private suspend fun pump(deadline: Long) {
        drain()
        val wait = minOf((deadline - System.nanoTime()) / 1_000_000, 20)
        if (wait <= 0) return
        val item = withTimeoutOrNull(wait) { items.receive() } ?: return
        take(item)
    }

    private suspend fun until(step: JsonObject, ms: Long): Boolean {
        val deadline = System.nanoTime() + ms * 1_000_000
        while (true) {
            drain()
            if (holds(step)) return true
            if (System.nanoTime() >= deadline) return false
            if (client == null) delay(10) else pump(deadline)
        }
    }

    private fun take(item: WseItem) {
        when (item) {
            is WseItem.Message -> {
                val message = item.message
                val payload = message.json?.let { JsonParser.parseString(it) } ?: JsonNull.INSTANCE
                val n = (payload as? JsonObject)?.get("n")?.takeIf { it.isJsonPrimitive && it.asJsonPrimitive.isNumber }
                record(
                    obj(
                        "ev" to "message",
                        "type" to message.kind,
                        "topic" to message.topic,
                        "n" to n?.asLong,
                        "payload" to payload,
                        "cid" to message.correlationId,
                    ),
                )
                // Applied: the checkpoint moves on.
                if (rr && message.topic != null) client?.commit(message)
            }
            is WseItem.Connected -> {
                ready = true
                record(obj("ev" to "ready"))
            }
            is WseItem.Disconnected, is WseItem.Paused -> ready = false
            is WseItem.Gap -> record(obj("ev" to "break", "topic" to item.topic, "kind" to "gap"))
            is WseItem.Continuity -> if (item.continuity != WseContinuity.CONTINUOUS) {
                record(obj("ev" to "break", "topic" to item.topic, "kind" to "continuity", "reason" to item.reason))
            }
            is WseItem.Resync -> {
                val cold = item.reasons.all { it == "cold_start" }
                record(obj("ev" to "resync", "topics" to item.topics, "auto" to cold, "reasons" to item.reasons))
                // At a cold start the application has its state at once.
                if (cold) {
                    record(obj("ev" to "resynced", "topics" to item.topics, "auto" to true))
                    client?.resynced(item.round)
                } else {
                    resyncs += item.round to item.topics
                }
            }
            is WseItem.Delivered -> record(obj("ev" to "delivered", "ids" to item.ids))
            is WseItem.NotDelivered -> record(obj("ev" to "not_delivered", "reason" to item.reason, "ids" to item.ids))
            else -> {}
        }
    }

    private fun holds(step: JsonObject): Boolean {
        fun messages(pick: (JsonObject) -> Boolean) = trace.count { it.text("ev") == "message" && pick(it) }
        val topic = step.text("topic")
        return when (step.text("for")) {
            "ready" -> ready
            "not_ready" -> !ready
            "messages" -> messages { it.text("topic") == topic } >= (step["count"]?.asInt ?: 1)
            "message" -> messages { it.text("type") == step.text("type") } >= (step["count"]?.asInt ?: 1)
            "published" -> {
                val last = step["last"]?.asInt ?: 1
                val numbers = published[topic].orEmpty()
                val wanted = numbers.takeLast(last)
                wanted.size == last && wanted.all { n ->
                    trace.any { it.text("ev") == "message" && it.text("topic") == topic && it["n"]?.takeUnless { v -> v.isJsonNull }?.asLong == n }
                }
            }
            "break" -> trace.any {
                (it.text("ev") == "break" && it.text("topic") == topic) ||
                    (it.text("ev") == "resync" && it.getAsJsonArray("topics").any { t -> t.asString == topic })
            }
            "resync" -> trace.any {
                it.text("ev") == "resync" && it["auto"]?.asBoolean != true &&
                    it.getAsJsonArray("topics").any { t -> t.asString == topic }
            }
            "delivered" -> {
                val delivered = trace.filter { it.text("ev") == "delivered" }.flatMap { e -> e.getAsJsonArray("ids").map { it.asString } }.toSet()
                val ids = if (step["all"]?.asBoolean == true) {
                    trace.filter {
                        it.text("ev") == "sent" && it["ok"]?.asBoolean == true &&
                            it["live"]?.asBoolean != true && it["signed"]?.asBoolean != true
                    }.mapNotNull { it.text("id") }
                } else {
                    step.getAsJsonArray("ids").map { it.asString }
                }
                ids.all { it in delivered }
            }
            else -> false
        }
    }

    suspend fun close() {
        drain()
        client?.let {
            it.disconnect(1_000u)
            // What came before the close is the case's too.
            reader?.join()
        }
        client = null
        reader = null
        drain()
        ready = false
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
        val control = Control.open(url)
        val hello = control.call(
            "hello",
            obj(
                "agent" to "kotlin",
                "version" to "3.2.0",
                "runtime" to "jvm ${System.getProperty("java.version")}",
                "os" to System.getProperty("os.name"),
                "arch" to System.getProperty("os.arch"),
                "features" to FEATURES,
            ),
        )
        val failed = mutableListOf<String>()
        for (case in hello.getAsJsonArray("cases")) {
            val name = case.asJsonObject.text("case").orEmpty()
            val begun = control.call("begin", obj("case" to name))
            val run = Run(begun, this)
            run.go(control)
            for (chunk in run.trace.chunked(500)) control.call("trace", obj("events" to chunk))
            val judged = control.call("end")
            if (judged.text("verdict") != "pass") failed += "$name: ${judged["failures"]}"
        }
        control.call("bye")
        control.close()
        assertEquals(emptyList(), failed)
    }
}
