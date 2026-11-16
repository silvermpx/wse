# WSE client for Kotlin and Android

The WSE client for Android (API 26+) and the JVM: the Rust client
(`wse-client` on crates.io) through UniFFI, with suspend functions and a
Flow of events. It keeps a connection to a WSE server, reconnects when the
connection is lost, recovers the messages it missed, and hands the server's
messages to the app in order.

## Android

Each release attaches `wse-client-android-<version>.aar`: the bindings, the
Kotlin wrapper and the Rust library for `arm64-v8a`, `armeabi-v7a` and
`x86_64`, linked for 16 KB pages. An app adds it with the libraries it uses:

```kotlin
dependencies {
    implementation(files("libs/wse-client-android-3.1.0.aar"))
    implementation("net.java.dev.jna:jna:5.19.1@aar")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("androidx.lifecycle:lifecycle-process:2.11.0")
    implementation("androidx.startup:startup-runtime:1.2.0")
}
```

The AAR carries consumer rules for R8 that keep what JNA and the bindings
reach by reflection, so a release build with R8 works as it is
(`android/run-consumer-check.sh` builds such an app and runs it on an
emulator). The app needs the `INTERNET` permission.

To build it from a checkout (Rust with the Android targets, cargo-ndk, the
Android NDK, JDK 21 and Gradle):

```sh
bindings/kotlin/build.sh                    # the bindings
bindings/kotlin/android/build-libs.sh       # the Rust library of every ABI
(cd bindings/kotlin/android && gradle assembleRelease)
```

## Use

```kotlin
import dev.wse.ffi.*

val client = connectClient(WseConfig(url = "wss://example.com/wse", token = token, topics = listOf("prices")))
client.send("order", """{"id":1}""")
client.trySendLive("open_valve", """{"id":7}""")   // on this connection or not at all
client.events().asFlow().collect { item ->
    when (item) {
        is WseItem.Message -> println("${item.message.topic} ${item.message.kind}: ${item.message.json}")
        is WseItem.Gap -> println("lost messages on ${item.topic}: reload it")
        else -> {}
    }
}
```

On Android, `WseAppLifecycle(client).start()` pauses the client five seconds
after the app went to the background and resumes it when it comes back (the
process's lifecycle: an Activity turning does not touch the connection), and
`WseNetworkMonitor(context, client).start()` pauses it while there is no
network and makes the connection again when the network changes. Both pause
for reasons of their own: `client.pause("manual")` stays until the app
resumes it.

A wss:// server's certificate is checked by the system's verifier (the
default, `trust = null`): the roots the device trusts. `trust =
WseTrust.Roots(listOf(ca))` trusts only a private CA's certificate (PEM or
DER), `WseTrust.Mozilla` the Mozilla roots built into the library. ws://
sends everything in the clear and needs `insecure = true` (for development).
On Android the library sets the verifier up as the app starts (through
androidx.startup); an app that removed that provider calls
`WseTls.init(context)` before it connects. Android checks revocation where
it can fetch the CA's lists, which come over http: an app whose network
security config refuses http to them connects without that check.

`client.topicStates()` says of each topic whether the server took it and
whether its messages run without a break. Where every fact counts,
`delivery = WseDelivery(consumer = "board")` (recover-or-resync) holds a topic
that is not continuous -- from a cold start, after every reconnect, after a
break -- until the app brought its state up to date: a `WseItem.Resync` asks
for it, and `client.resynced(item.round)` hands on what was held. After
applying a message the app calls `client.commit(topic, epoch, offset)`;
`client.checkpoint()` is what to save with its own state and give back as
`WseDelivery(checkpoint = ...)`, and recovery starts from it, so a message
can come twice: apply a repeat safely.

`events()` is the client's stream, until it closes; one reader at a time. A
collector cancelled in between loses nothing: the next collection gets the
rest. Cancelling the coroutine of `connectClient` gives up the attempt.
`disconnect` sends what is queued and closes.

## Tests

```sh
bindings/kotlin/run-tests.sh                     # on the JVM
bindings/kotlin/android/run-emulator-tests.sh    # in an Android emulator
```

They run against the Rust client's example server, which the scripts start.
