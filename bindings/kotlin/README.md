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
}
```

The AAR carries consumer rules for R8 that keep what JNA and the bindings
reach by reflection. The app needs the `INTERNET` permission.

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
