# WSE client for Kotlin and Java

The WSE client for Android (API 26+) and the JVM (Java 17+): the Rust client
(`wse-client` on crates.io) through UniFFI, with suspend functions and a
Flow of events for Kotlin, and futures for Java. It keeps a connection to a
WSE server, reconnects when the connection is lost, recovers the messages it
missed, and hands the server's messages to the app in order.

## Android

The library is on Maven Central: the bindings, the Kotlin wrapper and the
Rust library for `arm64-v8a`, `armeabi-v7a` and `x86_64`, linked for 16 KB
pages; what it uses (JNA, kotlinx-coroutines, AndroidX) comes with it.

```kotlin
dependencies {
    implementation("io.github.silvermpx:wse-client-android:3.1.0")
}
```

Each release also attaches the same `wse-client-android-<version>.aar`; an app
that adds the file itself adds the libraries it uses too:

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

## The JVM

For services and desktop apps in Java or Kotlin, on Java 17 or newer. The JAR
on Maven Central carries the bindings, the Kotlin wrapper and the Rust library
for Linux (x64 and arm64, glibc 2.17 and newer on x64 and 2.28 on arm64, and
musl, as in Alpine), macOS (Apple silicon and Intel, 10.15 and newer) and
Windows (x64 and arm64). It needs no Rust and no build step; JNA,
kotlinx-coroutines and the Kotlin standard library come as its dependencies.

```kotlin
dependencies {
    implementation("io.github.silvermpx:wse-client-jvm:3.2.0")
}
```

```xml
<dependency>
  <groupId>io.github.silvermpx</groupId>
  <artifactId>wse-client-jvm</artifactId>
  <version>3.2.0</version>
</dependency>
```

JNA loads the library for the platform from the JAR; on a musl system the
musl build. Java 22 and newer print a warning when a library loads native
code: `java --enable-native-access=ALL-UNNAMED` (or `Enable-Native-Access:
ALL-UNNAMED` in an executable JAR's manifest) allows it. Each release also
attaches the same `wse-client-jvm-<version>.jar`, which needs those three
dependencies beside it.

To build it from a checkout (Rust, JDK 17 or newer and Gradle), with this
machine's library:

```sh
bindings/kotlin/build.sh                    # the bindings
bindings/kotlin/jvm-natives.sh natives "$(rustc -vV | sed -n 's/^host: //p')"
(cd bindings/kotlin && WSE_JVM_NATIVES="$PWD/../../natives" gradle jar)
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
applying a message the app calls `client.commit(message)`;
`client.checkpoint()` is what to save with its own state and give back as
`WseDelivery(checkpoint = ...)`, and recovery starts from it, so a message
can come twice: apply a repeat safely. The user changes where the events show
the server took the connection for another user: what came before is the last
user's, and a message of theirs committed later counts for nobody.

`events()` is the client's stream, until it closes; one reader at a time. A
collector cancelled in between loses nothing: the next collection gets the
rest, and an item whose collection was cancelled while it was handed out
(behind `cancellable()` or `buffer()`, say) comes again, so it may be seen
twice. Cancelling the coroutine of `connectClient` gives up the attempt.
`disconnect` sends what is queued and closes.

## Java

Java calls the client through `Wse`: the calls that wait return a
`CompletableFuture` (cancelling it gives the call up), the configuration
comes from a builder, and the events are read one at a time, the thread
waiting for each. The client's other methods (`status()`, `subscribe(...)`,
`trySend(...)`, `pause(...)`, `checkpoint()`, ...) are called on it as they
are. Android apps written in Java use the same.

```java
import dev.wse.ffi.*;

WseClient client = Wse.connect(Wse.config("wss://example.com/wse").token(token).topics("prices").build()).join();
Wse.send(client, "order", "{\"id\":1}").join();
client.trySendLive("open_valve", "{\"id\":7}");   // on this connection or not at all

WseEventReader events = Wse.events(client);
for (WseItem item; (item = events.next()) != null; ) {
    if (item instanceof WseItem.Message m) {
        System.out.println(m.getMessage().getTopic() + " " + m.getMessage().getJson());
    } else if (item instanceof WseItem.Gap gap) {
        System.out.println("lost messages on " + gap.getTopic() + ": reload it");
    }
}
```

`events.next(Duration)` waits at most that long: nothing in time is a
`TimeoutException`, and the next call goes on from there. A token that
changes comes from `Wse.tokens(() -> fetchToken())`, a supplier of a
`CompletionStage<String>` asked before each connection attempt. The
recover-or-resync delivery is `Wse.config(url).delivery(Wse.delivery("board").checkpoint(saved).build())`;
`Wse.resynced(client, resync)` and `Wse.commit(client, message)` answer its
items. Signed messages: `Wse.generateSigningKey()`, `Wse.signingPublicKey(key)`
and `Wse.sendSigned(...)`. Kotlin's unsigned numbers are hidden from Java:
`Wse.retryInMillis(...)`, `Wse.attempt(...)` and `Wse.closeCode(...)` give a
`Disconnected` item's as plain numbers.

## Tests

```sh
bindings/kotlin/run-tests.sh                     # on the JVM, in Kotlin and in Java
bindings/kotlin/run-jvm-consumer-check.sh        # the JVM package in a Java application
bindings/kotlin/android/run-emulator-tests.sh    # in an Android emulator
```

They run against the Rust client's example server, which the scripts start.
`run-jvm-consumer-check.sh` makes the JAR with this machine's library, lays
it out in a Maven repository and runs an application that installs it from
there (`ALPINE=1` on Linux runs it in Alpine too).
