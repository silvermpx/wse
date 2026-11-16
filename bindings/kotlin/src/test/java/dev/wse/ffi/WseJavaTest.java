// The client from Java, through Wse, against the Rust client's example server
// (run-tests.sh starts it): what a Java application calls compiles and works.
package dev.wse.ffi;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class WseJavaTest {
    private final String url = System.getenv().getOrDefault("WSE_TEST_URL", "ws://127.0.0.1:5007/wse");

    /** The next item, within ten seconds; the events must not end. */
    private static WseItem next(WseEventReader events) throws Exception {
        WseItem item = events.next(Duration.ofSeconds(10));
        assertNotNull(item, "the events ended");
        return item;
    }

    /** The next message of kind. */
    private static WseMessage message(WseEventReader events, String kind) throws Exception {
        while (true) {
            if (next(events) instanceof WseItem.Message m && m.getMessage().getKind().equals(kind)) {
                return m.getMessage();
            }
        }
    }

    @Test
    void ticksComeAndEchoAnswersAndTheEventsEndWithTheClient() throws Exception {
        WseClient client = Wse.connect(Wse.config(url).topics("ticks").insecure(true).build()).get(10, SECONDS);
        assertEquals(WseStatus.CONNECTED, client.status());
        WseEventReader events = Wse.events(client);
        while (!(next(events) instanceof WseItem.Message m && "ticks".equals(m.getMessage().getTopic()))) {
            // up to the first tick
        }
        Wse.send(client, "echo", "{\"text\":\"java\"}").get(10, SECONDS);
        assertEquals("{\"text\":\"java\"}", message(events, "echoed").getJson());
        Wse.disconnect(client, Duration.ofSeconds(1)).get(10, SECONDS);
        while (events.next(Duration.ofSeconds(10)) != null) {
            // what came before the close
        }
    }

    @Test
    void nothingInTimeIsATimeoutAndLosesNothing() throws Exception {
        WseClient client = Wse.connect(Wse.config(url).insecure(true).build()).get(10, SECONDS);
        WseEventReader events = Wse.events(client);
        // No topics: once the first items are read, nothing more comes.
        assertThrows(TimeoutException.class, () -> {
            while (true) {
                events.next(Duration.ofMillis(300));
            }
        });
        Wse.send(client, "echo", "{\"n\":1}").get(10, SECONDS);
        assertEquals("{\"n\":1}", message(events, "echoed").getJson());
        Wse.disconnect(client, Duration.ofSeconds(1)).get(10, SECONDS);
    }

    @Test
    void tokensComeFromAFuture() throws Exception {
        AtomicInteger asked = new AtomicInteger();
        WseTokenSource tokens = Wse.tokens(() -> {
            asked.incrementAndGet();
            return CompletableFuture.completedFuture("java-token");
        });
        WseClient client = Wse.connect(Wse.config(url).insecure(true).build(), tokens).get(10, SECONDS);
        assertEquals(1, asked.get());
        Wse.disconnect(client, Duration.ofSeconds(1)).get(10, SECONDS);

        WseTokenSource failing = Wse.tokens(() -> CompletableFuture.failedFuture(new IllegalStateException("signed out")));
        ExecutionException error = assertThrows(
            ExecutionException.class,
            () -> Wse.connect(Wse.config(url).insecure(true).reconnect(false).build(), failing).get(10, SECONDS));
        assertInstanceOf(WseException.class, error.getCause());
        assertTrue(error.getCause().getMessage().contains("signed out"), error.getCause().toString());
    }

    @Test
    void encryptionAndSignaturesFromJava() throws Exception {
        String identity = System.getenv("WSE_TEST_IDENTITY");
        String signing = System.getenv("WSE_TEST_SIGNING");
        WseClient sealed = Wse.connect(Wse.config(url).serverIdentityKeys(identity).insecure(true).build()).get(10, SECONDS);
        Wse.send(sealed, "echo", "{\"sealed\":true}").get(10, SECONDS);
        assertEquals("{\"sealed\":true}", message(Wse.events(sealed), "echoed").getJson());
        Wse.disconnect(sealed, Duration.ofSeconds(1)).get(10, SECONDS);

        byte[] key = Wse.generateSigningKey();
        WseClient signer = Wse.connect(
                Wse.config(url).signingKey(key).serverSigningKeys(signing).insecure(true).build())
            .get(10, SECONDS);
        WseEventReader events = Wse.events(signer);
        Wse.send(signer, "bind_key", "{\"public_key\":\"" + Wse.signingPublicKey(key) + "\"}").get(10, SECONDS);
        message(events, "key_bound");
        String nonce = Wse.sendSigned(signer, "order", "{\"side\":\"buy\"}").get(10, SECONDS);
        assertFalse(nonce.isEmpty());
        assertTrue(message(events, "receipt").getSignature().getVerified());
        Wse.disconnect(signer, Duration.ofSeconds(1)).get(10, SECONDS);
    }

    @Test
    void recoverOrResyncFromJava() throws Exception {
        WseConfig config = Wse.config(url)
            .topics(List.of("ticks"))
            .insecure(true)
            .delivery(Wse.delivery("board").maxHeld(100).build())
            .build();
        WseClient client = Wse.connect(config).get(10, SECONDS);
        WseEventReader events = Wse.events(client);
        WseItem.Resync resync = null;
        while (resync == null) {
            WseItem item = next(events);
            if (item instanceof WseItem.Message m) {
                assertNotEquals("ticks", m.getMessage().getTopic(), "a tick before the resync");
            }
            if (item instanceof WseItem.Resync r) {
                resync = r;
            }
        }
        assertEquals(List.of("ticks"), resync.getTopics());
        Wse.resynced(client, resync);
        WseMessage tick = null;
        while (tick == null) {
            if (next(events) instanceof WseItem.Message m && "ticks".equals(m.getMessage().getTopic())) {
                tick = m.getMessage();
            }
        }
        Wse.commit(client, tick);
        assertEquals(WseContinuity.CONTINUOUS, client.topicStates().get(0).getContinuity());
        assertTrue(client.checkpoint().contains("\"consumer\":\"board\""), client.checkpoint());
        Wse.disconnect(client, Duration.ofSeconds(1)).get(10, SECONDS);
    }

    @Test
    void aDroppedConnectionSaysWhenItTriesAgain() throws Exception {
        WseClient client = Wse.connect(Wse.config(url).insecure(true).build()).get(10, SECONDS);
        WseEventReader events = Wse.events(client);
        client.reconnect();
        WseItem.Disconnected dropped = null;
        while (dropped == null) {
            if (next(events) instanceof WseItem.Disconnected d) {
                dropped = d;
            }
        }
        assertTrue(Wse.retryInMillis(dropped) >= 0);
        assertTrue(Wse.attempt(dropped) >= 0);
        int code = Wse.closeCode(dropped);
        assertTrue(code == -1 || (code >= 1000 && code < 5000), "close code " + code);
        while (!(next(events) instanceof WseItem.Connected connected && connected.getReconnected())) {
            // up to the connection made again
        }
        Wse.disconnect(client, Duration.ofSeconds(1)).get(10, SECONDS);
    }
}
