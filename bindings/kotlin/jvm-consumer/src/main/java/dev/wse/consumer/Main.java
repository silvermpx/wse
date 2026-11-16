// Checks the WSE JVM package the way a Java application uses it, against the
// server its arguments name, and prints the outcome: "ok", or what went wrong
// (and exits with 1). The system's certificate verifier turns down the wss://
// door of a CA it does not know, and a client that names that CA connects; a
// client hears a tick; an end-to-end encrypted session with the pinned server
// echoes; a signed message gets a signed receipt.
//
//   Main <ws:// URL> <wss:// URL> <CA file> <identity key> <signing key>
package dev.wse.consumer;

import static java.util.concurrent.TimeUnit.SECONDS;

import dev.wse.ffi.Wse;
import dev.wse.ffi.WseClient;
import dev.wse.ffi.WseEventReader;
import dev.wse.ffi.WseException;
import dev.wse.ffi.WseItem;
import dev.wse.ffi.WseMessage;
import dev.wse.ffi.WseStatus;
import dev.wse.ffi.WseTrust;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class Main {
    private static final Duration WAIT = Duration.ofSeconds(20);

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("usage: Main <ws:// URL> <wss:// URL> <CA file> <identity key> <signing key>");
            System.exit(2);
        }
        String outcome;
        try {
            outcome = check(args[0], args[1], Files.readAllBytes(Path.of(args[2])), args[3], args[4]);
        } catch (Throwable error) {
            outcome = "failed: " + (error instanceof ExecutionException ? error.getCause() : error);
        }
        System.out.println(outcome);
        System.exit("ok".equals(outcome) ? 0 : 1);
    }

    private static String check(String url, String tlsUrl, byte[] ca, String identity, String signing) throws Exception {
        try {
            WseClient stranger = Wse.connect(Wse.config(tlsUrl).reconnect(false).build()).get(20, SECONDS);
            Wse.disconnect(stranger, Duration.ofSeconds(1)).get(20, SECONDS);
            return "connected to a CA nobody trusts";
        } catch (ExecutionException error) {
            if (!(error.getCause() instanceof WseException.Connect connect)) {
                return "the verifier said " + error.getCause();
            }
            if (!connect.getReason().contains("certificate")) {
                return "the verifier said " + connect.getReason();
            }
        }
        WseClient trusting = Wse.connect(
                Wse.config(tlsUrl).reconnect(false).trust(new WseTrust.Roots(List.of(ca))).build())
            .get(20, SECONDS);
        if (trusting.status() != WseStatus.CONNECTED) {
            return "the client that names the CA is " + trusting.status();
        }
        Wse.disconnect(trusting, Duration.ofSeconds(1)).get(20, SECONDS);

        WseClient client = Wse.connect(Wse.config(url).topics("ticks").insecure(true).build()).get(20, SECONDS);
        WseEventReader events = Wse.events(client);
        while (true) {
            WseItem item = events.next(WAIT);
            if (item == null) {
                return "the events ended without a tick";
            }
            if (item instanceof WseItem.Message m && "ticks".equals(m.getMessage().getTopic())) {
                break;
            }
        }
        Wse.disconnect(client, Duration.ofSeconds(1)).get(20, SECONDS);

        WseClient sealed = Wse.connect(Wse.config(url).serverIdentityKeys(identity).insecure(true).build())
            .get(20, SECONDS);
        Wse.send(sealed, "echo", "{\"sealed\":true}").get(20, SECONDS);
        WseMessage echoed = message(Wse.events(sealed), "echoed");
        Wse.disconnect(sealed, Duration.ofSeconds(1)).get(20, SECONDS);
        if (echoed == null) {
            return "no echo on the encrypted session";
        }
        if (!"{\"sealed\":true}".equals(echoed.getJson())) {
            return "the encrypted echo said " + echoed.getJson();
        }

        byte[] key = Wse.generateSigningKey();
        WseClient signer = Wse.connect(
                Wse.config(url).signingKey(key).serverSigningKeys(signing).insecure(true).build())
            .get(20, SECONDS);
        WseEventReader signed = Wse.events(signer);
        Wse.send(signer, "bind_key", "{\"public_key\":\"" + Wse.signingPublicKey(key) + "\"}").get(20, SECONDS);
        if (message(signed, "key_bound") == null) {
            return "the key was not bound";
        }
        Wse.sendSigned(signer, "order", "{\"side\":\"buy\"}", "jvm-1").get(20, SECONDS);
        WseMessage receipt = message(signed, "receipt");
        Wse.disconnect(signer, Duration.ofSeconds(1)).get(20, SECONDS);
        if (receipt == null) {
            return "no receipt";
        }
        if (receipt.getSignature() == null || !receipt.getSignature().getVerified()) {
            return "the receipt's signature was not verified";
        }
        return "ok";
    }

    /** The next message of kind, or null once the events end. */
    private static WseMessage message(WseEventReader events, String kind) throws Exception {
        while (true) {
            WseItem item = events.next(WAIT);
            if (item == null) {
                return null;
            }
            if (item instanceof WseItem.Message m && m.getMessage().getKind().equals(kind)) {
                return m.getMessage();
            }
        }
    }
}
