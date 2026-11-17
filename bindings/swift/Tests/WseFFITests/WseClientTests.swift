// Against the Rust client's example server: run.sh starts it and passes its
// URL in WSE_TEST_URL.

import XCTest
#if canImport(UIKit)
import UIKit
#endif
@testable import WseFFI

final class WseClientTests: XCTestCase {
    private var url: String {
        ProcessInfo.processInfo.environment["WSE_TEST_URL"] ?? "ws://127.0.0.1:5007/wse"
    }

    /// The server's wss:// door and the file with its CA's certificate.
    private var tls: (url: String, ca: Data)? {
        let env = ProcessInfo.processInfo.environment
        guard let url = env["WSE_TEST_TLS_URL"], let path = env["WSE_TEST_CA"],
              let ca = FileManager.default.contents(atPath: path) else { return nil }
        return (url, ca)
    }

    func testRecoverOrResyncHoldsATopicUntilTheAppResyncedIt() async throws {
        let client = try await WseClient.connect(
            WseConfig(url: url, topics: ["ticks"], insecure: true, delivery: WseDelivery(consumer: "board")))
        var round: UInt64?
        for try await item in client.events() {
            if case let .message(message) = item, message.topic == "ticks" {
                XCTFail("a tick before the resync")
            }
            if case let .resync(r, topics, reasons, _) = item {
                XCTAssertEqual(topics, ["ticks"])
                XCTAssertEqual(reasons, ["cold_start"])
                round = r
                break
            }
        }
        XCTAssertEqual(client.topicStates().first?.continuity, .resyncing)
        // Ticks come meanwhile, and wait.
        try await Task.sleep(nanoseconds: 300_000_000)
        client.resynced(round: try XCTUnwrap(round))
        for try await item in client.events() {
            if case let .message(message) = item, message.topic == "ticks" {
                client.commit(message)
                break
            }
        }
        XCTAssertEqual(client.topicStates().first?.continuity, .continuous)
        let saved = try JSONSerialization.jsonObject(with: Data(client.checkpoint().utf8)) as? [String: Any]
        XCTAssertEqual(saved?["consumer"] as? String, "board")
        XCTAssertNotNil((saved?["topics"] as? [String: Any])?["ticks"])
        await client.disconnect(deadlineMs: 1_000)
    }

    func testASendThatRequiresATopicWaitsForItsResync() async throws {
        let client = try await WseClient.connect(
            WseConfig(url: url, topics: ["ticks"], insecure: true, delivery: WseDelivery(consumer: "board")))
        // One sequence, held across the reads.
        let events = client.events()
        var round: UInt64?
        for try await item in events {
            if case let .resync(r, _, _, _) = item {
                round = r
                break
            }
        }
        func echoed() async throws -> String? {
            for try await item in events {
                if case let .message(message) = item, message.kind == "echoed" { return message.json }
            }
            return nil
        }
        try await client.sendRequiring(kind: "echo", json: #"{"n":1}"#, topics: ["ticks"])
        try await client.send(kind: "echo", json: #"{"n":2}"#)
        // The one that requires nothing goes past it.
        let first = try await echoed()
        XCTAssertEqual(first, #"{"n":2}"#)
        client.resynced(round: try XCTUnwrap(round))
        let second = try await echoed()
        XCTAssertEqual(second, #"{"n":1}"#)
        await client.disconnect(deadlineMs: 1_000)
    }

    private func env(_ name: String) throws -> String {
        try XCTUnwrap(ProcessInfo.processInfo.environment[name], "run-tests.sh passes \(name)")
    }

    /// The next message of `kind`.
    private func message(_ client: WseClient, _ kind: String) async throws -> WseMessage {
        for try await item in client.events() {
            if case let .message(message) = item, message.kind == kind { return message }
        }
        throw WseError.Closed
    }

    func testAnEncryptedSessionWithThePinnedServer() async throws {
        let identity = try env("WSE_TEST_IDENTITY")
        let client = try await WseClient.connect(WseConfig(url: url, serverIdentityKeys: [identity], insecure: true))
        try await client.send(kind: "echo", json: #"{"sealed":true}"#)
        let echoed = try await message(client, "echoed")
        XCTAssertEqual(echoed.json, #"{"sealed":true}"#)
        await client.disconnect(deadlineMs: 1_000)
        // Another server's key: refused.
        let other = try signingPublicKey(key: generateSigningKey())
        do {
            _ = try await WseClient.connect(
                WseConfig(url: url, reconnect: false, serverIdentityKeys: [other], insecure: true))
            XCTFail("connected to a server of another key")
        } catch let error as WseError {
            guard case .Encryption = error else { return XCTFail("\(error)") }
        }
    }

    func testSignedMessagesBothWays() async throws {
        let signing = try env("WSE_TEST_SIGNING")
        let key = generateSigningKey()
        let client = try await WseClient.connect(
            WseConfig(url: url, signingKey: key, serverSigningKeys: [signing], insecure: true))
        try await client.send(kind: "bind_key", json: "{\"public_key\":\"\(try signingPublicKey(key: key))\"}")
        let bound = try await message(client, "key_bound")
        XCTAssertEqual(bound.json, #"{"bound":true}"#)
        let nonce = try await client.sendSigned(kind: "order", json: #"{"side":"buy"}"#, nonce: "swift-1")
        XCTAssertEqual(nonce, "swift-1")
        let receipt = try await message(client, "receipt")
        XCTAssertTrue(receipt.json?.contains(#""n":"swift-1""#) == true, receipt.json ?? "")
        XCTAssertEqual(receipt.signature?.verified, true)
        await client.disconnect(deadlineMs: 1_000)
    }

    func testWsGoesInTheClearOnlyWhenAsked() async throws {
        do {
            _ = try await WseClient.connect(WseConfig(url: url))
            XCTFail("connected over ws:// without insecure")
        } catch let error as WseError {
            guard case .Config = error else { return XCTFail("\(error)") }
        }
    }

    func testAPrivateCaIsTrustedWhereItIsNamed() async throws {
        let (tlsUrl, ca) = try XCTUnwrap(tls, "run-tests.sh passes the wss:// door")
        let client = try await WseClient.connect(WseConfig(url: tlsUrl, trust: .roots(certificates: [ca])))
        XCTAssertEqual(client.status(), .connected)
        await client.disconnect(deadlineMs: 1_000)
        // The system's verifier knows that CA only where the system trusts it
        // (run-tests.sh adds it to the simulator's trusted roots); the
        // Mozilla roots never do.
        let systemTrusts = ProcessInfo.processInfo.environment["WSE_TEST_CA_TRUSTED"] == "1"
        if systemTrusts {
            let trusted = try await WseClient.connect(WseConfig(url: tlsUrl, reconnect: false, trust: .platform))
            XCTAssertEqual(trusted.status(), .connected)
            await trusted.disconnect(deadlineMs: 1_000)
        }
        for trust in systemTrusts ? [WseTrust.mozilla] : [.platform, .mozilla] {
            do {
                _ = try await WseClient.connect(WseConfig(url: tlsUrl, reconnect: false, trust: trust))
                XCTFail("\(trust): connected")
            } catch let error as WseError {
                guard case let .Connect(reason) = error, reason.contains("certificate") else {
                    return XCTFail("\(trust): \(error)")
                }
            }
        }
    }

    func testTicksComeInOrderAndEchoAnswers() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, topics: ["ticks"], insecure: true))
        XCTAssertEqual(client.status(), .connected)
        var ticks: [UInt64] = []
        var echoed: String?
        try await client.send(kind: "echo", json: #"{"text":"hallo"}"#)
        for try await item in client.events() {
            if case let .message(message) = item {
                if message.kind == "echoed" { echoed = message.json }
                if message.topic == "ticks", let offset = message.offset { ticks.append(offset) }
            }
            if ticks.count >= 3 && echoed != nil { break }
        }
        XCTAssertEqual(echoed, #"{"text":"hallo"}"#)
        XCTAssertEqual(ticks, ticks.sorted())
        await client.disconnect(deadlineMs: 1_000)
    }

    func testDisconnectEndsTheEvents() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        let events = client.events()
        let reader = Task { () -> Int in
            var count = 0
            for try await _ in events { count += 1 }
            return count
        }
        try await Task.sleep(nanoseconds: 200_000_000)
        await client.disconnect(deadlineMs: 1_000)
        let count = try await reader.value
        XCTAssertGreaterThan(count, 0)  // at least the connected event
    }

    func testACancelledConnectGivesUp() async throws {
        // Nothing answers there: the attempt waits until cancelled.
        let attempt = Task {
            try await WseClient.connect(WseConfig(url: "ws://10.255.255.1:9/wse", connectTimeoutMs: 30_000, insecure: true))
        }
        try await Task.sleep(nanoseconds: 100_000_000)
        attempt.cancel()
        do {
            _ = try await attempt.value
            XCTFail("connected")
        } catch let error as WseError {
            guard case .Cancelled = error else { return XCTFail("\(error)") }
        }
    }

    func testALiveMessageGoesOnTheReadyConnection() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        try client.trySendLive(kind: "valve", json: #"{"open":true}"#)
        try await client.sendLive(kind: "valve", json: #"{"open":false}"#)
        await client.disconnect(deadlineMs: 1_000)
        XCTAssertThrowsError(try client.trySendLive(kind: "valve", json: "{}"))
    }

    func testACancelledSendDoesNotGoOut() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        let cancel = WseCancel()
        cancel.cancel()
        do {
            try await client.send(kind: "order", json: "{}", cancel: cancel)
            XCTFail("a cancelled send went out")
        } catch WseError.Cancelled {}
        // The wrapper hands the Task's cancellation to the call.
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            try await client.send(kind: "order", json: "{}")
        }
        do {
            try await task.value
            XCTFail("a cancelled Task sent")
        } catch WseError.Cancelled {}
        try await client.send(kind: "after", json: "{}")
        await client.disconnect(deadlineMs: 1_000)
    }

    func testAClientStartedWithNoServerKeepsWhatItSends() async throws {
        // Nobody answers on port 9: it starts at once all the same.
        let client = try await WseClient.start(WseConfig(url: "ws://127.0.0.1:9/wse", insecure: true))
        XCTAssertNotEqual(client.status(), .connected)
        try client.trySend(kind: "echo", json: #"{"n":1}"#)
        XCTAssertEqual(client.kept(), 1)
        await client.disconnect(deadlineMs: 0)
    }

    func testAStartedClientGetsInOnItsOwn() async throws {
        let client = try await WseClient.start(WseConfig(url: url, insecure: true))
        try await client.send(kind: "echo", json: #"{"n":3}"#)
        let echoed = try await message(client, "echoed")
        XCTAssertEqual(echoed.json, #"{"n":3}"#)
        await client.disconnect(deadlineMs: 1_000)
    }

    func testWhatIsSentWithNoConnectionGoesOnTheNext() async throws {
        let path = FileManager.default.temporaryDirectory
            .appendingPathComponent("wse-offline-\(UUID().uuidString)").path
        let client = try await WseClient.connect(
            WseConfig(url: url, insecure: true, offline: WseOffline(path: path)))
        let events = client.events()
        try client.pause(reason: "test")
        while client.status() == .connected { try await Task.sleep(nanoseconds: 20_000_000) }
        try await client.send(kind: "echo", json: #"{"n":1}"#)
        XCTAssertEqual(client.kept(), 1)
        try client.resume(reason: "test")
        var echoed: String?
        for try await item in events {
            if case let .message(message) = item, message.kind == "echoed" {
                echoed = message.json
                break
            }
        }
        XCTAssertEqual(echoed, #"{"n":1}"#)
        await client.disconnect(deadlineMs: 1_000)
        try? FileManager.default.removeItem(atPath: path)
        try? FileManager.default.removeItem(atPath: path + ".lock")
    }

    func testAMessageTheServerTookIsSaidDeliveredByItsId() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        let events = client.events()
        let id = try await client.sendWith(
            kind: "echo", json: #"{"n":2}"#, options: WseSendOptions(id: "mine-1"))
        XCTAssertEqual(id, "mine-1")
        var delivered = false
        for try await item in events {
            if case let .delivered(ids) = item, ids.contains("mine-1") {
                delivered = true
                break
            }
        }
        XCTAssertTrue(delivered)
        XCTAssertEqual(client.kept(), 0)
        try client.clear()
        await client.disconnect(deadlineMs: 1_000)
    }

    func testARequestNamesItsCorrelationIdAndTheReplyNamesItBack() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        let events = client.events()
        _ = try await client.sendWith(
            kind: "echo", json: #"{"n":5}"#, options: WseSendOptions(correlationId: "order-7", priority: 8))
        _ = try await client.sendWith(
            kind: "echo", json: #"{"n":6}"#, options: WseSendOptions(correlationId: "order-8", live: true))
        var replies = Set<String>()
        for try await item in events {
            if case let .message(message) = item, message.kind == "echoed", let cid = message.correlationId {
                replies.insert(cid)
                if replies.count == 2 { break }
            }
        }
        XCTAssertEqual(replies, ["order-7", "order-8"])
        await client.disconnect(deadlineMs: 1_000)
    }

    func testAClientGivesUpAfterItsAttemptsAsSoonAsItsDelaysSay() async throws {
        let began = Date()
        let client = try await WseClient.start(
            WseConfig(
                url: "ws://127.0.0.1:9/wse", reconnectBaseDelayMs: 20, reconnectMaxDelayMs: 40,
                reconnectJitter: false, reconnectMaxAttempts: 3, insecure: true))
        var reason: String?
        for try await item in client.events() {
            if case let .closed(why) = item {
                reason = why
                break
            }
        }
        XCTAssertTrue(reason?.hasPrefix("gave up reconnecting") == true, reason ?? "no reason")
        // Three attempts at 20 and 40 ms, not at a second and more.
        XCTAssertLessThan(Date().timeIntervalSince(began), 3)
    }

    func testALoopLeftLeavesTheRestToTheNext() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        try await client.send(kind: "echo", json: #"{"n":1}"#)
        try await client.send(kind: "echo", json: #"{"n":2}"#)
        // Both answers in one batch; each loop takes one and lets its
        // sequence go.
        try await Task.sleep(nanoseconds: 300_000_000)
        let first = try await message(client, "echoed")
        XCTAssertEqual(first.json, #"{"n":1}"#)
        let second = try await message(client, "echoed")
        XCTAssertEqual(second.json, #"{"n":2}"#)
        await client.disconnect(deadlineMs: 1_000)
    }

    func testEveryCallReadsOneStreamOneReaderAtATime() async throws {
        // No topics: past its first items the server stays quiet.
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        let events = client.events()
        XCTAssertTrue(events === client.events())
        let waiting = Task { while try await events.next() != nil {} }
        try await Task.sleep(nanoseconds: 300_000_000)
        do {
            _ = try await client.events().next()
            XCTFail("a second reader read at the same time")
        } catch WseError.Busy {}
        waiting.cancel()
        _ = try? await waiting.value
        try await client.send(kind: "echo", json: #"{"text":"after"}"#)
        var echoed: String?
        while echoed == nil, let item = try await events.next() {
            if case let .message(message) = item, message.kind == "echoed" { echoed = message.json }
        }
        XCTAssertEqual(echoed, #"{"text":"after"}"#)
        await client.disconnect(deadlineMs: 1_000)
    }

    func testTheNetworkPausesTheClientAndRenewsItsConnection() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        let monitor = WseNetworkMonitor(client: client, settle: 0, observing: false)
        monitor.update(usable: true, interfaces: ["en0"])
        monitor.update(usable: false, interfaces: [])
        try await waitFor { client.status() == .paused }
        monitor.update(usable: true, interfaces: ["en0"])
        try await waitFor { client.status() == .connected }
        // Over another interface: the connection is made again.
        monitor.update(usable: true, interfaces: ["pdp_ip0"])
        var renewed = false
        for try await item in client.events() {
            if case let .disconnected(_, reason, _, _) = item, reason == "reconnect asked" {
                renewed = true
                break
            }
        }
        XCTAssertTrue(renewed)
        await client.disconnect(deadlineMs: 1_000)
    }

    #if canImport(UIKit)
    func testTheAppInTheBackgroundPausesTheClient() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        let lifecycle = WseAppLifecycle(client: client, grace: 0)
        NotificationCenter.default.post(name: UIApplication.didEnterBackgroundNotification, object: nil)
        try await waitFor { client.status() == .paused }
        NotificationCenter.default.post(name: UIApplication.willEnterForegroundNotification, object: nil)
        try await waitFor { client.status() == .connected }
        withExtendedLifetime(lifecycle) {}
        await client.disconnect(deadlineMs: 1_000)
    }
    #endif

    /// Waits up to five seconds for `done`.
    private func waitFor(_ done: () -> Bool) async throws {
        for _ in 0..<250 where !done() {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        XCTAssertTrue(done())
    }

    func testAPayloadMustBeJson() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, insecure: true))
        XCTAssertThrowsError(try client.trySend(kind: "x", json: "not json"))
        await client.disconnect(deadlineMs: 1_000)
    }
}
