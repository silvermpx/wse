// Against the Rust client's example server: run.sh starts it and passes its
// URL in WSE_TEST_URL.

import XCTest
@testable import WseFFI

final class WseClientTests: XCTestCase {
    private var url: String {
        ProcessInfo.processInfo.environment["WSE_TEST_URL"] ?? "ws://127.0.0.1:5007/wse"
    }

    func testTicksComeInOrderAndEchoAnswers() async throws {
        let client = try await WseClient.connect(WseConfig(url: url, topics: ["ticks"]))
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
        let client = try await WseClient.connect(WseConfig(url: url))
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
            try await WseClient.connect(WseConfig(url: "ws://10.255.255.1:9/wse", connectTimeoutMs: 30_000))
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
        let client = try await WseClient.connect(WseConfig(url: url))
        try client.trySendLive(kind: "valve", json: #"{"open":true}"#)
        try await client.sendLive(kind: "valve", json: #"{"open":false}"#)
        await client.disconnect(deadlineMs: 1_000)
        XCTAssertThrowsError(try client.trySendLive(kind: "valve", json: "{}"))
    }

    func testAPayloadMustBeJson() async throws {
        let client = try await WseClient.connect(WseConfig(url: url))
        XCTAssertThrowsError(try client.trySend(kind: "x", json: "not json"))
        await client.disconnect(deadlineMs: 1_000)
    }
}
