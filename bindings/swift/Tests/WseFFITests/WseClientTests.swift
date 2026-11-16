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

    func testACancelledSendDoesNotGoOut() async throws {
        let client = try await WseClient.connect(WseConfig(url: url))
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

    func testEveryCallReadsOneStreamOneReaderAtATime() async throws {
        // No topics: past its first items the server stays quiet.
        let client = try await WseClient.connect(WseConfig(url: url))
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

    func testAPayloadMustBeJson() async throws {
        let client = try await WseClient.connect(WseConfig(url: url))
        XCTAssertThrowsError(try client.trySend(kind: "x", json: "not json"))
        await client.disconnect(deadlineMs: 1_000)
    }
}
