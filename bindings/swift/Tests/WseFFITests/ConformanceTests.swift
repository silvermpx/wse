// The Swift package's driver for the conformance server
// (crates/wse-conformance): it runs each case it can, step by step, and
// records what its client saw and did; the server judges. Runs when
// WSE_CONFORMANCE holds the server's control URL (run-tests.sh starts it).
import Foundation
import XCTest

@testable import WseFFI

/// What the Swift package has, by the catalog's names.
private let features = ["outbox", "start", "live", "msgpack", "e2e", "signing", "recover_or_resync"]

private typealias Object = [String: Any]

/// The control channel: requests and their answers, by id.
private final class Control {
    private let socket: URLSessionWebSocketTask
    private var next = 0

    init(_ url: URL) {
        socket = URLSession.shared.webSocketTask(with: url)
        socket.resume()
    }

    func call(_ op: String, _ body: Object = [:]) async throws -> Object {
        next += 1
        let id = next
        var request = body
        request["id"] = id
        request["op"] = op
        let text = String(decoding: try JSONSerialization.data(withJSONObject: request), as: UTF8.self)
        try await socket.send(.string(text))
        while true {
            guard case let .string(reply) = try await socket.receive(),
                let answer = try JSONSerialization.jsonObject(with: Data(reply.utf8)) as? Object,
                answer["id"] as? Int == id
            else { continue }
            guard answer["ok"] as? Bool == true else {
                throw ConformanceFailure.control("\(op): \(answer["error"] ?? "")")
            }
            return answer
        }
    }

    func close() {
        socket.cancel(with: .normalClosure, reason: nil)
    }
}

private enum ConformanceFailure: Error {
    case control(String)
    case step(String)
}

/// What a client's events reader took, for the case to take in turn.
private final class Inbox: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [WseItem] = []

    func add(_ item: WseItem) {
        lock.lock()
        items.append(item)
        lock.unlock()
    }

    func takeAll() -> [WseItem] {
        lock.lock()
        defer { lock.unlock() }
        let taken = items
        items.removeAll()
        return taken
    }
}

/// One case: the client, what it saw, and the steps' doings.
private final class Run {
    let begun: Object
    var trace: [Object] = []
    private var client: WseClient?
    private var reader: Task<Void, Never>?
    private let inbox = Inbox()
    private var ready = false
    private var rr = false
    private var published: [String: [Int]] = [:]
    /// Resync rounds waiting for a step to say the application resynced.
    private var resyncs: [(UInt64, [String])] = []
    private var key: Data?
    private var bound = false
    private let began = Date()

    init(_ begun: Object) {
        self.begun = begun
    }

    private func record(_ event: Object) {
        var event = event
        event["t"] = Int(Date().timeIntervalSince(began) * 1_000)
        trace.append(event)
    }

    func go(_ control: Control) async {
        let steps = begun["steps"] as? [Object] ?? []
        for (at, step) in steps.enumerated() {
            drain()
            if let server = step["server"] {
                record(["ev": "step", "server": server])
                do {
                    let done = try await control.call("act", ["step": step])
                    if step["server"] as? String == "publish", let topic = done["topic"] as? String {
                        published[topic, default: []] += done["n"] as? [Int] ?? []
                    }
                } catch {
                    record(["ev": "error", "message": "step \(at): \(error)"])
                    break
                }
                continue
            }
            do {
                if try await !clientStep(step) {
                    record(["ev": "timeout", "step": at, "waiting": step])
                    break
                }
            } catch {
                record(["ev": "error", "message": "step \(at): \(error)"])
                break
            }
        }
        await close()
    }

    private func open() throws -> WseClient {
        guard let client else { throw ConformanceFailure.step("no client") }
        return client
    }

    /// A step of the client's: false when what it waits for did not come.
    private func clientStep(_ step: Object) async throws -> Bool {
        switch step["client"] as? String {
        case "connect":
            try await connect(step)
        case "wait":
            return await until(step, ms: step["timeout_ms"] as? Int ?? 10_000)
        case "send":
            try await send(step)
        case "pause":
            try open().pause(reason: "test")
        case "resume":
            try open().resume(reason: "test")
        case "resynced":
            let topics = step["topics"] as? [String] ?? []
            record(["ev": "resynced", "topics": topics])
            let done = resyncs.filter { $0.1.contains(where: topics.contains) }
            resyncs.removeAll { $0.1.contains(where: topics.contains) }
            for (round, _) in done { try open().resynced(round: round) }
        case "sleep":
            let end = Date().addingTimeInterval(Double(step["ms"] as? Int ?? 0) / 1_000)
            while Date() < end { await pump() }
        case "close":
            await close()
        default:
            throw ConformanceFailure.step("no such client step: \(step["client"] ?? "")")
        }
        return true
    }

    private func connect(_ step: Object) async throws {
        // A client made again: the last one goes, as a dropped handle's does.
        if client != nil { await close() }
        let user = step["user"] as? String ?? "ann"
        rr = step["recover_or_resync"] as? Bool == true
        let signing = step["signing"] as? Bool == true
        key = signing ? generateSigningKey() : nil
        let tokens = begun["tokens"] as? Object ?? [:]
        let config = WseConfig(
            url: begun["url"] as? String ?? "",
            token: tokens[user] as? String,
            topics: step["topics"] as? [String] ?? [],
            msgpack: step["format"] as? String == "msgpack",
            reconnectBaseDelayMs: 50,
            reconnectMaxDelayMs: 300,
            reconnectJitter: false,
            serverIdentityKeys: step["e2e"] as? Bool == true ? [begun["identity_key"] as? String ?? ""] : [],
            signingKey: key,
            serverSigningKeys: signing ? [begun["signing_key"] as? String ?? ""] : [],
            insecure: true,
            delivery: rr ? WseDelivery(consumer: "conformance") : nil
        )
        let made =
            step["start"] as? Bool == true ? try await WseClient.start(config) : try await WseClient.connect(config)
        client = made
        let events = made.events()
        let inbox = inbox
        reader = Task {
            do {
                for try await item in events { inbox.add(item) }
            } catch {}
        }
    }

    private func send(_ step: Object) async throws {
        let kind = step["type"] as? String ?? ""
        let count = step["count"] as? Int ?? 1
        for k in 1...count {
            let id = step["id"] as? String ?? "\(kind)-\(k)"
            let payload = String(
                decoding: try JSONSerialization.data(withJSONObject: step["payload"] ?? ["n": k], options: [.fragmentsAllowed]),
                as: UTF8.self)
            if step["signed"] as? Bool == true {
                if !bound {
                    var ok = true
                    do {
                        let publicKey = try signingPublicKey(key: key ?? Data())
                        _ = try await open().sendWith(
                            kind: "bind_key", json: #"{"public_key":"\#(publicKey)"}"#,
                            options: WseSendOptions(id: "bind-key"))
                    } catch {
                        ok = false
                    }
                    record(["ev": "sent", "id": "bind-key", "type": "bind_key", "live": false, "ok": ok])
                    guard ok, await until(["for": "message", "type": "key_bound"], ms: 10_000) else {
                        throw ConformanceFailure.step("the server never bound the key")
                    }
                    bound = true
                }
                var ok = true
                do { _ = try await open().sendSigned(kind: kind, json: payload, nonce: id) } catch { ok = false }
                record(["ev": "sent", "id": id, "type": kind, "signed": true, "live": false, "ok": ok])
                continue
            }
            let live = step["live"] as? Bool == true
            let options = WseSendOptions(
                id: id,
                correlationId: step["correlation_id"] as? String,
                priority: UInt8(step["priority"] as? Int ?? 0),
                live: live
            )
            var ok = true
            do {
                if live {
                    _ = try open().trySendWith(kind: kind, json: payload, options: options)
                } else {
                    _ = try await open().sendWith(kind: kind, json: payload, options: options)
                }
            } catch {
                ok = false
            }
            record(["ev": "sent", "id": id, "type": kind, "live": live, "ok": ok])
        }
    }

    /// Takes what the client has for the application now.
    private func drain() {
        for item in inbox.takeAll() { take(item) }
    }

    /// Lets what comes arrive, then takes it.
    private func pump() async {
        try? await Task.sleep(nanoseconds: 10_000_000)
        drain()
    }

    private func until(_ step: Object, ms: Int) async -> Bool {
        let end = Date().addingTimeInterval(Double(ms) / 1_000)
        while true {
            drain()
            if holds(step) { return true }
            if Date() >= end { return false }
            await pump()
        }
    }

    private func take(_ item: WseItem) {
        switch item {
        case let .message(message):
            let payload = message.json.flatMap {
                try? JSONSerialization.jsonObject(with: Data($0.utf8), options: [.fragmentsAllowed])
            }
            let n = (payload as? Object)?["n"] as? Int
            record([
                "ev": "message",
                "type": message.kind,
                "topic": message.topic as Any? ?? NSNull(),
                "n": n as Any? ?? NSNull(),
                "payload": payload ?? NSNull(),
                "cid": message.correlationId as Any? ?? NSNull(),
            ])
            // Applied: the checkpoint moves on.
            if rr, message.topic != nil { client?.commit(message) }
        case .connected:
            ready = true
            record(["ev": "ready"])
        case .disconnected, .paused:
            ready = false
        case let .gap(topic, _, _, _, _):
            record(["ev": "break", "topic": topic, "kind": "gap"])
        case let .continuity(topic, continuity, reason):
            if continuity != .continuous {
                record(["ev": "break", "topic": topic, "kind": "continuity", "reason": reason as Any? ?? NSNull()])
            }
        case let .resync(round, topics, reasons, _):
            let cold = reasons.allSatisfy { $0 == "cold_start" }
            record(["ev": "resync", "topics": topics, "auto": cold, "reasons": reasons])
            // At a cold start the application has its state at once.
            if cold {
                record(["ev": "resynced", "topics": topics, "auto": true])
                client?.resynced(round: round)
            } else {
                resyncs.append((round, topics))
            }
        case let .delivered(ids):
            record(["ev": "delivered", "ids": ids])
        case let .notDelivered(reason, ids):
            record(["ev": "not_delivered", "reason": reason, "ids": ids])
        default:
            break
        }
    }

    private func holds(_ step: Object) -> Bool {
        func messages(_ pick: (Object) -> Bool) -> Int {
            trace.filter { $0["ev"] as? String == "message" && pick($0) }.count
        }
        let topic = step["topic"] as? String
        let count = step["count"] as? Int ?? 1
        switch step["for"] as? String {
        case "ready":
            return ready
        case "not_ready":
            return !ready
        case "messages":
            return messages { $0["topic"] as? String == topic } >= count
        case "message":
            return messages { $0["type"] as? String == step["type"] as? String } >= count
        case "published":
            let last = step["last"] as? Int ?? 1
            let wanted = Array((published[topic ?? ""] ?? []).suffix(last))
            return wanted.count == last
                && wanted.allSatisfy { n in
                    trace.contains {
                        $0["ev"] as? String == "message" && $0["topic"] as? String == topic && $0["n"] as? Int == n
                    }
                }
        case "break":
            return trace.contains {
                ($0["ev"] as? String == "break" && $0["topic"] as? String == topic)
                    || ($0["ev"] as? String == "resync" && ($0["topics"] as? [String] ?? []).contains(topic ?? ""))
            }
        case "resync":
            return trace.contains {
                $0["ev"] as? String == "resync" && $0["auto"] as? Bool != true
                    && ($0["topics"] as? [String] ?? []).contains(topic ?? "")
            }
        case "delivered":
            let delivered = Set(trace.filter { $0["ev"] as? String == "delivered" }.flatMap { $0["ids"] as? [String] ?? [] })
            let ids: [String]
            if step["all"] as? Bool == true {
                ids = trace.filter {
                    $0["ev"] as? String == "sent" && $0["ok"] as? Bool == true && $0["live"] as? Bool != true
                        && $0["signed"] as? Bool != true
                }.compactMap { $0["id"] as? String }
            } else {
                ids = step["ids"] as? [String] ?? []
            }
            return ids.allSatisfy(delivered.contains)
        default:
            return false
        }
    }

    func close() async {
        drain()
        if let client {
            await client.disconnect(deadlineMs: 1_000)
            // What came before the close is the case's too.
            await reader?.value
            drain()
        }
        client = nil
        reader = nil
        ready = false
    }
}

final class ConformanceTests: XCTestCase {
    func testTheSwiftPackagePassesEveryCaseItCan() async throws {
        let environment = ProcessInfo.processInfo.environment
        guard let url = environment["WSE_CONFORMANCE"].flatMap(URL.init(string:)) else {
            XCTAssertNotEqual(
                environment["WSE_CONFORMANCE_REQUIRED"], "1",
                "WSE_CONFORMANCE_REQUIRED is set, but WSE_CONFORMANCE is not")
            return
        }
        #if arch(arm64)
            let arch = "aarch64"
        #else
            let arch = "x86_64"
        #endif
        let control = Control(url)
        let hello = try await control.call(
            "hello",
            [
                "agent": "swift",
                "version": "3.2.0",
                "runtime": "swift",
                "os": ProcessInfo.processInfo.operatingSystemVersionString,
                "arch": arch,
                "features": features,
            ])
        var failed: [String] = []
        for entry in hello["cases"] as? [Object] ?? [] {
            let name = entry["case"] as? String ?? ""
            let begun = try await control.call("begin", ["case": name])
            let run = Run(begun)
            await run.go(control)
            for start in stride(from: 0, to: run.trace.count, by: 500) {
                let chunk = Array(run.trace[start..<min(start + 500, run.trace.count)])
                _ = try await control.call("trace", ["events": chunk])
            }
            let judged = try await control.call("end")
            if judged["verdict"] as? String != "pass" {
                failed.append("\(name): \(judged["failures"] ?? "")")
            }
        }
        _ = try await control.call("bye")
        control.close()
        XCTAssertEqual(failed, [])
    }
}
