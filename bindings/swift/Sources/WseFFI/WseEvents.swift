// The Swift side of the client: calls a cancelled Task stops, and the events
// as an AsyncSequence that reads a batch when the last one is used up.

import Foundation

public extension WseClient {
    /// Connects, and returns once the server took the connection. Cancelling
    /// the Task gives up the attempt.
    static func connect(_ config: WseConfig, tokens: WseTokenSource? = nil) async throws -> WseClient {
        let cancel = WseCancel()
        return try await withTaskCancellationHandler {
            try await WseFFI.connect(config: config, tokens: tokens, cancel: cancel)
        } onCancel: {
            cancel.cancel()
        }
    }

    /// What the server sends, in order, until the client closes. Every call
    /// reads the client's one stream (with the limits of the first, while it
    /// is held): an item goes to one reader, and a second reading at the
    /// same time gets `WseError.Busy`.
    func events(maxItems: UInt32 = 256, maxBytes: UInt32 = 1 << 20) -> WseEvents {
        WseEvents.of(self, maxItems: maxItems, maxBytes: maxBytes)
    }

    /// The consumer applied `message` (one of a topic): the checkpoint moves
    /// on through what was applied without a hole. A message handed out
    /// before the events showed another user counts for nobody.
    func commit(_ message: WseMessage) {
        guard let topic = message.topic, let epoch = message.epoch, let offset = message.offset else { return }
        commit(topic: topic, epoch: epoch, offset: offset, generation: message.generation)
    }

    /// Sends `{"t": kind, "p": json}`, waiting for room in the queue.
    /// Cancelling the Task gives up the wait; a message already queued still
    /// goes.
    func send(kind: String, json: String) async throws {
        try await cancellable { try await self.send(kind: kind, json: json, cancel: $0) }
    }

    /// Sends on the connection that is ready now, or not at all, waiting for
    /// room in the queue. Cancelling the Task gives up the wait.
    func sendLive(kind: String, json: String) async throws {
        try await cancellable { try await self.sendLive(kind: kind, json: json, cancel: $0) }
    }

    /// Sends a signed message on this connection, or not at all; the nonce it
    /// went with. Cancelling the Task gives up the wait.
    func sendSigned(kind: String, json: String, nonce: String? = nil) async throws -> String {
        try await cancellable { try await self.sendSigned(kind: kind, json: json, nonce: nonce, cancel: $0) }
    }
}

/// Runs `call` with a token the Task's cancellation cancels.
private func cancellable<T>(_ call: (WseCancel) async throws -> T) async throws -> T {
    let cancel = WseCancel()
    return try await withTaskCancellationHandler {
        try await call(cancel)
    } onCancel: {
        cancel.cancel()
    }
}

/// The events of a client. A batch is acknowledged only once all of it was
/// handed out, so a Task cancelled in between gets the rest from the same
/// sequence later, and nothing is handed out twice.
public final class WseEvents: AsyncSequence, @unchecked Sendable {
    public typealias Element = WseItem

    /// Each client's reader while one is held.
    private static let readers = NSMapTable<WseClient, WseEvents>.weakToWeakObjects()
    private static let readersLock = NSLock()

    static func of(_ client: WseClient, maxItems: UInt32, maxBytes: UInt32) -> WseEvents {
        readersLock.lock()
        defer { readersLock.unlock() }
        if let reader = readers.object(forKey: client) { return reader }
        let reader = WseEvents(client: client, maxItems: maxItems, maxBytes: maxBytes)
        readers.setObject(reader, forKey: client)
        return reader
    }

    private let client: WseClient
    private let maxItems: UInt32
    private let maxBytes: UInt32
    private var batch: WseBatch?
    private var index = 0
    private var acknowledged: UInt64 = 0
    private var ended = false
    private let lock = NSLock()
    private var reading = false

    private init(client: WseClient, maxItems: UInt32, maxBytes: UInt32) {
        self.client = client
        self.maxItems = maxItems
        self.maxBytes = maxBytes
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        let events: WseEvents

        public mutating func next() async throws -> WseItem? {
            try await events.next()
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(events: self)
    }

    func next() async throws -> WseItem? {
        lock.lock()
        if reading {
            lock.unlock()
            throw WseError.Busy
        }
        reading = true
        lock.unlock()
        defer {
            lock.lock()
            reading = false
            lock.unlock()
        }
        while true {
            if let batch, index < batch.items.count {
                index += 1
                return batch.items[index - 1]
            }
            if ended { return nil }
            if let batch { acknowledged = batch.id }
            let cancel = WseCancel()
            let next = try await withTaskCancellationHandler {
                try await client.nextBatch(ack: acknowledged, maxItems: maxItems, maxBytes: maxBytes, cancel: cancel)
            } onCancel: {
                cancel.cancel()
            }
            if case .end = next.kind {
                ended = true
                batch = nil
                return nil
            }
            batch = next
            index = 0
        }
    }
}
