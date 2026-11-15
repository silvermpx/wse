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

    /// What the server sends, in order, until the client closes. One reader
    /// at a time: a second gets `WseError.Busy`.
    func events(maxItems: UInt32 = 256, maxBytes: UInt32 = 1 << 20) -> WseEvents {
        WseEvents(client: self, maxItems: maxItems, maxBytes: maxBytes)
    }
}

/// The events of a client. A batch is acknowledged only once all of it was
/// handed out, so a Task cancelled in between gets the rest from the same
/// sequence later, and nothing is handed out twice.
public final class WseEvents: AsyncSequence, @unchecked Sendable {
    public typealias Element = WseItem

    private let client: WseClient
    private let maxItems: UInt32
    private let maxBytes: UInt32
    private var batch: WseBatch?
    private var index = 0
    private var acknowledged: UInt64 = 0
    private var ended = false

    init(client: WseClient, maxItems: UInt32, maxBytes: UInt32) {
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
