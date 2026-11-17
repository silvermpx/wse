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
    /// reads the client's one stream (with the limits of the first): an item
    /// goes to one reader, a loop left and a new one go on from the same
    /// place, and a second reading at the same time gets `WseError.Busy`.
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

    /// Sends `{"t": kind, "p": json}`, waiting for room. It waits in the
    /// outbox until the server answers for it (`.delivered` and
    /// `.notDelivered` say what became of it) and goes again after a
    /// reconnect. Cancelling the Task gives up the wait; a message already
    /// queued still goes.
    func send(kind: String, json: String) async throws {
        try await cancellable { try await self.send(kind: kind, json: json, cancel: $0) }
    }

    /// `send` with `options`: its own id (the app's, say; a random UUID
    /// without one), how long it may wait to be written first, the topics it
    /// requires. Its id, which its outcome names.
    func sendWith(kind: String, json: String, options: WseSendOptions = WseSendOptions()) async throws -> String {
        try await cancellable { try await self.sendWith(kind: kind, json: json, options: options, cancel: $0) }
    }

    /// Sends once each of `topics` is continuous on the connection (see
    /// `topicStates()`), waiting for room in the queue; until then it waits,
    /// in order with the others that wait. Cancelling the Task gives up the
    /// wait for room; a message already queued still goes.
    func sendRequiring(kind: String, json: String, topics: [String]) async throws {
        try await cancellable { try await self.sendRequiring(kind: kind, json: json, topics: topics, cancel: $0) }
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

/// Where the reading of a client's events stands, for as long as the client
/// lives: the sequences made of it one after another go on from one place.
final class WseReadState: @unchecked Sendable {
    let maxItems: UInt32
    let maxBytes: UInt32
    var batch: WseBatch?
    var index = 0
    var acknowledged: UInt64 = 0
    var ended = false
    private let lock = NSLock()
    private var reading = false

    init(maxItems: UInt32, maxBytes: UInt32) {
        self.maxItems = maxItems
        self.maxBytes = maxBytes
    }

    /// The reading, for one caller at a time: false while another reads.
    func claim() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if reading { return false }
        reading = true
        return true
    }

    func release() {
        lock.lock()
        defer { lock.unlock() }
        reading = false
    }
}

/// What a client's read state hangs on: it goes with the client.
nonisolated(unsafe) private var readStateKey: UInt8 = 0

/// The events of a client. A batch is acknowledged only once all of it was
/// handed out, so a Task cancelled in between, or a loop left, gets the rest
/// later, from this sequence or a new one, and nothing is handed out twice.
public final class WseEvents: AsyncSequence, @unchecked Sendable {
    public typealias Element = WseItem

    /// Each client's reader while one is held.
    private static let readers = NSMapTable<WseClient, WseEvents>.weakToWeakObjects()
    private static let readersLock = NSLock()

    static func of(_ client: WseClient, maxItems: UInt32, maxBytes: UInt32) -> WseEvents {
        readersLock.lock()
        defer { readersLock.unlock() }
        if let reader = readers.object(forKey: client) { return reader }
        let state: WseReadState
        if let held = objc_getAssociatedObject(client, &readStateKey) as? WseReadState {
            state = held
        } else {
            state = WseReadState(maxItems: maxItems, maxBytes: maxBytes)
            objc_setAssociatedObject(client, &readStateKey, state, .OBJC_ASSOCIATION_RETAIN)
        }
        let reader = WseEvents(client: client, state: state)
        readers.setObject(reader, forKey: client)
        return reader
    }

    private let client: WseClient
    private let state: WseReadState

    private init(client: WseClient, state: WseReadState) {
        self.client = client
        self.state = state
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
        guard state.claim() else { throw WseError.Busy }
        defer { state.release() }
        while true {
            if let batch = state.batch, state.index < batch.items.count {
                state.index += 1
                return batch.items[state.index - 1]
            }
            if state.ended { return nil }
            if let batch = state.batch { state.acknowledged = batch.id }
            let cancel = WseCancel()
            let (ack, maxItems, maxBytes) = (state.acknowledged, state.maxItems, state.maxBytes)
            let next = try await withTaskCancellationHandler {
                try await client.nextBatch(ack: ack, maxItems: maxItems, maxBytes: maxBytes, cancel: cancel)
            } onCancel: {
                cancel.cancel()
            }
            if case .end = next.kind {
                state.ended = true
                state.batch = nil
                return nil
            }
            state.batch = next
            state.index = 0
        }
    }
}
