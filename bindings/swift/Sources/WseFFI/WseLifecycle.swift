// What the device does to a client: the app going to the background, and
// the network coming, going or changing. Both pause and resume the client
// for reasons of their own, so the application's own pauses stay.

import Foundation
import Network
#if canImport(UIKit)
import UIKit
#endif

#if canImport(UIKit)
/// Pauses a client `grace` seconds after the app went to the background, and
/// resumes it when the app comes back. App-wide: a scene closing does not
/// pause it while another is on screen.
public final class WseAppLifecycle {
    private let client: WseClient
    private let grace: TimeInterval
    private var pending: DispatchWorkItem?
    private var observers: [NSObjectProtocol] = []

    public init(client: WseClient, grace: TimeInterval = 5) {
        self.client = client
        self.grace = grace
        let center = NotificationCenter.default
        observers.append(center.addObserver(
            forName: UIApplication.didEnterBackgroundNotification, object: nil, queue: .main
        ) { [weak self] _ in self?.wentToBackground() })
        observers.append(center.addObserver(
            forName: UIApplication.willEnterForegroundNotification, object: nil, queue: .main
        ) { [weak self] _ in self?.cameBack() })
    }

    deinit {
        observers.forEach(NotificationCenter.default.removeObserver)
        pending?.cancel()
    }

    private func wentToBackground() {
        pending?.cancel()
        let pause = DispatchWorkItem { [client] in try? client.pause(reason: "background") }
        pending = pause
        DispatchQueue.main.asyncAfter(deadline: .now() + grace, execute: pause)
    }

    private func cameBack() {
        pending?.cancel()
        pending = nil
        try? client.resume(reason: "background")
    }
}
#endif

/// Pauses a client while the device has no network path, and makes its
/// connection again when the path moves to other interfaces (Wi-Fi to
/// cellular, say): the old connection may hang on an interface that is
/// gone. Changes within `settle` seconds of each other count once. A LAN
/// server needs no Internet: any usable path counts.
public final class WseNetworkMonitor {
    private let client: WseClient
    private let settle: TimeInterval
    private let monitor = NWPathMonitor()
    private let queue = DispatchQueue(label: "dev.wse.network")
    private var interfaces: [String]?
    private var pending: DispatchWorkItem?

    public convenience init(client: WseClient, settle: TimeInterval = 1) {
        self.init(client: client, settle: settle, observing: true)
    }

    /// `observing` false: the paths come from `update` alone (tests).
    init(client: WseClient, settle: TimeInterval, observing: Bool) {
        self.client = client
        self.settle = settle
        guard observing else { return }
        monitor.pathUpdateHandler = { [weak self] path in
            self?.update(usable: path.status == .satisfied, interfaces: path.availableInterfaces.map(\.name))
        }
        monitor.start(queue: queue)
    }

    deinit {
        monitor.cancel()
        pending?.cancel()
    }

    /// One path update: usable or not, and the interfaces it goes over.
    func update(usable: Bool, interfaces now: [String]) {
        guard usable else {
            pending?.cancel()
            pending = nil
            interfaces = nil
            try? client.pause(reason: "offline")
            return
        }
        try? client.resume(reason: "offline")
        defer { interfaces = now }
        guard let before = interfaces, before != now else { return }
        pending?.cancel()
        let reconnect = DispatchWorkItem { [client] in try? client.reconnect() }
        pending = reconnect
        queue.asyncAfter(deadline: .now() + settle, execute: reconnect)
    }
}
