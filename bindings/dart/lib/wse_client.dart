/// The WSE client for Dart and Flutter, on the Rust client.
library;

import 'dart:async';
import 'dart:io';

import 'package:flutter/foundation.dart' show listEquals;
import 'package:flutter/widgets.dart';
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart'
    show ExternalLibrary;

import 'src/rust/api.dart' as rust;
import 'src/rust/frb_generated.dart';

export 'src/rust/api.dart'
    show WseConfig, WseError, WseItem, WseItemKind, WseTopicRecovery;

/// Under `flutter test` the build hook puts the library in
/// build/native_assets/<os>/, where loading it by name does not look; a
/// directory named in FRB_DART_LOAD_EXTERNAL_LIBRARY_NATIVE_LIB_DIR wins.
ExternalLibrary? _testLibrary() {
  final env = Platform.environment;
  if (!env.containsKey('FLUTTER_TEST') ||
      env.containsKey('FRB_DART_LOAD_EXTERNAL_LIBRARY_NATIVE_LIB_DIR')) {
    return null;
  }
  final file = switch (Platform.operatingSystem) {
    'macos' => File('build/native_assets/macos/libwse_dart.dylib'),
    'linux' => File('build/native_assets/linux/libwse_dart.so'),
    'windows' => File('build/native_assets/windows/wse_dart.dll'),
    _ => null,
  };
  return file != null && file.existsSync()
      ? ExternalLibrary.open(file.absolute.path)
      : null;
}

/// A connection to a WSE server, made again when it is lost.
class WseClient {
  WseClient._(this._native);

  final rust.WseNativeClient _native;
  BigInt _acknowledged = BigInt.zero;
  rust.WseBatch? _batch;
  int _index = 0;
  bool _ended = false;
  bool _closed = false;
  Future<void>? _closing;

  static const _closedError = rust.WseError(
    kind: 'closed',
    message: 'the client is closed',
  );

  /// The native client while this one is open.
  rust.WseNativeClient get _open => _closed ? throw _closedError : _native;

  /// Loads the native library; call once before anything else.
  static Future<void> init() => RustLib.init(externalLibrary: _testLibrary());

  /// Connects, and completes once the server took the connection.
  static Future<WseClient> connect(rust.WseConfig config) async => WseClient._(
    await rust.WseNativeClient.connect(
      config: config,
      cancel: rust.WseCancel(),
    ),
  );

  /// What the server sends, in order, until the client closes. A batch is
  /// acknowledged only once all of it was handed out, so a listener that
  /// stops in between gets the rest from the next one; one reader at a time.
  /// Cancelling a listener that waits for the server ends the wait.
  Stream<rust.WseItem> events({int maxItems = 256, int maxBytes = 1 << 20}) {
    late final StreamController<rust.WseItem> controller;
    Future<void>? pumping;
    rust.WseCancel? waiting;
    Completer<void>? resumed;
    var cancelled = false;

    Future<void> pump() async {
      try {
        while (!cancelled) {
          final batch = _batch;
          if (batch != null && _index < batch.items.length) {
            if (controller.isPaused) {
              await (resumed = Completer<void>()).future;
              continue;
            }
            // Handed out: the listener has it once add returns.
            final item = batch.items[_index++];
            controller.add(item);
            continue;
          }
          if (_ended) break;
          if (batch != null) _acknowledged = batch.id;
          final cancel = waiting = rust.WseCancel();
          final rust.WseBatch next;
          try {
            next = await _native.nextBatch(
              ack: _acknowledged,
              maxItems: maxItems,
              maxBytes: maxBytes,
              cancel: cancel,
            );
          } finally {
            waiting = null;
          }
          if (next.end) {
            _ended = true;
            _batch = null;
            break;
          }
          _batch = next;
          _index = 0;
        }
      } catch (error, stack) {
        if (!cancelled) controller.addError(error, stack);
      }
      if (!cancelled) await controller.close();
    }

    void wake() {
      resumed?.complete();
      resumed = null;
    }

    controller = StreamController<rust.WseItem>(
      sync: true,
      onListen: () => pumping = Future<void>.microtask(pump),
      onResume: wake,
      onCancel: () {
        cancelled = true;
        waiting?.cancel();
        wake();
        return pumping;
      },
    );
    return controller.stream;
  }

  /// Sends `{"t": kind, "p": json}`, waiting for room in the queue.
  Future<void> send(String kind, String json) => _closed
      ? Future.error(_closedError)
      : _native.send(kind: kind, json: json);

  /// Sends at once; throws when the queue is full.
  void trySend(String kind, String json) =>
      _open.trySend(kind: kind, json: json);

  /// Sends on the connection that is ready now, or not at all: never on a
  /// later one, for commands that must not run late. Waits for room in the
  /// queue; throws without a ready connection.
  Future<void> sendLive(String kind, String json) => _closed
      ? Future.error(_closedError)
      : _native.sendLive(kind: kind, json: json);

  /// [sendLive] at once; throws when the queue is full.
  void trySendLive(String kind, String json) =>
      _open.trySendLive(kind: kind, json: json);

  /// Sends a signed message on this connection, or not at all; the nonce it went with.
  Future<String> sendSigned(String kind, String json, {String? nonce}) =>
      _closed
      ? Future.error(_closedError)
      : _native.sendSigned(kind: kind, json: json, nonce: nonce);

  void subscribe(List<String> topics) => _open.subscribe(topics: topics);
  void unsubscribe(List<String> topics) => _open.unsubscribe(topics: topics);
  void reconnect() => _open.reconnect();

  /// Ends the connection for [reason] ('background', 'offline', ...) and
  /// makes none while any reason holds: topics and positions stay.
  void pause(String reason) => _open.pause(reason: reason);

  /// Clears [reason]; once none is left the client connects again at once
  /// and recovers what it missed.
  void resume(String reason) => _open.resume(reason: reason);

  /// connecting, connected, reconnecting, paused or closed.
  String get status => _closed ? 'closed' : _native.status();
  String? get userId => _closed ? null : _native.userId();

  /// Closes the client: what was sent goes out first, within [deadline].
  /// Afterwards its calls throw a 'closed' [WseError]; closing again waits
  /// for the first close.
  Future<void> close({Duration deadline = const Duration(seconds: 2)}) =>
      _closing ??= _close(deadline);

  Future<void> _close(Duration deadline) async {
    await _native.disconnect(deadlineMs: BigInt.from(deadline.inMilliseconds));
    // The events end after what was read already.
    _ended = true;
    _closed = true;
    _native.dispose();
  }
}

/// A configuration with the defaults for what is not given.
rust.WseConfig wseConfig(
  String url, {
  String? token,
  List<String> topics = const [],
  bool msgpack = false,
  Map<String, String> headers = const {},
  bool reconnect = true,
}) => rust.WseConfig(
  url: url,
  token: token,
  topics: topics,
  msgpack: msgpack,
  headerNames: headers.keys.toList(),
  headerValues: headers.values.toList(),
  sendQueue: 0,
  reconnect: reconnect,
  connectTimeoutMs: BigInt.zero,
  recoveryTimeoutMs: BigInt.zero,
  serverIdentityKeys: const [],
  signingKey: null,
  serverSigningKeys: const [],
);

/// Pauses [client] [grace] after the app went to the background, and
/// resumes it when the app comes back. It pauses for a reason of its own, so
/// a pause of the application's stays. Dispose of it with the client.
class WseAppLifecycle with WidgetsBindingObserver {
  WseAppLifecycle(this.client, {this.grace = const Duration(seconds: 5)}) {
    WidgetsBinding.instance.addObserver(this);
  }

  final WseClient client;
  final Duration grace;
  Timer? _pending;

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    switch (state) {
      case AppLifecycleState.resumed:
        _pending?.cancel();
        _pending = null;
        _control(() => client.resume('background'));
      case AppLifecycleState.hidden:
      case AppLifecycleState.paused:
        _pending ??= Timer(
          grace,
          () => _control(() => client.pause('background')),
        );
      case AppLifecycleState.inactive:
      case AppLifecycleState.detached:
        break;
    }
  }

  void dispose() {
    _pending?.cancel();
    WidgetsBinding.instance.removeObserver(this);
  }
}

/// Pauses [client] while the device has no usable network, and makes its
/// connection again when the network moves to other interfaces (Wi-Fi to
/// mobile, say): the old connection may hang on an interface that is gone.
/// Changes within [settle] of each other count once. It pauses for a reason
/// of its own, so a pause of the application's stays. A LAN server needs no
/// Internet: any usable network counts. The app feeds it what it sees, with
/// connectivity_plus for one:
///
/// ```dart
/// final network = WseNetworkMonitor(client);
/// Connectivity().onConnectivityChanged.listen(
///   (results) => network.update(
///     usable: !results.contains(ConnectivityResult.none),
///     interfaces: [for (final result in results) result.name],
///   ),
/// );
/// ```
class WseNetworkMonitor {
  WseNetworkMonitor(this.client, {this.settle = const Duration(seconds: 1)});

  final WseClient client;
  final Duration settle;
  List<String>? _interfaces;
  Timer? _pending;

  /// One network change: usable or not, and the interfaces it goes over.
  void update({required bool usable, required List<String> interfaces}) {
    if (!usable) {
      _pending?.cancel();
      _pending = null;
      _interfaces = null;
      _control(() => client.pause('offline'));
      return;
    }
    _control(() => client.resume('offline'));
    final before = _interfaces;
    _interfaces = List.unmodifiable(interfaces);
    if (before == null || listEquals(before, interfaces)) return;
    _pending?.cancel();
    _pending = Timer(settle, () {
      _pending = null;
      _control(client.reconnect);
    });
  }

  void dispose() {
    _pending?.cancel();
    _pending = null;
  }
}

/// Runs a control call; a client closed already has nothing to control.
void _control(void Function() call) {
  try {
    call();
  } on rust.WseError catch (error) {
    if (error.kind != 'closed') rethrow;
  }
}
