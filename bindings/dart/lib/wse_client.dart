/// The WSE client for Dart and Flutter, on the Rust client.
library;

import 'src/rust/api.dart' as rust;
import 'src/rust/frb_generated.dart';

export 'src/rust/api.dart' show WseConfig, WseError, WseItem, WseItemKind;

/// A connection to a WSE server, made again when it is lost.
class WseClient {
  WseClient._(this._native);

  final rust.WseNativeClient _native;
  BigInt _acknowledged = BigInt.zero;
  rust.WseBatch? _batch;
  int _index = 0;
  bool _ended = false;

  /// Loads the native library; call once before anything else.
  static Future<void> init() => RustLib.init();

  /// Connects, and completes once the server took the connection.
  static Future<WseClient> connect(rust.WseConfig config) async =>
      WseClient._(await rust.WseNativeClient.connect(config: config, cancel: rust.WseCancel()));

  /// What the server sends, in order, until the client closes. A batch is
  /// acknowledged only once all of it was handed out, so a listener that
  /// stops in between gets the rest from the next one; one reader at a time.
  Stream<rust.WseItem> events({int maxItems = 256, int maxBytes = 1 << 20}) async* {
    while (true) {
      final batch = _batch;
      if (batch != null && _index < batch.items.length) {
        yield batch.items[_index++];
        continue;
      }
      if (_ended) return;
      if (batch != null) _acknowledged = batch.id;
      final next = await _native.nextBatch(
          ack: _acknowledged, maxItems: maxItems, maxBytes: maxBytes, cancel: rust.WseCancel());
      if (next.end) {
        _ended = true;
        _batch = null;
        return;
      }
      _batch = next;
      _index = 0;
    }
  }

  /// Sends `{"t": kind, "p": json}`, waiting for room in the queue.
  Future<void> send(String kind, String json) => _native.send(kind: kind, json: json);

  /// Sends at once; throws when the queue is full.
  void trySend(String kind, String json) => _native.trySend(kind: kind, json: json);

  /// Sends a signed message on this connection, or not at all; the nonce it went with.
  Future<String> sendSigned(String kind, String json, {String? nonce}) =>
      _native.sendSigned(kind: kind, json: json, nonce: nonce);

  void subscribe(List<String> topics) => _native.subscribe(topics: topics);
  void unsubscribe(List<String> topics) => _native.unsubscribe(topics: topics);
  void reconnect() => _native.reconnect();

  /// connecting, connected, reconnecting or closed.
  String get status => _native.status();
  String? get userId => _native.userId();

  /// Closes the client: what was sent goes out first, within [deadline].
  Future<void> close({Duration deadline = const Duration(seconds: 2)}) async {
    await _native.disconnect(deadlineMs: BigInt.from(deadline.inMilliseconds));
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
}) =>
    rust.WseConfig(
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
