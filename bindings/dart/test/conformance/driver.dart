// The Dart package's driver for the conformance server
// (crates/wse-conformance): it runs each case it can, step by step, and
// records what its client saw and did; the server judges. The package's
// tests run it on this machine (test/conformance_test.dart), the example's
// integration test on a phone's system (example/integration_test).
import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:ffi' show Abi;
import 'dart:io';
import 'dart:typed_data';

import 'package:wse_client/wse_client.dart';

/// What the Dart package has, by the catalog's names.
const features = [
  'outbox',
  'start',
  'live',
  'msgpack',
  'e2e',
  'signing',
  'recover_or_resync',
];

/// The control channel: requests and their answers, by id.
class Control {
  Control._(this._socket) {
    _socket.listen((text) {
      final answer = jsonDecode(text as String) as Map<String, dynamic>;
      _answers.remove(answer['id'])?.complete(answer);
    });
  }

  final WebSocket _socket;
  final _answers = <int, Completer<Map<String, dynamic>>>{};
  var _next = 0;

  static Future<Control> open(String url) async =>
      Control._(await WebSocket.connect(url));

  Future<Map<String, dynamic>> call(
    String op, [
    Map<String, dynamic> body = const {},
  ]) async {
    final id = ++_next;
    final answer = Completer<Map<String, dynamic>>();
    _answers[id] = answer;
    _socket.add(jsonEncode({...body, 'id': id, 'op': op}));
    final got = await answer.future;
    if (got['ok'] != true) throw StateError('$op: ${got['error']}');
    return got;
  }

  Future<void> close() => _socket.close();
}

/// One case: the client, what it saw, and the steps' doings.
class Run {
  Run(this.begun);

  final Map<String, dynamic> begun;
  final trace = <Map<String, dynamic>>[];
  final _items = Queue<WseItem>();
  final _began = Stopwatch()..start();
  final _published = <String, List<int>>{};

  /// Resync rounds waiting for a step to say the application resynced.
  final _resyncs = <(BigInt, List<String>)>[];
  WseClient? _client;
  StreamSubscription<WseItem>? _reading;
  var _ready = false;
  var _rr = false;
  Uint8List? _key;
  var _bound = false;

  void _record(Map<String, dynamic> event) =>
      trace.add({...event, 't': _began.elapsedMilliseconds});

  Future<void> go(Control control) async {
    final steps = (begun['steps'] as List).cast<Map<String, dynamic>>();
    for (var at = 0; at < steps.length; at++) {
      final step = steps[at];
      _drain();
      if (step['server'] != null) {
        _record({'ev': 'step', 'server': step['server']});
        try {
          final done = await control.call('act', {'step': step});
          if (step['server'] == 'publish') {
            _published
                .putIfAbsent(done['topic'] as String, () => [])
                .addAll((done['n'] as List).cast<int>());
          }
        } catch (error) {
          _record({'ev': 'error', 'message': 'step $at: $error'});
          break;
        }
        continue;
      }
      try {
        if (!await _clientStep(step)) {
          _record({'ev': 'timeout', 'step': at, 'waiting': step});
          break;
        }
      } catch (error) {
        _record({'ev': 'error', 'message': 'step $at: $error'});
        break;
      }
    }
    await close();
  }

  WseClient get _open => _client ?? (throw StateError('no client'));

  /// A step of the client's: false when what it waits for did not come.
  Future<bool> _clientStep(Map<String, dynamic> step) async {
    switch (step['client']) {
      case 'connect':
        await _connect(step);
      case 'wait':
        return _until(step, (step['timeout_ms'] as int?) ?? 10000);
      case 'send':
        await _send(step);
      case 'pause':
        _open.pause('test');
      case 'resume':
        _open.resume('test');
      case 'resynced':
        final topics = (step['topics'] as List).cast<String>();
        _record({'ev': 'resynced', 'topics': topics});
        final done = _resyncs.where((r) => r.$2.any(topics.contains)).toList();
        _resyncs.removeWhere((r) => r.$2.any(topics.contains));
        for (final (round, _) in done) {
          _open.resynced(round);
        }
      case 'sleep':
        final end = _began.elapsedMilliseconds + ((step['ms'] as int?) ?? 0);
        while (_began.elapsedMilliseconds < end) {
          await _pump();
        }
      case 'close':
        await close();
      default:
        throw StateError('no such client step: ${step['client']}');
    }
    return true;
  }

  Future<void> _connect(Map<String, dynamic> step) async {
    // A client made again: the last one goes, as a dropped handle's does.
    if (_client != null) await close();
    final user = (step['user'] as String?) ?? 'ann';
    _rr = step['recover_or_resync'] == true;
    final signing = step['signing'] == true;
    _key = signing ? generateSigningKey() : null;
    final config = wseConfig(
      begun['url'] as String,
      token: (begun['tokens'] as Map)[user] as String?,
      topics: ((step['topics'] as List?) ?? const []).cast<String>(),
      msgpack: step['format'] == 'msgpack',
      reconnectDelay: const Duration(milliseconds: 50),
      reconnectMaxDelay: const Duration(milliseconds: 300),
      reconnectJitter: false,
      insecure: true,
      serverIdentityKeys: step['e2e'] == true
          ? [begun['identity_key'] as String]
          : const [],
      signingKey: _key,
      serverSigningKeys: signing ? [begun['signing_key'] as String] : const [],
      delivery: _rr ? wseDelivery('conformance') : null,
    );
    final client = step['start'] == true
        ? await WseClient.start(config)
        : await WseClient.connect(config);
    _client = client;
    _reading = client.events().listen(_items.add);
  }

  Future<void> _send(Map<String, dynamic> step) async {
    final kind = step['type'] as String;
    final count = (step['count'] as int?) ?? 1;
    for (var k = 1; k <= count; k++) {
      final id = (step['id'] as String?) ?? '$kind-$k';
      final payload = jsonEncode(step['payload'] ?? {'n': k});
      if (step['signed'] == true) {
        if (!_bound) {
          var ok = true;
          try {
            await _open.sendWith(
              'bind_key',
              jsonEncode({'public_key': signingPublicKey(key: _key!)}),
              id: 'bind-key',
            );
          } catch (_) {
            ok = false;
          }
          _record({
            'ev': 'sent',
            'id': 'bind-key',
            'type': 'bind_key',
            'live': false,
            'ok': ok,
          });
          if (!ok ||
              !await _until({'for': 'message', 'type': 'key_bound'}, 10000)) {
            throw StateError('the server never bound the key');
          }
          _bound = true;
        }
        var ok = true;
        try {
          await _open.sendSigned(kind, payload, nonce: id);
        } catch (_) {
          ok = false;
        }
        _record({
          'ev': 'sent',
          'id': id,
          'type': kind,
          'signed': true,
          'live': false,
          'ok': ok,
        });
        continue;
      }
      final live = step['live'] == true;
      var ok = true;
      try {
        if (live) {
          _open.trySendWith(
            kind,
            payload,
            id: id,
            correlationId: step['correlation_id'] as String?,
            priority: (step['priority'] as int?) ?? 0,
            live: true,
          );
        } else {
          await _open.sendWith(
            kind,
            payload,
            id: id,
            correlationId: step['correlation_id'] as String?,
            priority: (step['priority'] as int?) ?? 0,
          );
        }
      } catch (_) {
        ok = false;
      }
      _record({'ev': 'sent', 'id': id, 'type': kind, 'live': live, 'ok': ok});
    }
  }

  /// Takes what the client has for the application now.
  void _drain() {
    while (_items.isNotEmpty) {
      _take(_items.removeFirst());
    }
  }

  /// Lets what comes arrive, then takes it.
  Future<void> _pump() async {
    await Future<void>.delayed(const Duration(milliseconds: 10));
    _drain();
  }

  Future<bool> _until(Map<String, dynamic> step, int ms) async {
    final end = _began.elapsedMilliseconds + ms;
    while (true) {
      _drain();
      if (_holds(step)) return true;
      if (_began.elapsedMilliseconds >= end) return false;
      await _pump();
    }
  }

  void _take(WseItem item) {
    switch (item.kind) {
      case WseItemKind.message:
        final payload = item.json == null ? null : jsonDecode(item.json!);
        final n = payload is Map ? payload['n'] : null;
        _record({
          'ev': 'message',
          'type': item.name,
          'topic': item.topic,
          'n': n is int ? n : null,
          'payload': payload,
          'cid': item.correlationId,
        });
        // Applied: the checkpoint moves on.
        if (_rr && item.topic != null) _client?.commit(item);
      case WseItemKind.connected:
        _ready = true;
        _record({'ev': 'ready'});
      case WseItemKind.disconnected || WseItemKind.paused:
        _ready = false;
      case WseItemKind.gap:
        _record({'ev': 'break', 'topic': item.topic, 'kind': 'gap'});
      case WseItemKind.continuity:
        if (item.continuity != WseContinuity.continuous) {
          _record({
            'ev': 'break',
            'topic': item.topic,
            'kind': 'continuity',
            'reason': item.reason,
          });
        }
      case WseItemKind.resync:
        final cold = item.reasons.every((r) => r == 'cold_start');
        _record({
          'ev': 'resync',
          'topics': item.topics,
          'auto': cold,
          'reasons': item.reasons,
        });
        // At a cold start the application has its state at once.
        if (cold) {
          _record({'ev': 'resynced', 'topics': item.topics, 'auto': true});
          _client?.resynced(item.round!);
        } else {
          _resyncs.add((item.round!, item.topics));
        }
      case WseItemKind.delivered:
        _record({'ev': 'delivered', 'ids': item.ids});
      case WseItemKind.notDelivered:
        _record({
          'ev': 'not_delivered',
          'reason': item.reason,
          'ids': item.ids,
        });
      default:
        break;
    }
  }

  bool _holds(Map<String, dynamic> step) {
    int messages(bool Function(Map<String, dynamic>) pick) =>
        trace.where((e) => e['ev'] == 'message' && pick(e)).length;
    final topic = step['topic'];
    final count = (step['count'] as int?) ?? 1;
    switch (step['for']) {
      case 'ready':
        return _ready;
      case 'not_ready':
        return !_ready;
      case 'messages':
        return messages((e) => e['topic'] == topic) >= count;
      case 'message':
        return messages((e) => e['type'] == step['type']) >= count;
      case 'published':
        final last = (step['last'] as int?) ?? 1;
        final numbers = _published[topic] ?? const <int>[];
        final wanted = numbers.length < last
            ? numbers
            : numbers.sublist(numbers.length - last);
        return wanted.length == last &&
            wanted.every(
              (n) => trace.any(
                (e) =>
                    e['ev'] == 'message' && e['topic'] == topic && e['n'] == n,
              ),
            );
      case 'break':
        return trace.any(
          (e) =>
              (e['ev'] == 'break' && e['topic'] == topic) ||
              (e['ev'] == 'resync' && (e['topics'] as List).contains(topic)),
        );
      case 'resync':
        return trace.any(
          (e) =>
              e['ev'] == 'resync' &&
              e['auto'] != true &&
              (e['topics'] as List).contains(topic),
        );
      case 'delivered':
        final delivered = trace
            .where((e) => e['ev'] == 'delivered')
            .expand((e) => e['ids'] as List)
            .toSet();
        final ids = step['all'] == true
            ? trace
                  .where(
                    (e) =>
                        e['ev'] == 'sent' &&
                        e['ok'] == true &&
                        e['live'] != true &&
                        e['signed'] != true,
                  )
                  .map((e) => e['id'])
            : step['ids'] as List;
        return ids.every(delivered.contains);
      default:
        return false;
    }
  }

  Future<void> close() async {
    _drain();
    final client = _client;
    if (client != null) {
      await client.close(deadline: const Duration(seconds: 1));
      // What came before the close is the case's too.
      await Future<void>.delayed(const Duration(milliseconds: 20));
      _drain();
      await _reading?.cancel();
    }
    _client = null;
    _reading = null;
    _ready = false;
  }
}

/// Runs, as [agent], every case the conformance server at [control] gives:
/// the cases that failed, with why. WseClient.init() comes first.
Future<List<String>> runCases(String control, {required String agent}) async {
  final channel = await Control.open(control);
  final hello = await channel.call('hello', {
    'agent': agent,
    'version': '3.2.0',
    'runtime': 'dart ${Platform.version.split(' ').first}',
    'os': '${Platform.operatingSystem} ${Platform.operatingSystemVersion}',
    'arch': Abi.current().toString(),
    'features': features,
  });
  final failed = <String>[];
  for (final entry in (hello['cases'] as List).cast<Map>()) {
    final name = entry['case'] as String;
    final begun = await channel.call('begin', {'case': name});
    final run = Run(begun);
    await run.go(channel);
    for (var at = 0; at < run.trace.length; at += 500) {
      final end = at + 500 < run.trace.length ? at + 500 : run.trace.length;
      await channel.call('trace', {'events': run.trace.sublist(at, end)});
    }
    final judged = await channel.call('end');
    if (judged['verdict'] != 'pass') {
      failed.add('$name: ${judged['failures']}');
    }
  }
  await channel.call('bye');
  await channel.close();
  return failed;
}
