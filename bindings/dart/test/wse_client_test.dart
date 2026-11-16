// Against the Rust client's example server: run-tests.sh starts it and
// passes its URL in WSE_TEST_URL.
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:wse_client/wse_client.dart';

void main() {
  final url = Platform.environment['WSE_TEST_URL'] ?? 'ws://127.0.0.1:5007/wse';

  setUpAll(() async => WseClient.init());

  test('ticks come in order and echo answers', () async {
    final client = await WseClient.connect(wseConfig(url, topics: ['ticks']));
    expect(client.status, 'connected');
    await client.send('echo', '{"text":"hallo"}');
    final ticks = <BigInt>[];
    String? echoed;
    await for (final item in client.events()) {
      if (item.kind == WseItemKind.message) {
        if (item.name == 'echoed') echoed = item.json;
        if (item.topic == 'ticks' && item.offset != null) ticks.add(item.offset!);
      }
      if (ticks.length >= 3 && echoed != null) break;
    }
    expect(echoed, '{"text":"hallo"}');
    expect(ticks, [...ticks]..sort());
    await client.close();
  });

  test('the connection and the subscription say what the server keeps', () async {
    final client = await WseClient.connect(wseConfig(url, topics: ['ticks']));
    WseItem? connected;
    WseItem? subscribed;
    await for (final item in client.events()) {
      if (item.kind == WseItemKind.connected) connected = item;
      if (item.kind == WseItemKind.subscribed) subscribed = item;
      if (connected != null && subscribed != null) break;
    }
    expect(connected!.recovery, isTrue);
    expect(subscribed!.topics, contains('ticks'));
    expect(subscribed.recoveries.map((r) => r.topic), contains('ticks'));
    await client.close();
  });

  test('a listener that stops in between loses nothing', () async {
    final client = await WseClient.connect(wseConfig(url, topics: ['ticks']));
    final first = await client.events().first;
    final second = await client.events().first;
    expect(identical(first, second), isFalse);
    await client.close();
  });

  test('a live message goes on the ready connection', () async {
    final client = await WseClient.connect(wseConfig(url));
    client.trySendLive('valve', '{"open":true}');
    await client.sendLive('valve', '{"open":false}');
    expect(() => client.trySendLive('valve', 'not json'), throwsA(isA<WseError>()));
    await client.close();
  });

  test('cancelling a listener that waits ends it, and the next one reads on', () async {
    // No topics: past its first items the server stays quiet.
    final client = await WseClient.connect(wseConfig(url));
    final waiting = client.events().listen((_) {});
    await Future<void>.delayed(const Duration(milliseconds: 300));
    await waiting.cancel().timeout(const Duration(seconds: 2));
    // The next listener is not refused as a second reader, and loses nothing.
    await client.send('echo', '{"text":"again"}');
    final echoed = await client
        .events()
        .firstWhere((item) => item.kind == WseItemKind.message && item.name == 'echoed')
        .timeout(const Duration(seconds: 5));
    expect(echoed.json, '{"text":"again"}');
    await client.close();
  });

  test('a paused client ends its connection and comes back when resumed', () async {
    final client = await WseClient.connect(wseConfig(url, topics: ['ticks']));
    client.pause('manual');
    final paused = await client.events().firstWhere((item) => item.kind == WseItemKind.paused);
    expect(paused.reasons, ['manual']);
    expect(client.status, 'paused');
    client.resume('manual');
    final back = await client.events().firstWhere((item) => item.kind == WseItemKind.connected);
    expect(back.reconnected, isTrue);
    await client.close();
  });

  test('a payload must be JSON', () async {
    final client = await WseClient.connect(wseConfig(url));
    expect(() => client.trySend('x', 'not json'), throwsA(isA<WseError>()));
    await client.close();
  });
}
