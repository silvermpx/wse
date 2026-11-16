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

  test('a payload must be JSON', () async {
    final client = await WseClient.connect(wseConfig(url));
    expect(() => client.trySend('x', 'not json'), throwsA(isA<WseError>()));
    await client.close();
  });
}
