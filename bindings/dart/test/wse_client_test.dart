// Against the Rust client's example server: run-tests.sh starts it and
// passes its URL in WSE_TEST_URL.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:wse_client/wse_client.dart';

void main() {
  final url = Platform.environment['WSE_TEST_URL'] ?? 'ws://127.0.0.1:5007/wse';

  setUpAll(() async => WseClient.init());

  test('ws:// goes in the clear only when asked', () async {
    await expectLater(
      WseClient.connect(wseConfig(url)),
      throwsA(isA<WseError>().having((error) => error.kind, 'kind', 'config')),
    );
  });

  test('a private CA is trusted where it is named', () async {
    final tlsUrl = Platform.environment['WSE_TEST_TLS_URL']!;
    final ca = File(Platform.environment['WSE_TEST_CA']!).readAsBytesSync();
    final client = await WseClient.connect(
      wseConfig(tlsUrl, trust: WseTrust.roots([ca])),
    );
    expect(client.status, 'connected');
    await client.close();
    // The system's verifier and the Mozilla roots do not know that CA.
    for (final trust in [WseTrust.platform, WseTrust.mozilla]) {
      await expectLater(
        WseClient.connect(wseConfig(tlsUrl, reconnect: false, trust: trust)),
        throwsA(
          isA<WseError>()
              .having((error) => error.kind, 'kind', 'connect')
              .having(
                (error) => error.message,
                'message',
                contains('certificate'),
              ),
        ),
      );
    }
  });

  test('ticks come in order and echo answers', () async {
    final client = await WseClient.connect(
      wseConfig(url, topics: ['ticks'], insecure: true),
    );
    expect(client.status, 'connected');
    await client.send('echo', '{"text":"hallo"}');
    final ticks = <BigInt>[];
    String? echoed;
    await for (final item in client.events()) {
      if (item.kind == WseItemKind.message) {
        if (item.name == 'echoed') echoed = item.json;
        if (item.topic == 'ticks' && item.offset != null)
          ticks.add(item.offset!);
      }
      if (ticks.length >= 3 && echoed != null) break;
    }
    expect(echoed, '{"text":"hallo"}');
    expect(ticks, [...ticks]..sort());
    await client.close();
  });

  test(
    'the connection and the subscription say what the server keeps',
    () async {
      final client = await WseClient.connect(
        wseConfig(url, topics: ['ticks'], insecure: true),
      );
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
    },
  );

  test('a listener that stops in between loses nothing', () async {
    final client = await WseClient.connect(
      wseConfig(url, topics: ['ticks'], insecure: true),
    );
    final first = await client.events().first;
    final second = await client.events().first;
    expect(identical(first, second), isFalse);
    await client.close();
  });

  test(
    'one reader at a time: a second listener is refused, the first keeps its place',
    () async {
      final client = await WseClient.connect(
        wseConfig(url, topics: ['ticks'], insecure: true),
      );
      // A few items gather: the first batch holds more than one.
      await Future<void>.delayed(const Duration(milliseconds: 300));
      late final StreamSubscription<WseItem> first;
      final paused = Completer<void>();
      first = client.events().listen((_) {
        if (paused.isCompleted) return;
        // Paused with the rest of its batch in hand.
        first.pause();
        paused.complete();
      });
      await paused.future;
      final refused = Completer<Object>();
      final second = <WseItem>[];
      client.events().listen(second.add, onError: refused.complete);
      final error = await refused.future.timeout(const Duration(seconds: 5));
      expect(error, isA<WseError>().having((e) => e.kind, 'kind', 'busy'));
      expect(second, isEmpty);
      first.resume();
      await first.cancel();
      // Let go: the next reader reads on.
      expect(await client.events().first, isA<WseItem>());
      await client.close();
    },
  );

  test('a live message goes on the ready connection', () async {
    final client = await WseClient.connect(wseConfig(url, insecure: true));
    client.trySendLive('valve', '{"open":true}');
    await client.sendLive('valve', '{"open":false}');
    expect(
      () => client.trySendLive('valve', 'not json'),
      throwsA(isA<WseError>()),
    );
    await client.close();
  });

  test(
    'cancelling a listener that waits ends it, and the next one reads on',
    () async {
      // No topics: past its first items the server stays quiet.
      final client = await WseClient.connect(wseConfig(url, insecure: true));
      final waiting = client.events().listen((_) {});
      await Future<void>.delayed(const Duration(milliseconds: 300));
      await waiting.cancel().timeout(const Duration(seconds: 2));
      // The next listener is not refused as a second reader, and loses nothing.
      await client.send('echo', '{"text":"again"}');
      final echoed = await client
          .events()
          .firstWhere(
            (item) => item.kind == WseItemKind.message && item.name == 'echoed',
          )
          .timeout(const Duration(seconds: 5));
      expect(echoed.json, '{"text":"again"}');
      await client.close();
    },
  );

  test(
    'a paused client ends its connection and comes back when resumed',
    () async {
      final client = await WseClient.connect(
        wseConfig(url, topics: ['ticks'], insecure: true),
      );
      client.pause('manual');
      final paused = await client.events().firstWhere(
        (item) => item.kind == WseItemKind.paused,
      );
      expect(paused.reasons, ['manual']);
      expect(client.status, 'paused');
      client.resume('manual');
      final back = await client.events().firstWhere(
        (item) => item.kind == WseItemKind.connected,
      );
      expect(back.reconnected, isTrue);
      await client.close();
    },
  );

  test('recover-or-resync holds a topic until the app resynced it', () async {
    final client = await WseClient.connect(
      wseConfig(
        url,
        topics: ['ticks'],
        insecure: true,
        delivery: wseDelivery('board'),
      ),
    );
    final items = StreamIterator(client.events());
    WseItem? resync;
    while (await items.moveNext()) {
      final item = items.current;
      expect(
        item.kind == WseItemKind.message && item.topic == 'ticks',
        isFalse,
      );
      if (item.kind == WseItemKind.resync) {
        resync = item;
        break;
      }
    }
    expect(resync!.topics, ['ticks']);
    expect(resync.reasons, ['cold_start']);
    expect(client.topicStates.single.continuity, WseContinuity.resyncing);
    // Ticks come meanwhile, and wait.
    await Future<void>.delayed(const Duration(milliseconds: 300));
    client.resynced(resync.round!);
    WseItem? tick;
    while (await items.moveNext()) {
      final item = items.current;
      if (item.kind == WseItemKind.message && item.topic == 'ticks') {
        tick = item;
        break;
      }
    }
    expect(client.topicStates.single.continuity, WseContinuity.continuous);
    client.commit(tick!);
    final saved = jsonDecode(client.checkpoint()) as Map<String, dynamic>;
    expect(saved['consumer'], 'board');
    expect((saved['topics'] as Map)['ticks'], isNotNull);
    await items.cancel();
    await client.close();
  });

  Future<WseItem> message(StreamIterator<WseItem> items, String kind) async {
    while (await items.moveNext()) {
      final item = items.current;
      if (item.kind == WseItemKind.message && item.name == kind) return item;
    }
    throw StateError('the events ended before $kind');
  }

  test('a client started with no server keeps what it sends', () async {
    // Nobody answers on port 9: it starts at once all the same.
    final client = await WseClient.start(
      wseConfig('ws://127.0.0.1:9/wse', insecure: true),
    ).timeout(const Duration(seconds: 2));
    expect(client.status, isNot('connected'));
    client.trySend('echo', '{"n":1}');
    expect(client.kept, 1);
    await client.close(deadline: Duration.zero);
  });

  test('the events say what the close did not deliver, then end', () async {
    final client = await WseClient.connect(wseConfig(url, insecure: true));
    final items = StreamIterator(client.events());
    client.pause('test');
    while (client.status == 'connected') {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    // Paused, it waits in the outbox, in memory: the close refuses it.
    await client.sendWith('echo', '{"n":4}', id: 'left-behind');
    expect(client.kept, 1);
    await client.close(deadline: const Duration(milliseconds: 25));
    final refused = <String>[];
    while (await items.moveNext()) {
      final item = items.current;
      if (item.kind == WseItemKind.notDelivered) {
        refused.add('${item.reason}: ${item.ids.join(',')}');
      }
    }
    expect(refused, ['closed: left-behind']);
  });

  test('a send whose time ran out before it went is said expired', () async {
    // Nobody answers on port 9: what is sent waits, and its time runs out.
    final client = await WseClient.start(
      wseConfig('ws://127.0.0.1:9/wse', insecure: true),
    ).timeout(const Duration(seconds: 2));
    final items = StreamIterator(client.events());
    client.trySendWith(
      'echo',
      '{}',
      id: 'late',
      ttl: const Duration(milliseconds: -1),
    );
    String? why;
    while (why == null && await items.moveNext()) {
      final item = items.current;
      if (item.kind == WseItemKind.notDelivered && item.ids.contains('late')) {
        why = item.reason;
      }
    }
    expect(why, 'expired');
    expect(client.kept, 0);
    await items.cancel();
    // A deadline already past closes at once.
    await client
        .close(deadline: const Duration(milliseconds: -5))
        .timeout(const Duration(seconds: 5));
  });

  test('a started client gets in on its own', () async {
    final client = await WseClient.start(wseConfig(url, insecure: true));
    final items = StreamIterator(client.events());
    await client.send('echo', '{"n":3}');
    expect((await message(items, 'echoed')).json, '{"n":3}');
    await items.cancel();
    await client.close();
  });

  test('what is sent with no connection goes on the next', () async {
    final dir = Directory.systemTemp.createTempSync('wse-offline');
    final client = await WseClient.connect(
      wseConfig(
        url,
        insecure: true,
        offline: wseOffline(path: '${dir.path}/queue'),
      ),
    );
    final items = StreamIterator(client.events());
    client.pause('test');
    while (client.status == 'connected') {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    await client.send('echo', '{"n":1}');
    expect(client.kept, 1);
    client.resume('test');
    expect((await message(items, 'echoed')).json, '{"n":1}');
    await items.cancel();
    await client.close();
    dir.deleteSync(recursive: true);
  });

  test('a message the server took is said delivered by its id', () async {
    final client = await WseClient.connect(wseConfig(url, insecure: true));
    final items = StreamIterator(client.events());
    final id = await client.sendWith('echo', '{"n":2}', id: 'mine-1');
    expect(id, 'mine-1');
    var delivered = false;
    while (await items.moveNext()) {
      final item = items.current;
      if (item.kind == WseItemKind.delivered && item.ids.contains('mine-1')) {
        delivered = true;
        break;
      }
    }
    expect(delivered, isTrue);
    expect(client.kept, 0);
    client.clear();
    await items.cancel();
    await client.close();
  });

  test(
    'a request names its correlation id, and the reply names it back',
    () async {
      final client = await WseClient.connect(wseConfig(url, insecure: true));
      final items = StreamIterator(client.events());
      await client.sendWith(
        'echo',
        '{"n":5}',
        correlationId: 'order-7',
        priority: 8,
      );
      await client.sendWith(
        'echo',
        '{"n":6}',
        correlationId: 'order-8',
        live: true,
      );
      final replies = <String?>{};
      while (replies.length < 2 && await items.moveNext()) {
        final item = items.current;
        if (item.kind == WseItemKind.message && item.name == 'echoed') {
          replies.add(item.correlationId);
        }
      }
      expect(replies, {'order-7', 'order-8'});
      expect(
        () => client.sendWith('echo', '{}', priority: 256),
        throwsRangeError,
      );
      await items.cancel();
      await client.close();
    },
  );

  test(
    'a client gives up after its attempts, as soon as its delays say',
    () async {
      final watch = Stopwatch()..start();
      final client = await WseClient.start(
        wseConfig(
          'ws://127.0.0.1:9/wse',
          insecure: true,
          reconnectDelay: const Duration(milliseconds: 20),
          reconnectMaxDelay: const Duration(milliseconds: 40),
          reconnectJitter: false,
          reconnectMaxAttempts: 3,
        ),
      );
      final closed = await client.events().firstWhere(
        (item) => item.kind == WseItemKind.closed,
      );
      expect(closed.reason, startsWith('gave up reconnecting'));
      // Three attempts at 20 and 40 ms, not at a second and more.
      expect(watch.elapsed, lessThan(const Duration(seconds: 3)));
      await client.close();
    },
  );

  test('a send that requires a topic waits for its resync', () async {
    final client = await WseClient.connect(
      wseConfig(
        url,
        topics: ['ticks'],
        insecure: true,
        delivery: wseDelivery('board'),
      ),
    );
    final items = StreamIterator(client.events());
    WseItem? resync;
    while (await items.moveNext()) {
      if (items.current.kind == WseItemKind.resync) {
        resync = items.current;
        break;
      }
    }
    await client.sendRequiring('echo', '{"n":1}', ['ticks']);
    await client.send('echo', '{"n":2}');
    // The one that requires nothing goes past it.
    expect((await message(items, 'echoed')).json, '{"n":2}');
    client.resynced(resync!.round!);
    expect((await message(items, 'echoed')).json, '{"n":1}');
    await items.cancel();
    await client.close();
  });

  test('an encrypted session with the pinned server', () async {
    final identity = Platform.environment['WSE_TEST_IDENTITY']!;
    final client = await WseClient.connect(
      wseConfig(url, insecure: true, serverIdentityKeys: [identity]),
    );
    final items = StreamIterator(client.events());
    await client.send('echo', '{"sealed":true}');
    expect((await message(items, 'echoed')).json, '{"sealed":true}');
    await items.cancel();
    await client.close();
    // Another server's key: refused.
    final other = signingPublicKey(key: generateSigningKey());
    await expectLater(
      WseClient.connect(
        wseConfig(
          url,
          insecure: true,
          reconnect: false,
          serverIdentityKeys: [other],
        ),
      ),
      throwsA(isA<WseError>().having((e) => e.kind, 'kind', 'encryption')),
    );
  });

  test('signed messages both ways', () async {
    final signing = Platform.environment['WSE_TEST_SIGNING']!;
    final key = generateSigningKey();
    final client = await WseClient.connect(
      wseConfig(
        url,
        insecure: true,
        signingKey: key,
        serverSigningKeys: [signing],
      ),
    );
    final items = StreamIterator(client.events());
    await client.send(
      'bind_key',
      '{"public_key":"${signingPublicKey(key: key)}"}',
    );
    expect((await message(items, 'key_bound')).json, '{"bound":true}');
    expect(
      await client.sendSigned('order', '{"side":"buy"}', nonce: 'dart-1'),
      'dart-1',
    );
    final receipt = await message(items, 'receipt');
    expect(receipt.json, contains('"n":"dart-1"'));
    expect(receipt.verified, isTrue);
    await items.cancel();
    await client.close();
  });

  test('a closed client says so', () async {
    final client = await WseClient.connect(wseConfig(url, insecure: true));
    await client.close();
    expect(client.status, 'closed');
    expect(client.userId, isNull);
    final closed = throwsA(
      isA<WseError>().having((error) => error.kind, 'kind', 'closed'),
    );
    expect(() => client.pause('manual'), closed);
    expect(() => client.resume('manual'), closed);
    expect(() => client.reconnect(), closed);
    expect(() => client.subscribe(['ticks']), closed);
    expect(() => client.trySend('echo', '{}'), closed);
    await expectLater(client.send('echo', '{}'), closed);
    await expectLater(client.sendLive('echo', '{}'), closed);
    // What was not read yet, ending with the close; then nothing more.
    final items = await client.events().toList();
    expect(items.last.kind, WseItemKind.closed);
    await expectLater(client.events().toList(), completion(isEmpty));
    await client.close();
  });

  testWidgets('the app in the background pauses the client after the grace', (
    tester,
  ) async {
    final client = (await tester.runAsync(
      () => WseClient.connect(wseConfig(url, insecure: true)),
    ))!;
    final lifecycle = WseAppLifecycle(
      client,
      grace: const Duration(seconds: 5),
    );
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.paused);
    await tester.pump(const Duration(seconds: 4));
    await settle(tester, client, 'paused');
    expect(client.status, 'connected');
    await tester.pump(const Duration(seconds: 1));
    await settle(tester, client, 'paused');
    expect(client.status, 'paused');
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.resumed);
    await settle(tester, client, 'connected');
    expect(client.status, 'connected');
    lifecycle.dispose();
    await tester.runAsync(() => client.close());
  });

  testWidgets('an app back within the grace is not paused', (tester) async {
    final client = (await tester.runAsync(
      () => WseClient.connect(wseConfig(url, insecure: true)),
    ))!;
    final lifecycle = WseAppLifecycle(
      client,
      grace: const Duration(seconds: 5),
    );
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.paused);
    await tester.pump(const Duration(seconds: 2));
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.resumed);
    await tester.pump(const Duration(seconds: 10));
    await settle(tester, client, 'paused');
    expect(client.status, 'connected');
    lifecycle.dispose();
    await tester.runAsync(() => client.close());
  });

  testWidgets(
    'on a desktop a minimized window keeps the client',
    variant: TargetPlatformVariant.desktop(),
    (tester) async {
      final client = (await tester.runAsync(
        () => WseClient.connect(wseConfig(url, insecure: true)),
      ))!;
      final lifecycle = WseAppLifecycle(
        client,
        grace: const Duration(seconds: 5),
      );
      tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.hidden);
      await tester.pump(const Duration(seconds: 10));
      await settle(tester, client, 'paused');
      expect(client.status, 'connected');
      tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.resumed);
      lifecycle.dispose();
      await tester.runAsync(() => client.close());
    },
  );

  testWidgets(
    'a minimized window pauses the client when the app asks for it',
    variant: TargetPlatformVariant.desktop(),
    (tester) async {
      final client = (await tester.runAsync(
        () => WseClient.connect(wseConfig(url, insecure: true)),
      ))!;
      final lifecycle = WseAppLifecycle(
        client,
        grace: const Duration(seconds: 5),
        pauseMinimized: true,
      );
      tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.hidden);
      await tester.pump(const Duration(seconds: 5));
      await settle(tester, client, 'paused');
      expect(client.status, 'paused');
      tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.resumed);
      await settle(tester, client, 'connected');
      expect(client.status, 'connected');
      lifecycle.dispose();
      await tester.runAsync(() => client.close());
    },
  );

  testWidgets(
    'on a phone a hidden app pauses the client after the grace',
    variant: TargetPlatformVariant.mobile(),
    (tester) async {
      final client = (await tester.runAsync(
        () => WseClient.connect(wseConfig(url, insecure: true)),
      ))!;
      final lifecycle = WseAppLifecycle(
        client,
        grace: const Duration(seconds: 5),
      );
      tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.hidden);
      await tester.pump(const Duration(seconds: 5));
      await settle(tester, client, 'paused');
      expect(client.status, 'paused');
      tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.resumed);
      await settle(tester, client, 'connected');
      lifecycle.dispose();
      await tester.runAsync(() => client.close());
    },
  );

  testWidgets('no network pauses the client, another network renews it', (
    tester,
  ) async {
    final client = (await tester.runAsync(
      () => WseClient.connect(wseConfig(url, insecure: true)),
    ))!;
    final reasons = <String?>[];
    // Read in real time: the client's calls complete outside the fake one.
    final listening = (await tester.runAsync(
      () async => client.events().listen((item) {
        if (item.kind == WseItemKind.disconnected) reasons.add(item.reason);
      }),
    ))!;
    final network = WseNetworkMonitor(client);
    network.update(usable: true, interfaces: ['wifi']);
    network.update(usable: false, interfaces: []);
    await settle(tester, client, 'paused');
    expect(client.status, 'paused');
    network.update(usable: true, interfaces: ['mobile']);
    await settle(tester, client, 'connected');
    expect(client.status, 'connected');
    // Wi-Fi joins, mobile goes: one renewal, a settle after the last change.
    network.update(usable: true, interfaces: ['wifi', 'mobile']);
    await tester.pump(const Duration(milliseconds: 500));
    network.update(usable: true, interfaces: ['wifi']);
    await tester.pump(const Duration(milliseconds: 999));
    await tester.runAsync(
      () => Future<void>.delayed(const Duration(milliseconds: 300)),
    );
    expect(reasons, isNot(contains('reconnect asked')));
    await tester.pump(const Duration(milliseconds: 1));
    await tester.runAsync(() async {
      for (var i = 0; i < 40 && !reasons.contains('reconnect asked'); i++) {
        await Future<void>.delayed(const Duration(milliseconds: 50));
      }
    });
    expect(
      reasons.where((reason) => reason == 'reconnect asked'),
      hasLength(1),
    );
    network.dispose();
    await tester.runAsync(() async {
      await listening.cancel();
      await client.close();
    });
  });

  testWidgets('a closed client is left alone', (tester) async {
    final client = (await tester.runAsync(
      () => WseClient.connect(wseConfig(url, insecure: true)),
    ))!;
    final lifecycle = WseAppLifecycle(client, grace: Duration.zero);
    final network = WseNetworkMonitor(client);
    await tester.runAsync(() => client.close());
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.paused);
    await tester.pump(Duration.zero);
    network.update(usable: false, interfaces: []);
    network.update(usable: true, interfaces: ['wifi']);
    tester.binding.handleAppLifecycleStateChanged(AppLifecycleState.resumed);
    expect(client.status, 'closed');
    lifecycle.dispose();
    network.dispose();
  });

  test('a payload must be JSON', () async {
    final client = await WseClient.connect(wseConfig(url, insecure: true));
    expect(() => client.trySend('x', 'not json'), throwsA(isA<WseError>()));
    await client.close();
  });
}

/// Gives the client up to a second of real time to reach `status`.
Future<void> settle(WidgetTester tester, WseClient client, String status) =>
    tester.runAsync(() async {
      for (var i = 0; i < 20 && client.status != status; i++) {
        await Future<void>.delayed(const Duration(milliseconds: 50));
      }
    });
