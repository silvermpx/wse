// On a device or simulator, against the Rust client's example server: pass
// its URL with --dart-define=WSE_URL=..., its wss:// door with WSE_TLS_URL
// and the door's CA certificate (PEM, in base64) with WSE_CA.
import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:wse_client/wse_client.dart';
import 'package:wse_client_example/main.dart';

const tlsUrl = String.fromEnvironment('WSE_TLS_URL');
const identity = String.fromEnvironment('WSE_IDENTITY');
const signing = String.fromEnvironment('WSE_SIGNING');

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('the client connects and the ticks come, in German and English', (
    tester,
  ) async {
    await WseClient.init();
    await tester.pumpWidget(const ExampleApp());
    for (
      var i = 0;
      i < 100 &&
          find
              .textContaining('Letzter Tick: noch keiner')
              .evaluate()
              .isNotEmpty;
      i++
    ) {
      await tester.pump(const Duration(milliseconds: 100));
    }
    expect(find.text('Verbindung: verbunden'), findsOneWidget);
    expect(find.textContaining('Letzter Tick: '), findsOneWidget);
    expect(find.textContaining('noch keiner'), findsNothing);
    expectMenu(tester, ['English', 'WSE Demo beenden']);
    await tester.tap(find.byKey(const Key('language')));
    await tester.pump();
    expect(find.text('Connection: connected'), findsOneWidget);
    expect(find.textContaining('Verbindung'), findsNothing);
    expectMenu(tester, ['Deutsch', 'Quit WSE Demo']);
  });

  test('a direct client hears the ticks too', () async {
    final client = await WseClient.connect(
      wseConfig(serverUrl, topics: ['ticks'], insecure: true),
    );
    final first = await client.events().firstWhere(
      (item) => item.kind == WseItemKind.message,
    );
    expect(first.topic, 'ticks');
    await client.close();
  });

  test('a private CA is trusted where it is named', () async {
    final ca = base64.decode(const String.fromEnvironment('WSE_CA'));
    final client = await WseClient.connect(
      wseConfig(tlsUrl, trust: WseTrust.roots([ca])),
    );
    expect(client.status, 'connected');
    await client.close();
    // The system's verifier (on Android set up as the app started) and the
    // Mozilla roots do not know that CA.
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

  test('an encrypted session and signed messages on the device', () async {
    final sealed = await WseClient.connect(
      wseConfig(serverUrl, insecure: true, serverIdentityKeys: [identity]),
    );
    await sealed.send('echo', '{"sealed":true}');
    final echoed = await sealed.events().firstWhere(
      (item) => item.kind == WseItemKind.message && item.name == 'echoed',
    );
    expect(echoed.json, '{"sealed":true}');
    await sealed.close();

    final key = generateSigningKey();
    final client = await WseClient.connect(
      wseConfig(
        serverUrl,
        insecure: true,
        signingKey: key,
        serverSigningKeys: [signing],
      ),
    );
    final items = StreamIterator(client.events());
    Future<WseItem> next(String kind) async {
      while (await items.moveNext()) {
        if (items.current.kind == WseItemKind.message &&
            items.current.name == kind) {
          return items.current;
        }
      }
      throw StateError('no $kind');
    }

    await client.send(
      'bind_key',
      '{"public_key":"${signingPublicKey(key: key)}"}',
    );
    expect((await next('key_bound')).json, '{"bound":true}');
    await client.sendSigned('order', '{"side":"buy"}', nonce: 'flutter-1');
    final receipt = await next('receipt');
    expect(receipt.verified, isTrue);
    await items.cancel();
    await client.close();
  });
}

/// The menu bar's items on macOS, in the app's language; the other systems
/// show none.
void expectMenu(WidgetTester tester, List<String> labels) {
  if (defaultTargetPlatform != TargetPlatform.macOS) {
    expect(find.byType(PlatformMenuBar), findsNothing);
    return;
  }
  final bar = tester.widget<PlatformMenuBar>(find.byType(PlatformMenuBar));
  final shown = <String>[];
  void walk(List<PlatformMenuItem> items) {
    for (final item in items) {
      switch (item) {
        case PlatformMenu(:final menus):
          walk(menus);
        case PlatformMenuItemGroup(:final members):
          walk(members);
        default:
          shown.add(item.label);
      }
    }
  }

  walk(bar.menus);
  expect(shown, labels);
}
