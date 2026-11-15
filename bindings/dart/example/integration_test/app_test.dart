// On a device or simulator, against the Rust client's example server: pass
// its URL with --dart-define=WSE_URL=...
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:wse_client/wse_client.dart';
import 'package:wse_client_example/main.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('the client connects and the ticks come, in German and English', (tester) async {
    await WseClient.init();
    await tester.pumpWidget(const ExampleApp());
    for (var i = 0; i < 100 && find.textContaining('Letzter Tick: noch keiner').evaluate().isNotEmpty; i++) {
      await tester.pump(const Duration(milliseconds: 100));
    }
    expect(find.text('Verbindung: verbunden'), findsOneWidget);
    expect(find.textContaining('Letzter Tick: '), findsOneWidget);
    expect(find.textContaining('noch keiner'), findsNothing);
    await tester.tap(find.byKey(const Key('language')));
    await tester.pump();
    expect(find.text('Connection: connected'), findsOneWidget);
    expect(find.textContaining('Verbindung'), findsNothing);
  });

  test('a direct client hears the ticks too', () async {
    final client = await WseClient.connect(wseConfig(serverUrl, topics: ['ticks']));
    final first = await client.events().firstWhere((item) => item.kind == WseItemKind.message);
    expect(first.topic, 'ticks');
    await client.close();
  });
}
