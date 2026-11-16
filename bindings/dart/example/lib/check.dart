// The package in a release build of an app: it connects, hears a tick, trusts
// the wss:// door of the CA it is given and finds the system's verifier
// turning that door down. It prints "WSE_CHECK ok" or what failed, and shows
// nothing. run-integration-tests.sh builds it with MODE=release and passes
// the server with --dart-define (WSE_URL, WSE_TLS_URL, WSE_CA).
import 'dart:convert';

import 'package:flutter/widgets.dart';
import 'package:wse_client/wse_client.dart';

const serverUrl = String.fromEnvironment('WSE_URL');
const tlsUrl = String.fromEnvironment('WSE_TLS_URL');
const caBase64 = String.fromEnvironment('WSE_CA');

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  String outcome;
  try {
    outcome = await check();
  } catch (error) {
    outcome = 'failed: $error';
  }
  debugPrint('WSE_CHECK $outcome');
}

Future<String> check() async {
  await WseClient.init();
  final client = await WseClient.connect(
    wseConfig(serverUrl, topics: ['ticks'], insecure: true),
  );
  await client
      .events()
      .firstWhere(
        (item) => item.kind == WseItemKind.message && item.topic == 'ticks',
      )
      .timeout(const Duration(seconds: 20));
  await client.close();
  final named = await WseClient.connect(
    wseConfig(tlsUrl, trust: WseTrust.roots([base64.decode(caBase64)])),
  );
  await named.close();
  try {
    final unknown = await WseClient.connect(
      wseConfig(tlsUrl, reconnect: false),
    );
    await unknown.close();
    return 'failed: the system trusted a CA it does not know';
  } on WseError catch (error) {
    if (error.kind != 'connect' || !error.message.contains('certificate')) {
      return 'failed: ${error.kind}: ${error.message}';
    }
  }
  return 'ok';
}
