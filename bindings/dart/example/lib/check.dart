// The package in a release build of an app: it connects, hears a tick, trusts
// the wss:// door of the CA it is given and finds the system's verifier
// turning that door down, has an end-to-end encrypted session with the
// pinned server echo, and gets a signed receipt for a signed message. It
// prints "WSE_CHECK ok" or what failed, and shows nothing; on a desktop it
// also writes that to the file WSE_CHECK_FILE names (where it may: a macOS
// app's sandbox keeps it to its stdout) and exits, 0 for ok.
// run-integration-tests.sh builds it with MODE=release and passes the server
// with --dart-define (WSE_URL, WSE_TLS_URL, WSE_CA, WSE_IDENTITY,
// WSE_SIGNING).
import 'dart:convert';
import 'dart:io';

import 'package:flutter/widgets.dart';
import 'package:wse_client/wse_client.dart';

const serverUrl = String.fromEnvironment('WSE_URL');
const tlsUrl = String.fromEnvironment('WSE_TLS_URL');
const caBase64 = String.fromEnvironment('WSE_CA');
const identity = String.fromEnvironment('WSE_IDENTITY');
const signing = String.fromEnvironment('WSE_SIGNING');

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  String outcome;
  try {
    outcome = await check();
  } catch (error) {
    outcome = 'failed: $error';
  }
  debugPrint('WSE_CHECK $outcome');
  if (Platform.isMacOS || Platform.isWindows || Platform.isLinux) {
    stdout.writeln('WSE_CHECK $outcome');
    await stdout.flush();
    final file = Platform.environment['WSE_CHECK_FILE'];
    if (file != null) {
      try {
        File(file).writeAsStringSync('$outcome\n');
      } on FileSystemException {
        // Sandboxed: the outcome went to stdout.
      }
    }
    exit(outcome == 'ok' ? 0 : 1);
  }
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

  final sealed = await WseClient.connect(
    wseConfig(serverUrl, insecure: true, serverIdentityKeys: [identity]),
  );
  await sealed.send('echo', '{"sealed":true}');
  final echoed = await sealed
      .events()
      .firstWhere(
        (item) => item.kind == WseItemKind.message && item.name == 'echoed',
      )
      .timeout(const Duration(seconds: 20));
  await sealed.close();
  if (echoed.json != '{"sealed":true}') return 'failed: echo ${echoed.json}';

  final key = generateSigningKey();
  final signer = await WseClient.connect(
    wseConfig(
      serverUrl,
      insecure: true,
      signingKey: key,
      serverSigningKeys: [signing],
    ),
  );
  final items = signer.events().asBroadcastStream();
  final bound = items.firstWhere(
    (item) => item.kind == WseItemKind.message && item.name == 'key_bound',
  );
  await signer.send(
    'bind_key',
    '{"public_key":"${signingPublicKey(key: key)}"}',
  );
  await bound.timeout(const Duration(seconds: 20));
  final receipt = items.firstWhere(
    (item) => item.kind == WseItemKind.message && item.name == 'receipt',
  );
  await signer.sendSigned('order', '{"side":"buy"}', nonce: 'release-1');
  final signed = await receipt.timeout(const Duration(seconds: 20));
  await signer.close();
  if (!signed.verified) return 'failed: the receipt was not verified';
  return 'ok';
}
