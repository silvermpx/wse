// The Dart package against the conformance server (crates/wse-conformance),
// on this machine. Runs when WSE_CONFORMANCE holds the server's control URL
// (run-tests.sh starts it).
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:wse_client/wse_client.dart';

import 'conformance/driver.dart';

void main() {
  final control = Platform.environment['WSE_CONFORMANCE'];

  setUpAll(() async => WseClient.init());

  test('the Dart package passes every case it can', () async {
    if (control == null) {
      expect(
        Platform.environment['WSE_CONFORMANCE_REQUIRED'],
        isNot('1'),
        reason: 'WSE_CONFORMANCE_REQUIRED is set, but WSE_CONFORMANCE is not',
      );
      return;
    }
    expect(await runCases(control, agent: 'dart'), isEmpty);
  }, timeout: const Timeout(Duration(minutes: 10)));
}
