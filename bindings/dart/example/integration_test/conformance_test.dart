// The Dart package against the conformance server (crates/wse-conformance)
// on a phone's system, an iOS simulator's or an Android emulator's: pass the
// control channel's URL with --dart-define=WSE_CONFORMANCE=ws://...
// (run-integration-tests.sh does, for a simulator or an emulator). Each
// system's runs are an agent of their own: dart-ios, dart-android.
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:wse_client/wse_client.dart';

import '../../test/conformance/driver.dart';

const control = String.fromEnvironment('WSE_CONFORMANCE');

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'the Dart package passes every case it can, on ${Platform.operatingSystem}',
    (tester) async {
      await WseClient.init();
      final failed = await runCases(
        control,
        agent: 'dart-${Platform.operatingSystem}',
      );
      expect(failed, isEmpty);
    },
    skip: control.isEmpty,
    timeout: const Timeout(Duration(minutes: 10)),
  );
}
