// The build hook's prebuilt path: which library a target takes, where from,
// and that a file whose SHA-256 differs is never used.
import 'dart:convert';
import 'dart:io';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:hooks/hooks.dart';

import '../hook/prebuilt.dart';

void main() {
  late Directory dir;
  final library = utf8.encode('a library');
  final digest = sha256.convert(library).toString();

  setUp(() async => dir = await Directory.systemTemp.createTemp('wse-hook-'));
  tearDown(() async => dir.delete(recursive: true));

  PackageUserDefines defines(Map<String, Object?> values) => PackageUserDefines(
        workspacePubspec: PackageUserDefinesSource(defines: values, basePath: dir.uri),
      );

  Future<void> run({
    required Map<String, String> binaries,
    Map<String, Object?> userDefines = const {},
    OS targetOS = OS.iOS,
    Architecture architecture = Architecture.arm64,
    IOSSdk sdk = IOSSdk.iPhoneOS,
    Uri? base,
    required void Function(List<CodeAsset> assets) check,
  }) =>
      testCodeBuildHook(
        mainMethod: (args) => runHook(args, version: '9.9.9', binaries: binaries, base: base),
        targetOS: targetOS,
        targetArchitecture: architecture,
        targetIOSSdk: sdk,
        userDefines: defines(userDefines),
        check: (input, output) => check(output.assets.code),
      );

  test('a release takes the listed library of its target', () async {
    await Directory('${dir.path}/libs').create();
    await File('${dir.path}/libs/wse_dart-9.9.9-aarch64-apple-ios-sim.dylib').writeAsBytes(library);
    await run(
      binaries: {'aarch64-apple-ios-sim': digest, 'aarch64-apple-ios': '00'},
      userDefines: {'binaries': 'libs'},
      sdk: IOSSdk.iPhoneSimulator,
      check: (assets) {
        expect(assets, hasLength(1));
        expect(assets.single.id, 'package:wse_client/src/rust/frb_generated.io.dart');
        expect(assets.single.linkMode, isA<DynamicLoadingBundled>());
        expect(assets.single.file!.pathSegments.last, 'libwse_dart.dylib');
        expect(File.fromUri(assets.single.file!).readAsBytesSync(), library);
      },
    );
  });

  test('a library whose SHA-256 differs is refused', () async {
    await File('${dir.path}/wse_dart-9.9.9-aarch64-apple-ios.dylib').writeAsBytes(utf8.encode('another'));
    await expectLater(
      run(binaries: {'aarch64-apple-ios': digest}, userDefines: {'binaries': '.'}, check: (_) {}),
      throwsA(predicate((e) => '$e'.contains('SHA-256'))),
    );
  });

  test('a target without a library names the way to build it from source', () async {
    await expectLater(
      run(
        binaries: {'aarch64-apple-ios': digest},
        targetOS: OS.linux,
        architecture: Architecture.riscv64,
        check: (_) {},
      ),
      throwsA(predicate((e) => '$e'.contains('riscv64') && '$e'.contains('build: source'))),
    );
  });

  test('the library is downloaded from the release', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final asked = <String>[];
    server.listen((request) {
      asked.add(request.uri.path);
      request.response
        ..add(library)
        ..close();
    });
    final base = Uri.parse('http://127.0.0.1:${server.port}/v9.9.9/');
    try {
      for (var i = 0; i < 2; i++) {
        await run(
          binaries: {'aarch64-linux-android': digest},
          targetOS: OS.android,
          base: base,
          check: (assets) {
            expect(assets.single.file!.pathSegments.last, 'libwse_dart.so');
            expect(File.fromUri(assets.single.file!).readAsBytesSync(), library);
          },
        );
      }
    } finally {
      await server.close(force: true);
    }
    // Each run has a shared directory of its own, so each downloads.
    expect(asked, List.filled(2, '/v9.9.9/wse_dart-9.9.9-aarch64-linux-android.so'));
  });

  test('the mode: source in a checkout, binary in a release, as the app says', () {
    expect(buildMode(null, const {}), 'source');
    expect(buildMode(null, const {'aarch64-apple-ios': '00'}), 'binary');
    expect(buildMode('source', const {'aarch64-apple-ios': '00'}), 'source');
    expect(() => buildMode('binary', const {}), throwsA(predicate((e) => '$e'.contains('no prebuilt'))));
    expect(() => buildMode('fast', const {}), throwsArgumentError);
  });
}
