// The prebuilt Rust library of a release: the file of the target, from a
// directory the app names or downloaded from the release, and used only when
// its SHA-256 is the one the release listed.
import 'dart:io';
import 'dart:typed_data';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';
import 'package:flutter_rust_bridge_hooks/flutter_rust_bridge_hooks.dart';

const _asset = 'src/rust/frb_generated.io.dart';
const _crate = 'wse_dart';
const _releases = 'https://github.com/silvermpx/wse/releases/download/';
const _howToBuild =
    'build it from source (a Rust toolchain, plus Xcode or the Android NDK) with, in the '
    "app's pubspec.yaml:\n\n  hooks:\n    user_defines:\n      wse_client:\n        build: source\n";

/// Runs the build hook: the library of [version] listed in [binaries]
/// (target triple to SHA-256), or Cargo when the app asks for source or no
/// library is listed (a checkout of the repository). [base] is where the
/// release's files are, by default its GitHub release.
Future<void> runHook(
  List<String> args, {
  required String version,
  required Map<String, String> binaries,
  Uri? base,
}) => build(args, (input, output) async {
  if (!input.config.buildCodeAssets) return;
  switch (buildMode(input.userDefines['build'], binaries)) {
    case 'source':
      await const FlutterRustBridgeNativeAssetsBuilder(
        assetName: _asset,
        cratePath: 'rust',
        extraCargoEnvironmentVariables: {'IPHONEOS_DEPLOYMENT_TARGET': '15.0'},
      ).run(input: input, output: output);
    default:
      await _prebuilt(
        input,
        output,
        version,
        binaries,
        base ??
            Uri.parse(
              '$_releases'
              'v$version/',
            ),
      );
  }
});

/// `source` or `binary`: what the app's `build` define says, else binary
/// when the release listed its libraries.
String buildMode(Object? define, Map<String, String> binaries) =>
    switch (define) {
      null => binaries.isEmpty ? 'source' : 'binary',
      'source' => 'source',
      'binary' when binaries.isEmpty => throw StateError(
        'wse_client: this copy has no prebuilt libraries; $_howToBuild',
      ),
      'binary' => 'binary',
      _ => throw ArgumentError.value(
        define,
        'build',
        'wse_client: build is "source" or "binary"',
      ),
    };

Future<void> _prebuilt(
  BuildInput input,
  BuildOutputBuilder output,
  String version,
  Map<String, String> binaries,
  Uri base,
) async {
  final code = input.config.code;
  if (code.linkModePreference == LinkModePreference.static) {
    throw UnsupportedError(
      'wse_client: the prebuilt libraries are dynamic; $_howToBuild',
    );
  }
  final triple = _triple(code);
  final digest = binaries[triple];
  if (triple == null || digest == null) {
    throw UnsupportedError(
      'wse_client $version has no prebuilt library for ${code.targetOS} on ${code.targetArchitecture}'
      '${triple == null ? '' : ' ($triple)'}; $_howToBuild',
    );
  }
  final fileName = code.targetOS.libraryFileName(
    _crate,
    DynamicLoadingBundled(),
  );
  final released =
      '$_crate-$version-$triple${fileName.substring(fileName.lastIndexOf('.'))}';
  // Kept under the library's own name, whichever way it came.
  final library = File.fromUri(
    input.outputDirectoryShared.resolve('$_crate-$version/$triple/$fileName'),
  );
  if (!await library.exists() ||
      sha256.convert(await library.readAsBytes()).toString() != digest) {
    // A directory, whether or not the define ends in a slash.
    final local = switch (input.userDefines.path('binaries')) {
      null => null,
      final path => Directory.fromUri(path).uri,
    };
    final from = local?.resolve(released) ?? base.resolve(released);
    final bytes = local != null
        ? await File.fromUri(from).readAsBytes()
        : await _download(from);
    _check(bytes, digest, local != null ? from.toFilePath() : '$from');
    await library.parent.create(recursive: true);
    final part = File('${library.path}.part');
    await part.writeAsBytes(bytes, flush: true);
    await part.rename(library.path);
  }
  output.assets.code.add(
    CodeAsset(
      package: input.packageName,
      name: _asset,
      linkMode: DynamicLoadingBundled(),
      file: library.uri,
    ),
  );
}

void _check(List<int> bytes, String digest, String from) {
  final actual = sha256.convert(bytes).toString();
  if (actual != digest) {
    throw StateError(
      'wse_client: the library from $from has SHA-256 $actual, the release listed $digest',
    );
  }
}

Future<List<int>> _download(Uri url) async {
  final client = HttpClient()
    ..connectionTimeout = const Duration(seconds: 30)
    ..findProxy = HttpClient.findProxyFromEnvironment;
  try {
    final response = await (await client.getUrl(url)).close();
    if (response.statusCode != HttpStatus.ok) {
      throw HttpException(
        'wse_client: ${response.statusCode} for the prebuilt library',
        uri: url,
      );
    }
    final bytes = BytesBuilder(copy: false);
    await response.forEach(bytes.add);
    return bytes.takeBytes();
  } finally {
    client.close(force: true);
  }
}

String? _triple(CodeConfig code) =>
    switch ((code.targetOS, code.targetArchitecture)) {
      (OS.android, Architecture.arm64) => 'aarch64-linux-android',
      (OS.android, Architecture.arm) => 'armv7-linux-androideabi',
      (OS.android, Architecture.x64) => 'x86_64-linux-android',
      (OS.iOS, Architecture.arm64) =>
        code.iOS.targetSdk == IOSSdk.iPhoneSimulator
            ? 'aarch64-apple-ios-sim'
            : 'aarch64-apple-ios',
      (OS.iOS, Architecture.x64) => 'x86_64-apple-ios',
      (OS.macOS, Architecture.arm64) => 'aarch64-apple-darwin',
      (OS.macOS, Architecture.x64) => 'x86_64-apple-darwin',
      (OS.linux, Architecture.arm64) => 'aarch64-unknown-linux-gnu',
      (OS.linux, Architecture.x64) => 'x86_64-unknown-linux-gnu',
      (OS.windows, Architecture.x64) => 'x86_64-pc-windows-msvc',
      (OS.windows, Architecture.arm64) => 'aarch64-pc-windows-msvc',
      _ => null,
    };
