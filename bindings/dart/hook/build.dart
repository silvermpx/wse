// Builds the Rust library from source with Cargo (the app needs a Rust
// toolchain); a release of the package downloads prebuilt binaries instead.
import 'package:flutter_rust_bridge_hooks/flutter_rust_bridge_hooks.dart';

void main(List<String> args) => build(args, (input, output) async {
      await const FlutterRustBridgeNativeAssetsBuilder(
        cratePath: 'rust',
        extraCargoEnvironmentVariables: {'IPHONEOS_DEPLOYMENT_TARGET': '15.0'},
      ).run(input: input, output: output);
    });
