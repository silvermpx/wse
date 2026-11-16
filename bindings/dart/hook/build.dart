// Takes the prebuilt Rust library of this release (checked by its SHA-256),
// or builds it from source with Cargo when the app asks for that or this is
// a checkout of the repository; see lib/src/hook/prebuilt.dart. pub.dev takes
// no file in hook/ but this one, so what it uses lives under lib/src/hook.
import 'package:wse_client/src/hook/binaries.dart';
import 'package:wse_client/src/hook/prebuilt.dart';

void main(List<String> args) =>
    runHook(args, version: version, binaries: binaries);
