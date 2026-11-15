// Takes the prebuilt Rust library of this release (checked by its SHA-256),
// or builds it from source with Cargo when the app asks for that or this is
// a checkout of the repository; see prebuilt.dart.
import 'binaries.dart';
import 'prebuilt.dart';

void main(List<String> args) => runHook(args, version: version, binaries: binaries);
