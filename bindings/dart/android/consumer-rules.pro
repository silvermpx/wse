# The Rust library calls the verifier's Java part through JNI.
-keep, includedescriptorclasses class org.rustls.platformverifier.** { *; }
-keep class dev.wse.flutter.WseClientPlugin { native <methods>; }
