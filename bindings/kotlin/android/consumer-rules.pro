# JNA reaches its classes by reflection; the bindings call into the library by name.
-dontwarn java.awt.*
-keep class com.sun.jna.* { *; }
-keepclassmembers class * extends com.sun.jna.* { public *; }
-keep class dev.wse.ffi.** { *; }
