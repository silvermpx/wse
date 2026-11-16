// Where the bindings load the Rust library from on the JVM. The JAR carries
// it for each platform under JNA's own prefix (linux-x86-64/libwse_ffi.so,
// darwin-aarch64/libwse_ffi.dylib, win32-x86-64/wse_ffi.dll, ...), where JNA
// finds it by name. A build for Linux on musl (Alpine) sits beside it, under
// the prefix with -musl, which JNA does not look for: on such a system the
// bindings name that one by its path in the JAR, and JNA takes it from there.
package dev.wse.ffi

import com.sun.jna.Platform
import java.io.File

internal object WseNativeLibrary {
    /** The name, or the path in the JAR, the bindings load the library by. */
    val name: String by lazy {
        val musl = "/${Platform.RESOURCE_PREFIX}-musl/libwse_ffi.so"
        if (Platform.isLinux() && onMusl() && WseNativeLibrary::class.java.getResource(musl) != null) musl else "wse_ffi"
    }

    // musl's dynamic loader is libc itself, mapped into every process it runs.
    private fun onMusl(): Boolean =
        try {
            File("/proc/self/maps").useLines { lines -> lines.any { "/ld-musl-" in it } }
        } catch (_: Exception) {
            false
        }
}
