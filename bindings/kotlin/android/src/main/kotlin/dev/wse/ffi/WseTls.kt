// Android's certificate verifier, which the default trust (WseTrust.Platform
// or none) uses: it reaches the system's trust store through the JVM and
// needs the application's context once per process.
package dev.wse.ffi

import android.content.Context
import androidx.startup.Initializer

/**
 * Sets up Android's certificate verifier. The library does it as the app starts, through
 * androidx.startup; an app that turned that off calls [init] itself before it connects.
 */
object WseTls {
    @Volatile private var ready = false

    /** Gives the verifier the application's context; once is enough. */
    @JvmStatic
    @Synchronized
    fun init(context: Context) {
        if (ready) return
        System.loadLibrary("wse_ffi")
        check(initNative(context.applicationContext)) { "Android's certificate verifier could not be set up" }
        ready = true
    }

    @JvmStatic
    private external fun initNative(context: Context): Boolean
}

/** Runs [WseTls.init] as the app starts. */
class WseTlsInitializer : Initializer<Unit> {
    override fun create(context: Context) = WseTls.init(context)

    override fun dependencies(): List<Class<out Initializer<*>>> = emptyList()
}
