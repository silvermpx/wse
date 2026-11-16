// What the device does to a client: the app going to the background, and
// the network coming, going or changing. Both pause and resume the client
// for reasons of their own, so the application's own pauses stay.
package dev.wse.ffi

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.os.Handler
import android.os.Looper
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner

/**
 * Pauses a client [graceMillis] after the app went to the background, and resumes it when the app
 * comes back. The process's lifecycle: an Activity turning, or another one opening, does not touch
 * the connection. Call [start] and [stop] on the main thread.
 */
class WseAppLifecycle(
    private val client: WseClient,
    private val graceMillis: Long = 5_000,
) : DefaultLifecycleObserver {
    private val main = Handler(Looper.getMainLooper())
    private val pause = Runnable { runCatching { client.pause("background") } }

    fun start() = ProcessLifecycleOwner.get().lifecycle.addObserver(this)

    fun stop() {
        ProcessLifecycleOwner.get().lifecycle.removeObserver(this)
        main.removeCallbacks(pause)
    }

    override fun onStop(owner: LifecycleOwner) {
        main.removeCallbacks(pause)
        main.postDelayed(pause, graceMillis)
    }

    override fun onStart(owner: LifecycleOwner) {
        main.removeCallbacks(pause)
        runCatching { client.resume("background") }
    }
}

/**
 * Pauses a client while the device has no network, and makes its connection again when the default
 * network changes (Wi-Fi to cellular, say): the old connection may hang on a network that is gone.
 * Changes within [settleMillis] of each other count once. A LAN server needs no Internet: any
 * network counts. Needs ACCESS_NETWORK_STATE, which the library's manifest asks for.
 */
class WseNetworkMonitor(
    context: Context,
    private val client: WseClient,
    private val settleMillis: Long = 1_000,
) {
    private val connectivity = context.getSystemService(ConnectivityManager::class.java)
    private val main = Handler(Looper.getMainLooper())
    private val reconnect = Runnable { runCatching { client.reconnect() } }
    private var current: Any? = null
    private val callback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) {
            main.post { update(network) }
        }

        override fun onLost(network: Network) {
            main.post { if (network == current) update(null) }
        }
    }

    fun start() = connectivity.registerDefaultNetworkCallback(callback)

    fun stop() {
        runCatching { connectivity.unregisterNetworkCallback(callback) }
        main.removeCallbacks(reconnect)
    }

    /** The default network now, or null with none (on the main thread). */
    internal fun update(network: Any?) {
        if (network == null) {
            main.removeCallbacks(reconnect)
            current = null
            runCatching { client.pause("offline") }
            return
        }
        runCatching { client.resume("offline") }
        val before = current
        current = network
        if (before != null && before != network) {
            main.removeCallbacks(reconnect)
            main.postDelayed(reconnect, settleMillis)
        }
    }
}
