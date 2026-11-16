// Sets up Android's certificate verifier, which the default trust
// (WseTrust.platform) uses, as the Flutter engine starts: it reaches the
// system's trust store through the JVM and needs the application's context.
package dev.wse.flutter;

import android.content.Context;
import androidx.annotation.NonNull;
import io.flutter.embedding.engine.plugins.FlutterPlugin;

public final class WseClientPlugin implements FlutterPlugin {
    private static boolean ready;

    @Override
    public void onAttachedToEngine(@NonNull FlutterPluginBinding binding) {
        init(binding.getApplicationContext());
    }

    @Override
    public void onDetachedFromEngine(@NonNull FlutterPluginBinding binding) {}

    private static synchronized void init(Context context) {
        if (ready) return;
        System.loadLibrary("wse_dart");
        if (!initNative(context.getApplicationContext())) {
            throw new IllegalStateException("Android's certificate verifier could not be set up");
        }
        ready = true;
    }

    private static native boolean initNative(Context context);
}
