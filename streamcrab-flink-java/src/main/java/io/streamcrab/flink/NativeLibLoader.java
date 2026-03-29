package io.streamcrab.flink;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Singleton native library loader that handles Flink's classloader isolation.
 * Loads the native library from system classloader to avoid FLINK-5408 issues.
 */
public class NativeLibLoader {
    private static final AtomicBoolean loaded = new AtomicBoolean(false);
    private static volatile String libraryPath = null;

    /**
     * Set the path to the native library before calling ensureLoaded().
     * Can be set from Flink configuration.
     */
    public static void setLibraryPath(String path) {
        libraryPath = path;
    }

    /**
     * Ensure the native library is loaded exactly once.
     * Thread-safe, idempotent.
     */
    public static void ensureLoaded() {
        if (loaded.get()) return;

        synchronized (NativeLibLoader.class) {
            if (loaded.get()) return;

            if (libraryPath != null && new File(libraryPath).exists()) {
                System.load(libraryPath);
            } else {
                // Try standard library path
                System.loadLibrary("streamcrab_flink_bridge");
            }
            loaded.set(true);
        }
    }
}
