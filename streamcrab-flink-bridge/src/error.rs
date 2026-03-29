//! JNI error conversion helpers.

use jni::JNIEnv;

/// Throw a Java RuntimeException with the given message.
/// Any error from the JNI call itself is silently ignored — we are already
/// in an error-handling path and cannot propagate further.
pub fn throw_java_exception(env: &mut JNIEnv, msg: &str) {
    let _ = env.throw_new("java/lang/RuntimeException", msg);
}

/// Execute `f` inside a `catch_unwind` boundary and convert any Rust error or
/// panic to a Java exception.  Returns `default_val` on failure so JNI entry
/// points can return a safe sentinel value (0, null, etc.).
pub fn jni_safe<F, R>(env: &mut JNIEnv, default_val: R, f: F) -> R
where
    F: FnOnce(&mut JNIEnv) -> anyhow::Result<R> + std::panic::UnwindSafe,
{
    // We need to re-borrow env inside the closure, but catch_unwind requires
    // UnwindSafe.  We use AssertUnwindSafe here because JNIEnv is not
    // UnwindSafe, but we never resume after an unwind — we immediately
    // throw a Java exception and return, so no invariants are violated.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(env))) {
        Ok(Ok(val)) => val,
        Ok(Err(e)) => {
            throw_java_exception(env, &format!("{:#}", e));
            default_val
        }
        Err(_panic) => {
            throw_java_exception(env, "Rust panic in native code");
            default_val
        }
    }
}

#[cfg(test)]
mod tests {
    // error.rs helpers require a live JVM to test fully; we verify the module
    // at least compiles correctly via the integration of jni_safe in jni_entry.
}
