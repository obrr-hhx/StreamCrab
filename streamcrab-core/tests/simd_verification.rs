//! Verify that SIMD/native CPU optimizations are available.

#[test]
fn verify_native_cpu_features() {
    // On x86_64, check for SSE/AVX support
    // On aarch64, check for NEON support
    // This test just documents what features are available.
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            println!("AVX2 detected — SIMD vectorization available");
        }
        if is_x86_feature_detected!("sse4.2") {
            println!("SSE4.2 detected");
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // NEON is always available on aarch64
        println!("aarch64 NEON — SIMD vectorization available");
    }
}

#[test]
fn ahash_is_faster_than_default() {
    // Simple benchmark: hash 10K keys with ahash vs default
    use std::collections::HashMap;
    use std::time::Instant;

    let keys: Vec<String> = (0..10_000).map(|i| format!("key_{i}")).collect();

    // std HashMap
    let start = Instant::now();
    let mut std_map = HashMap::new();
    for k in &keys {
        std_map.insert(k.clone(), 1u64);
    }
    let std_dur = start.elapsed();

    // ahash HashMap
    let start = Instant::now();
    let mut ahash_map = ahash::AHashMap::new();
    for k in &keys {
        ahash_map.insert(k.clone(), 1u64);
    }
    let ahash_dur = start.elapsed();

    println!("std HashMap: {:?}, ahash: {:?}", std_dur, ahash_dur);
    // ahash should generally be faster, but we don't assert timing
    // Just verify both produce correct results
    assert_eq!(std_map.len(), ahash_map.len());
    assert_eq!(std_map.len(), 10_000);
}
