use streamcrab_api::environment::StreamExecutionEnvironment;

#[test]
fn test_wordcount() {
    let env = StreamExecutionEnvironment::new("wordcount");

    // Input: pre-tokenized words with count 1
    // Simulates: "hello world" + "hello streamcrab" + "world of streams"
    let words = vec![
        ("hello".to_string(), 1i32),
        ("world".to_string(), 1),
        ("hello".to_string(), 1),
        ("streamcrab".to_string(), 1),
        ("world".to_string(), 1),
        ("of".to_string(), 1),
        ("streams".to_string(), 1),
    ];

    let results = env
        .from_iter(words)
        .key_by(|(word, _): &(String, i32)| word.clone())
        .reduce(|(w1, c1), (_, c2)| (w1, c1 + c2))
        .execute_with_parallelism(2)
        .unwrap();

    // Verify final counts
    let final_counts = results.lock().unwrap();
    assert_eq!(final_counts.get("hello"), Some(&("hello".to_string(), 2)));
    assert_eq!(final_counts.get("world"), Some(&("world".to_string(), 2)));
    assert_eq!(
        final_counts.get("streamcrab"),
        Some(&("streamcrab".to_string(), 1))
    );
    assert_eq!(final_counts.get("of"), Some(&("of".to_string(), 1)));
    assert_eq!(final_counts.get("streams"), Some(&("streams".to_string(), 1)));
    assert_eq!(final_counts.len(), 5);
}

#[test]
fn test_keyed_sum() {
    let env = StreamExecutionEnvironment::new("keyed-sum");

    // Input: user_id -> amount
    let transactions = vec![
        ("user_1".to_string(), 10i32),
        ("user_2".to_string(), 20),
        ("user_1".to_string(), 15),
        ("user_3".to_string(), 30),
        ("user_2".to_string(), 25),
        ("user_1".to_string(), 5),
    ];

    let results = env
        .from_iter(transactions)
        .key_by(|(user, _): &(String, i32)| user.clone())
        .reduce(|(u1, a1), (_, a2)| (u1, a1 + a2))
        .execute_with_parallelism(2)
        .unwrap();

    // Verify final sums
    let final_sums = results.lock().unwrap();
    assert_eq!(final_sums.get("user_1"), Some(&("user_1".to_string(), 30)));
    assert_eq!(final_sums.get("user_2"), Some(&("user_2".to_string(), 45)));
    assert_eq!(final_sums.get("user_3"), Some(&("user_3".to_string(), 30)));
    assert_eq!(final_sums.len(), 3);
}

/// WordCount using flat_map to tokenize lines, then reduce to count words.
/// This is the canonical stream processing example: lines -> words -> counts.
#[test]
fn test_wordcount_with_flat_map() {
    let env = StreamExecutionEnvironment::new("wordcount-flatmap");

    let lines = vec![
        "hello world".to_string(),
        "hello streamcrab".to_string(),
        "world of streams".to_string(),
    ];

    let results = env
        .from_iter(lines)
        // flat_map: each line -> multiple (word, 1) pairs
        .flat_map(|line: &String| {
            line.split_whitespace()
                .map(|w| (w.to_string(), 1i32))
                .collect::<Vec<_>>()
        })
        .key_by(|(word, _): &(String, i32)| word.clone())
        .reduce(|(w, c1), (_, c2)| (w, c1 + c2))
        .execute_with_parallelism(2)
        .unwrap();

    let final_counts = results.lock().unwrap();
    assert_eq!(final_counts.get("hello"), Some(&("hello".to_string(), 2)));
    assert_eq!(final_counts.get("world"), Some(&("world".to_string(), 2)));
    assert_eq!(final_counts.get("streamcrab"), Some(&("streamcrab".to_string(), 1)));
    assert_eq!(final_counts.get("of"), Some(&("of".to_string(), 1)));
    assert_eq!(final_counts.get("streams"), Some(&("streams".to_string(), 1)));
    assert_eq!(final_counts.len(), 5);
}

/// Stress test: 100K records, 100 unique keys, parallelism=8.
///
/// Generates records in round-robin order to simulate interleaved arrival:
///   (key=0, 1), (key=1, 1), ..., (key=99, 1), (key=0, 1), ...
/// Each key appears exactly 1000 times, so the expected sum per key is 1000.
/// Any data loss would show up as a count < 1000 for some key.
#[test]
fn test_stress_100k_parallelism_8() {
    let env = StreamExecutionEnvironment::new("stress-100k");

    const NUM_KEYS: usize = 100;
    const RECORDS_PER_KEY: i64 = 1000;

    // Round-robin to mix keys (simulates interleaved record arrival).
    let data: Vec<(usize, i64)> = (0..RECORDS_PER_KEY)
        .flat_map(|_| (0..NUM_KEYS).map(|k| (k, 1i64)))
        .collect();

    assert_eq!(data.len(), (NUM_KEYS as i64 * RECORDS_PER_KEY) as usize);

    let results = env
        .from_iter(data)
        .key_by(|(k, _): &(usize, i64)| *k)
        .reduce(|(k, c1), (_, c2)| (k, c1 + c2))
        .execute_with_parallelism(8)
        .unwrap();

    let final_results = results.lock().unwrap();

    // All 100 keys must appear (no key routed to wrong subtask).
    assert_eq!(
        final_results.len(),
        NUM_KEYS,
        "expected {} unique keys, got {}",
        NUM_KEYS,
        final_results.len()
    );

    // Each key must accumulate exactly RECORDS_PER_KEY (no data loss).
    for key in 0..NUM_KEYS {
        let (_, count) = final_results
            .get(&key)
            .unwrap_or_else(|| panic!("key {} is missing from results", key));
        assert_eq!(
            *count, RECORDS_PER_KEY,
            "key {} lost data: expected {}, got {}",
            key, RECORDS_PER_KEY, count
        );
    }
}

/// Chained map + filter + key_by + reduce.
/// Verifies multi-step stateless transformations compose correctly with stateful reduce.
#[test]
fn test_map_filter_chain() {
    let env = StreamExecutionEnvironment::new("map-filter-chain");

    // Input: raw numbers
    let numbers: Vec<i32> = (1..=10).collect();

    let results = env
        .from_iter(numbers)
        // map: x -> (x, x * x)
        .map(|x: &i32| (*x, x * x))
        // filter: keep only pairs where square > 25 (i.e., x >= 6)
        .filter(|(_, sq): &(i32, i32)| *sq > 25)
        // key_by: group by original value (each key appears once)
        .key_by(|(x, _): &(i32, i32)| *x)
        // reduce: sum of squares per key (trivial here, just validates the pipeline)
        .reduce(|(k, sq1), (_, sq2)| (k, sq1 + sq2))
        .execute_with_parallelism(2)
        .unwrap();

    let final_results = results.lock().unwrap();
    // x in [6, 7, 8, 9, 10] have square > 25
    assert_eq!(final_results.get(&6), Some(&(6, 36)));
    assert_eq!(final_results.get(&7), Some(&(7, 49)));
    assert_eq!(final_results.get(&8), Some(&(8, 64)));
    assert_eq!(final_results.get(&9), Some(&(9, 81)));
    assert_eq!(final_results.get(&10), Some(&(10, 100)));
    // x in [1..5] were filtered out
    assert_eq!(final_results.get(&1), None);
    assert_eq!(final_results.get(&5), None);
    assert_eq!(final_results.len(), 5);
}
