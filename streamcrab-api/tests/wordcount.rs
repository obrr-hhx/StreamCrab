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
