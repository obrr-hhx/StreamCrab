use std::collections::HashMap;

use streamcrab_api::environment::StreamExecutionEnvironment;

#[test]
fn test_wordcount() {
    let env = StreamExecutionEnvironment::new("wordcount");

    let lines = vec![
        "hello world".to_string(),
        "hello streamcrab".to_string(),
        "world of streams".to_string(),
    ];

    let results = env
        .from_iter(lines)
        .flat_map(|line: String| {
            line.split_whitespace()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        })
        .map(|word: String| (word, 1i32))
        .key_by::<String, _>(|(word, _)| word.clone())
        .reduce(|(w1, c1), (_, c2)| (w1, c1 + c2))
        .collect();

    env.execute().unwrap();

    // The reduce emits on every update, so we get intermediate results.
    // The final count for each word is the last emitted value.
    let output = results.borrow();
    let mut final_counts: HashMap<String, i32> = HashMap::new();
    for (word, count) in output.iter() {
        final_counts.insert(word.clone(), *count);
    }

    assert_eq!(final_counts.get("hello"), Some(&2));
    assert_eq!(final_counts.get("world"), Some(&2));
    assert_eq!(final_counts.get("streamcrab"), Some(&1));
    assert_eq!(final_counts.get("of"), Some(&1));
    assert_eq!(final_counts.get("streams"), Some(&1));
    assert_eq!(final_counts.len(), 5);
}

#[test]
fn test_map_filter_pipeline() {
    let env = StreamExecutionEnvironment::new("map-filter");

    let results = env
        .from_iter(1..=10)
        .filter(|x: &i32| x % 2 == 0)
        .map(|x: i32| x * 10)
        .collect();

    env.execute().unwrap();

    let output = results.borrow();
    assert_eq!(*output, vec![20, 40, 60, 80, 100]);
}

#[test]
fn test_flat_map() {
    let env = StreamExecutionEnvironment::new("flat-map");

    let results = env
        .from_iter(vec![1, 2, 3])
        .flat_map(|x: i32| vec![x, x * 10])
        .collect();

    env.execute().unwrap();

    let output = results.borrow();
    assert_eq!(*output, vec![1, 10, 2, 20, 3, 30]);
}
