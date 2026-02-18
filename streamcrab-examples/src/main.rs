use streamcrab_api::environment::StreamExecutionEnvironment;

fn main() -> anyhow::Result<()> {
    let env = StreamExecutionEnvironment::new("wordcount");

    let lines = vec![
        "hello world".to_string(),
        "hello streamcrab".to_string(),
        "world of streams".to_string(),
    ];

    let results = env
        .from_iter(lines)
        .flat_map(|line: &String| {
            line.split_whitespace()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        })
        .map(|word: &String| (word.clone(), 1i32))
        .key_by::<String, _>(|(word, _): &(String, i32)| word.clone())
        .reduce(|(w1, c1), (_, c2)| (w1, c1 + c2))
        .execute_with_parallelism(2)?;

    // Print results sorted by word for deterministic output.
    // The HashMap value is (word, count); extract count from the tuple.
    let mut output: Vec<_> = results
        .lock()
        .unwrap()
        .iter()
        .map(|(k, (_, count))| (k.clone(), *count))
        .collect();
    output.sort_by(|a, b| a.0.cmp(&b.0));
    for (word, count) in output {
        println!("{word}: {count}");
    }

    Ok(())
}
