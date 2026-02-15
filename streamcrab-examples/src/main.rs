use streamcrab_api::environment::StreamExecutionEnvironment;

fn main() -> anyhow::Result<()> {
    let env = StreamExecutionEnvironment::new("wordcount");

    let lines = vec![
        "hello world".to_string(),
        "hello streamcrab".to_string(),
        "world of streams".to_string(),
    ];

    env.from_iter(lines)
        .flat_map(|line: String| {
            line.split_whitespace()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        })
        .map(|word: String| (word, 1i32))
        .key_by::<String, _>(|(word, _)| word.clone())
        .reduce(|(w1, c1), (_, c2)| (w1, c1 + c2))
        .print();

    env.execute()?;

    Ok(())
}
