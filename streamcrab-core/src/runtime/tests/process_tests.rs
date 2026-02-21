use super::*;
use crate::state::{HashMapStateBackend, ValueStateHandle};

/// Example: Count aggregator that counts elements per key
struct CountAggregator {
    count_state: ValueStateHandle<i64>,
}

impl CountAggregator {
    fn new() -> Self {
        Self {
            count_state: ValueStateHandle::new("count"),
        }
    }
}

impl KeyedProcessFunction<String, i32, (String, i64), HashMapStateBackend> for CountAggregator {
    fn process_element(
        &mut self,
        key: &String,
        _value: i32,
        ctx: &mut ProcessContext<(String, i64), HashMapStateBackend>,
    ) -> Result<()> {
        // Get current count
        let count = self.count_state.get(ctx.state())?.unwrap_or(0);
        let new_count = count + 1;

        // Update state
        self.count_state.put(ctx.state_mut(), new_count)?;

        // Emit result
        ctx.collect((key.clone(), new_count));

        Ok(())
    }
}

#[test]
fn test_process_operator_single_key() {
    let aggregator = CountAggregator::new();
    let backend = HashMapStateBackend::new();
    let mut operator = ProcessOperator::new(aggregator, backend);

    // Process batch with same key
    let input = vec![
        ("user_1".to_string(), 10),
        ("user_1".to_string(), 20),
        ("user_1".to_string(), 30),
    ];

    let mut output = Vec::new();
    operator.process_batch(&input, &mut output).unwrap();

    // Should emit count after each element
    assert_eq!(output.len(), 3);
    assert_eq!(output[0], ("user_1".to_string(), 1));
    assert_eq!(output[1], ("user_1".to_string(), 2));
    assert_eq!(output[2], ("user_1".to_string(), 3));
}

#[test]
fn test_process_operator_multiple_keys() {
    let aggregator = CountAggregator::new();
    let backend = HashMapStateBackend::new();
    let mut operator = ProcessOperator::new(aggregator, backend);

    // Process batch with different keys
    let input = vec![
        ("user_1".to_string(), 10),
        ("user_2".to_string(), 20),
        ("user_1".to_string(), 30),
        ("user_2".to_string(), 40),
        ("user_3".to_string(), 50),
    ];

    let mut output = Vec::new();
    operator.process_batch(&input, &mut output).unwrap();

    // Each key maintains separate count
    assert_eq!(output.len(), 5);
    assert_eq!(output[0], ("user_1".to_string(), 1));
    assert_eq!(output[1], ("user_2".to_string(), 1));
    assert_eq!(output[2], ("user_1".to_string(), 2));
    assert_eq!(output[3], ("user_2".to_string(), 2));
    assert_eq!(output[4], ("user_3".to_string(), 1));
}

#[test]
fn test_process_operator_state_persistence() {
    let aggregator = CountAggregator::new();
    let backend = HashMapStateBackend::new();
    let mut operator = ProcessOperator::new(aggregator, backend);

    // First batch
    let input1 = vec![("user_1".to_string(), 10), ("user_2".to_string(), 20)];
    let mut output1 = Vec::new();
    operator.process_batch(&input1, &mut output1).unwrap();

    assert_eq!(output1[0], ("user_1".to_string(), 1));
    assert_eq!(output1[1], ("user_2".to_string(), 1));

    // Second batch (state should persist)
    let input2 = vec![("user_1".to_string(), 30), ("user_2".to_string(), 40)];
    let mut output2 = Vec::new();
    operator.process_batch(&input2, &mut output2).unwrap();

    // Counts should continue from previous batch
    assert_eq!(output2[0], ("user_1".to_string(), 2));
    assert_eq!(output2[1], ("user_2".to_string(), 2));
}

/// Example: Sum aggregator using multiple state handles
struct SumAggregator {
    sum_state: ValueStateHandle<i64>,
    count_state: ValueStateHandle<i64>,
}

impl SumAggregator {
    fn new() -> Self {
        Self {
            sum_state: ValueStateHandle::new("sum"),
            count_state: ValueStateHandle::new("count"),
        }
    }
}

impl KeyedProcessFunction<String, i32, (String, i64, i64), HashMapStateBackend> for SumAggregator {
    fn process_element(
        &mut self,
        key: &String,
        value: i32,
        ctx: &mut ProcessContext<(String, i64, i64), HashMapStateBackend>,
    ) -> Result<()> {
        // Get current sum and count
        let sum = self.sum_state.get(ctx.state())?.unwrap_or(0);
        let count = self.count_state.get(ctx.state())?.unwrap_or(0);

        // Update state
        let new_sum = sum + value as i64;
        let new_count = count + 1;
        self.sum_state.put(ctx.state_mut(), new_sum)?;
        self.count_state.put(ctx.state_mut(), new_count)?;

        // Emit (key, sum, count)
        ctx.collect((key.clone(), new_sum, new_count));

        Ok(())
    }
}

#[test]
fn test_process_operator_multiple_states() {
    let aggregator = SumAggregator::new();
    let backend = HashMapStateBackend::new();
    let mut operator = ProcessOperator::new(aggregator, backend);

    let input = vec![
        ("user_1".to_string(), 10),
        ("user_1".to_string(), 20),
        ("user_1".to_string(), 30),
    ];

    let mut output = Vec::new();
    operator.process_batch(&input, &mut output).unwrap();

    // Should track both sum and count
    assert_eq!(output.len(), 3);
    assert_eq!(output[0], ("user_1".to_string(), 10, 1));
    assert_eq!(output[1], ("user_1".to_string(), 30, 2));
    assert_eq!(output[2], ("user_1".to_string(), 60, 3));
}

// ========== ReduceFunction Tests ==========

/// Example: Sum reducer
struct SumReducer;

impl ReduceFunction<i32> for SumReducer {
    fn reduce(&mut self, value1: i32, value2: i32) -> Result<i32> {
        Ok(value1 + value2)
    }
}

#[test]
fn test_reduce_operator_single_key() {
    let reducer = SumReducer;
    let backend = HashMapStateBackend::new();
    let mut operator = ReduceOperator::new(reducer, backend);

    let input = vec![
        ("user_1".to_string(), 10),
        ("user_1".to_string(), 20),
        ("user_1".to_string(), 30),
    ];

    let mut output = Vec::new();
    operator.process_batch(&input, &mut output).unwrap();

    // Should emit cumulative sum after each element
    assert_eq!(output.len(), 3);
    assert_eq!(output[0], ("user_1".to_string(), 10)); // First value
    assert_eq!(output[1], ("user_1".to_string(), 30)); // 10 + 20
    assert_eq!(output[2], ("user_1".to_string(), 60)); // 30 + 30
}

#[test]
fn test_reduce_operator_multiple_keys() {
    let reducer = SumReducer;
    let backend = HashMapStateBackend::new();
    let mut operator = ReduceOperator::new(reducer, backend);

    let input = vec![
        ("user_1".to_string(), 10),
        ("user_2".to_string(), 100),
        ("user_1".to_string(), 20),
        ("user_2".to_string(), 200),
        ("user_3".to_string(), 1000),
    ];

    let mut output = Vec::new();
    operator.process_batch(&input, &mut output).unwrap();

    // Each key maintains separate sum
    assert_eq!(output.len(), 5);
    assert_eq!(output[0], ("user_1".to_string(), 10));
    assert_eq!(output[1], ("user_2".to_string(), 100));
    assert_eq!(output[2], ("user_1".to_string(), 30)); // 10 + 20
    assert_eq!(output[3], ("user_2".to_string(), 300)); // 100 + 200
    assert_eq!(output[4], ("user_3".to_string(), 1000));
}

#[test]
fn test_reduce_operator_state_persistence() {
    let reducer = SumReducer;
    let backend = HashMapStateBackend::new();
    let mut operator = ReduceOperator::new(reducer, backend);

    // First batch
    let input1 = vec![("user_1".to_string(), 10), ("user_2".to_string(), 100)];
    let mut output1 = Vec::new();
    operator.process_batch(&input1, &mut output1).unwrap();

    assert_eq!(output1[0], ("user_1".to_string(), 10));
    assert_eq!(output1[1], ("user_2".to_string(), 100));

    // Second batch (state should persist)
    let input2 = vec![("user_1".to_string(), 20), ("user_2".to_string(), 200)];
    let mut output2 = Vec::new();
    operator.process_batch(&input2, &mut output2).unwrap();

    // Sums should continue from previous batch
    assert_eq!(output2[0], ("user_1".to_string(), 30)); // 10 + 20
    assert_eq!(output2[1], ("user_2".to_string(), 300)); // 100 + 200
}

/// Example: Max reducer
struct MaxReducer;

impl ReduceFunction<i32> for MaxReducer {
    fn reduce(&mut self, value1: i32, value2: i32) -> Result<i32> {
        Ok(value1.max(value2))
    }
}

#[test]
fn test_reduce_operator_max() {
    let reducer = MaxReducer;
    let backend = HashMapStateBackend::new();
    let mut operator = ReduceOperator::new(reducer, backend);

    let input = vec![
        ("user_1".to_string(), 10),
        ("user_1".to_string(), 30),
        ("user_1".to_string(), 20), // Smaller than current max
        ("user_1".to_string(), 50),
    ];

    let mut output = Vec::new();
    operator.process_batch(&input, &mut output).unwrap();

    // Should track maximum value
    assert_eq!(output.len(), 4);
    assert_eq!(output[0], ("user_1".to_string(), 10));
    assert_eq!(output[1], ("user_1".to_string(), 30));
    assert_eq!(output[2], ("user_1".to_string(), 30)); // Max stays 30
    assert_eq!(output[3], ("user_1".to_string(), 50));
}

#[test]
fn test_process_operator_snapshot_restore() {
    let aggregator = CountAggregator::new();
    let backend = HashMapStateBackend::new();
    let mut operator = ProcessOperator::new(aggregator, backend);

    let mut output = Vec::new();
    operator
        .process_batch(&[("user_1".to_string(), 10)], &mut output)
        .unwrap();
    assert_eq!(output, vec![("user_1".to_string(), 1)]);

    let snapshot = operator.snapshot_state().unwrap();

    let aggregator = CountAggregator::new();
    let backend = HashMapStateBackend::new();
    let mut restored = ProcessOperator::new(aggregator, backend);
    restored.restore_state(&snapshot).unwrap();

    let mut out_after_restore = Vec::new();
    restored
        .process_batch(&[("user_1".to_string(), 20)], &mut out_after_restore)
        .unwrap();
    assert_eq!(out_after_restore, vec![("user_1".to_string(), 2)]);
}

#[test]
fn test_reduce_operator_snapshot_restore() {
    let reducer = SumReducer;
    let backend = HashMapStateBackend::new();
    let mut operator = ReduceOperator::new(reducer, backend);

    let mut output = Vec::new();
    operator
        .process_batch(&[("user_1".to_string(), 10)], &mut output)
        .unwrap();
    assert_eq!(output, vec![("user_1".to_string(), 10)]);

    let snapshot = operator.snapshot_state().unwrap();

    let reducer = SumReducer;
    let backend = HashMapStateBackend::new();
    let mut restored = ReduceOperator::new(reducer, backend);
    restored.restore_state(&snapshot).unwrap();

    let mut out_after_restore = Vec::new();
    restored
        .process_batch(&[("user_1".to_string(), 20)], &mut out_after_restore)
        .unwrap();
    assert_eq!(out_after_restore, vec![("user_1".to_string(), 30)]);
}
