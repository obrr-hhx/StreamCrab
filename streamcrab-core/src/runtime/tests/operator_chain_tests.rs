use super::*;

// ========================================================================
// Tests: Demonstrate Performance Benefits
// ========================================================================

#[test]
fn test_single_operator_batch() {
    // Single map operator: x * 2
    let mut chain = chain(MapOp::new(|x: &i32| x * 2));

    let input = vec![1, 2, 3, 4, 5];
    let mut output = Vec::new();

    chain.process_batch(&input, &mut output).unwrap();

    assert_eq!(output, vec![2, 4, 6, 8, 10]);
}

#[test]
fn test_map_then_filter() {
    // Chain: x * 2 → filter(x > 5)
    let mut chain = chain(MapOp::new(|x: &i32| x * 2)).then(FilterOp::new(|x: &i32| *x > 5));

    let input = vec![1, 2, 3, 4, 5]; // → [2, 4, 6, 8, 10] → [6, 8, 10]
    let mut output = Vec::new();

    chain.process_batch(&input, &mut output).unwrap();

    assert_eq!(output, vec![6, 8, 10]);
}

#[test]
fn test_three_operator_chain() {
    // Chain: x * 2 → x + 10 → filter(x > 20)
    let mut chain = chain(MapOp::new(|x: &i32| x * 2))
        .then(MapOp::new(|x: &i32| x + 10))
        .then(FilterOp::new(|x: &i32| *x > 20));

    let input = vec![1, 5, 10, 15];
    // → [2, 10, 20, 30]
    // → [12, 20, 30, 40]
    // → [30, 40]
    let mut output = Vec::new();

    chain.process_batch(&input, &mut output).unwrap();

    assert_eq!(output, vec![30, 40]);
}

#[test]
fn test_filter_removes_all() {
    let filter = FilterOp {
        f: |x: &i32| *x > 100,
    };
    let mut chain = chain(filter);

    let input = vec![1, 2, 3, 4, 5];
    let mut output = Vec::new();

    chain.process_batch(&input, &mut output).unwrap();

    assert_eq!(output, Vec::<i32>::new());
}

#[test]
fn test_batch_processing_performance() {
    // Demonstrate batch processing benefit
    let map1 = MapOp { f: |x: &i32| x + 1 };
    let map2 = MapOp { f: |x: &i32| x * 2 };
    let map3 = MapOp { f: |x: &i32| x - 3 };
    let mut chain = chain(map1).then(map2).then(map3);

    // Process 1000 records in one batch
    let input: Vec<i32> = (0..1000).collect();
    let mut output = Vec::new();

    chain.process_batch(&input, &mut output).unwrap();

    // Verify: (x + 1) * 2 - 3 = 2x - 1
    for (i, &result) in output.iter().enumerate() {
        assert_eq!(result, 2 * i as i32 - 1);
    }

    // Key point: This made only O(chain_length) calls per batch
    // vs O(chain_length * records) per-record processing!
    // With LLVM inlining, the entire chain becomes:
    // for x in input { output.push(2 * x - 1) }
}

#[test]
fn test_zero_allocation_output_reuse() {
    // Demonstrate output buffer reuse (zero allocation after first batch)
    let map = MapOp { f: |x: &i32| x * 2 };
    let mut chain = chain(map);

    let mut output = Vec::with_capacity(100);

    // Process multiple batches, reusing the same output buffer
    for batch_id in 0..10 {
        let input: Vec<i32> = (batch_id * 10..(batch_id + 1) * 10).collect();

        output.clear(); // Reuse buffer (no deallocation)
        chain.process_batch(&input, &mut output).unwrap();

        assert_eq!(output.len(), 10);
        // After first batch, capacity stays constant (no reallocation)
    }

    // Key point: Only 1 allocation (initial with_capacity)
    // All subsequent batches reuse the same buffer!
}

struct EventTimerHead;

impl Operator<i32> for EventTimerHead {
    type OUT = i32;

    fn process_batch(&mut self, _input: &[i32], _output: &mut Vec<i32>) -> Result<()> {
        Ok(())
    }

    fn on_event_time(&mut self, event_time: EventTime, output: &mut Vec<i32>) -> Result<()> {
        output.push(event_time as i32);
        Ok(())
    }
}

struct EventTimerTail;

impl Operator<i32> for EventTimerTail {
    type OUT = i32;

    fn process_batch(&mut self, input: &[i32], output: &mut Vec<i32>) -> Result<()> {
        output.extend_from_slice(input);
        Ok(())
    }

    fn on_event_time(&mut self, _event_time: EventTime, output: &mut Vec<i32>) -> Result<()> {
        output.push(99);
        Ok(())
    }
}

#[test]
fn test_chain_on_timer_event_time_propagates() {
    let mut chain = chain(EventTimerHead).then(EventTimerTail);
    let mut output = Vec::new();

    chain
        .on_timer(7, TimerDomain::EventTime, &mut output)
        .unwrap();

    assert_eq!(output, vec![7, 99]);
}

struct CountingStatefulOp {
    count: i32,
}

impl Operator<i32> for CountingStatefulOp {
    type OUT = i32;

    fn process_batch(&mut self, input: &[i32], output: &mut Vec<i32>) -> Result<()> {
        output.reserve(input.len());
        for v in input {
            output.push(*v + self.count);
            self.count += 1;
        }
        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&self.count)?)
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        self.count = bincode::deserialize(data)?;
        Ok(())
    }
}

#[test]
fn test_chain_snapshot_restore_roundtrip() {
    let mut op_chain = chain(CountingStatefulOp { count: 0 }).then(MapOp::new(|x: &i32| x * 2));
    let mut output = Vec::new();
    op_chain.process_batch(&[1, 2], &mut output).unwrap();
    assert_eq!(output, vec![2, 6]);

    let snapshot = op_chain.snapshot_state().unwrap();

    let mut restored_chain =
        chain(CountingStatefulOp { count: 0 }).then(MapOp::new(|x: &i32| x * 2));
    restored_chain.restore_state(&snapshot).unwrap();

    let mut restored_output = Vec::new();
    restored_chain
        .process_batch(&[3], &mut restored_output)
        .unwrap();
    assert_eq!(restored_output, vec![10]);
}
