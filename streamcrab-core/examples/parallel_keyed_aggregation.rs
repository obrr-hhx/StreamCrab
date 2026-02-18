//! Parallel Keyed Aggregation Example (P1 Phase 7)
//!
//! This example demonstrates:
//! - Multi-threaded Task execution
//! - Hash partitioning by key
//! - Keyed state management with ReduceOperator
//! - Parallel aggregation with correct results
//!
//! Architecture:
//! ```
//! Source Task (parallelism=1)
//!     |
//!     | Hash Partition (by user_id)
//!     v
//! Reduce Tasks (parallelism=2)
//!     |
//!     v
//! Collector
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use streamcrab_core::channel::local_channel;
use streamcrab_core::operator_chain::Operator;
use streamcrab_core::partitioner::{HashPartitioner, Partitioner};
use streamcrab_core::process::{ReduceFunction, ReduceOperator};
use streamcrab_core::state::HashMapStateBackend;
use streamcrab_core::types::StreamElement;

use anyhow::Result;

/// Sum reducer for i32 values
struct SumReducer;

impl ReduceFunction<i32> for SumReducer {
    fn reduce(&mut self, value1: i32, value2: i32) -> Result<i32> {
        Ok(value1 + value2)
    }
}

/// Source operator that generates test data
struct SourceOperator {
    data: Vec<(String, i32)>,
    index: usize,
}

impl SourceOperator {
    fn new(data: Vec<(String, i32)>) -> Self {
        Self { data, index: 0 }
    }

    fn next(&mut self) -> Option<(String, i32)> {
        if self.index < self.data.len() {
            let item = self.data[self.index].clone();
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
}

fn main() -> Result<()> {
    println!("=== Parallel Keyed Aggregation Example ===\n");

    // Test data: (user_id, value)
    let test_data = vec![
        ("user_1".to_string(), 10),
        ("user_2".to_string(), 20),
        ("user_1".to_string(), 15),
        ("user_3".to_string(), 30),
        ("user_2".to_string(), 25),
        ("user_1".to_string(), 5),
        ("user_3".to_string(), 35),
        ("user_2".to_string(), 10),
    ];

    println!("Input data:");
    for (user, value) in &test_data {
        println!("  {} -> {}", user, value);
    }
    println!();

    // Expected results: user_1=30, user_2=55, user_3=65
    let expected: HashMap<String, i32> = [
        ("user_1".to_string(), 30),
        ("user_2".to_string(), 55),
        ("user_3".to_string(), 65),
    ]
    .iter()
    .cloned()
    .collect();

    // Configuration
    let parallelism = 2;
    let buffer_size = 10;

    // Create channels: Source -> Reduce Tasks
    let mut source_to_reduce_channels = Vec::new();
    for _ in 0..parallelism {
        source_to_reduce_channels.push(local_channel(buffer_size));
    }

    // Create channels: Reduce Tasks -> Collector
    let mut reduce_to_collector_channels = Vec::new();
    for _ in 0..parallelism {
        reduce_to_collector_channels.push(local_channel(buffer_size));
    }

    // Shared result collector
    let results = Arc::new(Mutex::new(HashMap::new()));

    // Clone for threads
    let results_clone = Arc::clone(&results);

    // Spawn Source Task
    let source_senders: Vec<_> = source_to_reduce_channels
        .iter()
        .map(|(sender, _)| sender.clone())
        .collect();

    let source_receivers: Vec<_> = source_to_reduce_channels
        .into_iter()
        .map(|(_, receiver)| receiver)
        .collect();

    let source_handle = thread::spawn(move || -> Result<()> {
        let mut source = SourceOperator::new(test_data);
        let partitioner = HashPartitioner::new(|key: &(String, i32)| key.0.clone());

        while let Some((key, value)) = source.next() {
            // Determine target partition
            let partition = partitioner.partition(&(key.clone(), value), parallelism);

            // Send to target reduce task
            let element = StreamElement::record((key, value));
            source_senders[partition].send(element)?;
        }

        // Send End markers
        for sender in &source_senders {
            sender.send(StreamElement::End)?;
        }

        println!("[Source] Finished sending {} records", 8);
        Ok(())
    });

    // Spawn Reduce Tasks
    let mut reduce_handles = Vec::new();

    let collector_senders: Vec<_> = reduce_to_collector_channels
        .iter()
        .map(|(sender, _)| sender.clone())
        .collect();

    let collector_receivers: Vec<_> = reduce_to_collector_channels
        .into_iter()
        .map(|(_, receiver)| receiver)
        .collect();

    let mut source_receivers_iter = source_receivers.into_iter();

    for task_id in 0..parallelism {
        let receiver = source_receivers_iter.next().unwrap();
        let sender = collector_senders[task_id].clone();

        let handle = thread::spawn(move || -> Result<()> {
            // Create reduce operator
            let reducer = SumReducer;
            let backend = HashMapStateBackend::new();
            let mut operator = ReduceOperator::new(reducer, backend);

            let mut processed = 0;

            loop {
                // Receive from source
                let element = receiver.recv()?;

                match element {
                    StreamElement::Record(record) => {
                        // Process batch of 1 (simplified for example)
                        let input = vec![record.value];
                        let mut output = Vec::new();
                        operator.process_batch(&input, &mut output)?;

                        // Send results to collector
                        for result in output {
                            let out_element = StreamElement::record(result);
                            sender.send(out_element)?;
                        }

                        processed += 1;
                    }
                    StreamElement::End => {
                        // Forward End marker
                        sender.send(StreamElement::End)?;
                        println!(
                            "[Reduce-{}] Finished processing {} records",
                            task_id, processed
                        );
                        break;
                    }
                    _ => {}
                }
            }

            Ok(())
        });

        reduce_handles.push(handle);
    }

    // Spawn Collector Task
    let collector_handle = thread::spawn(move || -> Result<()> {
        let mut ended_count = 0;
        let total_tasks = collector_receivers.len();

        while ended_count < total_tasks {
            // Round-robin read from all reduce tasks
            for receiver in &collector_receivers {
                match receiver.try_recv() {
                    Ok(Some(element)) => match element {
                        StreamElement::Record(record) => {
                            let (key, value): (String, i32) = record.value;
                            results_clone.lock().unwrap().insert(key, value);
                        }
                        StreamElement::End => {
                            ended_count += 1;
                        }
                        _ => {}
                    },
                    Ok(None) | Err(_) => {
                        // No data available, continue
                    }
                }
            }

            // Small sleep to avoid busy waiting
            thread::sleep(std::time::Duration::from_micros(100));
        }

        println!("[Collector] Finished collecting results");
        Ok(())
    });

    // Wait for all tasks to complete
    source_handle.join().unwrap()?;
    for handle in reduce_handles {
        handle.join().unwrap()?;
    }
    collector_handle.join().unwrap()?;

    // Verify results
    println!("\n=== Results ===");
    let final_results = results.lock().unwrap();

    let mut keys: Vec<_> = final_results.keys().collect();
    keys.sort();

    for key in keys {
        let value = final_results[key];
        let expected_value = expected[key];
        let status = if value == expected_value {
            "✓"
        } else {
            "✗"
        };
        println!(
            "  {} {} -> {} (expected: {})",
            status, key, value, expected_value
        );
    }

    // Validate
    println!("\n=== Validation ===");
    let mut all_correct = true;
    for (key, expected_value) in &expected {
        match final_results.get(key) {
            Some(&actual_value) if actual_value == *expected_value => {
                println!("  ✓ {} correct", key);
            }
            Some(&actual_value) => {
                println!(
                    "  ✗ {} incorrect: got {}, expected {}",
                    key, actual_value, expected_value
                );
                all_correct = false;
            }
            None => {
                println!("  ✗ {} missing", key);
                all_correct = false;
            }
        }
    }

    if all_correct {
        println!("\nAll results correct! Parallel keyed aggregation works!");
    } else {
        println!("\nSome results incorrect!");
        return Err(anyhow::anyhow!("Validation failed"));
    }

    Ok(())
}
