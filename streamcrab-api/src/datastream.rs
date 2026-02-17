use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use streamcrab_core::types::StreamData;

/// A stream of elements of type `T`.
///
/// Created by [`StreamExecutionEnvironment::from_iter`].
/// Call [`key_by`](Self::key_by) to partition by key.
pub struct DataStream<T>
where
    T: StreamData + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
}

impl<T> DataStream<T>
where
    T: StreamData + Send + std::fmt::Debug + 'static,
{
    /// Partition the stream by key, returning a [`KeyedStream`].
    pub fn key_by<K, F>(self, key_fn: F) -> KeyedStream<K, T, F>
    where
        K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
        F: Fn(&T) -> K + Send + Sync + Clone + 'static,
    {
        KeyedStream {
            source_data: self.source_data,
            key_fn,
            _phantom: PhantomData,
        }
    }
}

/// A keyed stream that supports stateful operations like [`reduce`](Self::reduce).
///
/// Created by calling [`DataStream::key_by`]. Elements with the same key are
/// grouped together for aggregation.
pub struct KeyedStream<K, T, KeyFn>
where
    K: StreamData + Send + 'static,
    T: StreamData + Send + 'static,
    KeyFn: Fn(&T) -> K + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
    pub(crate) key_fn: KeyFn,
    pub(crate) _phantom: PhantomData<K>,
}

impl<K, T, KeyFn> KeyedStream<K, T, KeyFn>
where
    K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
    T: StreamData + Send + std::fmt::Debug + 'static,
    KeyFn: Fn(&T) -> K + Send + Sync + Clone + 'static,
{
    /// Reduce elements with the same key by repeatedly applying `reduce_fn` to the
    /// accumulated value and each new element.
    ///
    /// Returns a [`ReduceJob`] that can be executed with [`execute_with_parallelism`](ReduceJob::execute_with_parallelism).
    pub fn reduce<ReduceFn>(self, reduce_fn: ReduceFn) -> ReduceJob<K, T, KeyFn, ReduceFn>
    where
        ReduceFn: Fn(T, T) -> T + Send + Sync + Clone + 'static,
    {
        ReduceJob {
            source_data: self.source_data,
            key_fn: self.key_fn,
            reduce_fn,
            _phantom: PhantomData,
        }
    }
}

/// A job that performs keyed reduce aggregation.
///
/// Created by calling [`KeyedStream::reduce`].
/// Execute with [`execute_with_parallelism`](Self::execute_with_parallelism).
pub struct ReduceJob<K, T, KeyFn, ReduceFn>
where
    K: StreamData + Send + 'static,
    T: StreamData + Send + 'static,
    KeyFn: Fn(&T) -> K + Send + 'static,
    ReduceFn: Fn(T, T) -> T + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
    pub(crate) key_fn: KeyFn,
    pub(crate) reduce_fn: ReduceFn,
    pub(crate) _phantom: PhantomData<K>,
}

impl<K, T, KeyFn, ReduceFn> ReduceJob<K, T, KeyFn, ReduceFn>
where
    K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
    T: StreamData + Send + std::fmt::Debug + Clone + 'static,
    KeyFn: Fn(&T) -> K + Send + Sync + Clone + 'static,
    ReduceFn: Fn(T, T) -> T + Send + Sync + Clone + 'static,
{
    /// Execute the job with the specified parallelism.
    ///
    /// Returns the aggregated results as a HashMap.
    ///
    /// # Architecture
    ///
    /// Creates a pipeline:
    /// ```text
    /// Source Task (1 thread)
    ///     |
    ///     | Hash Partition (by key)
    ///     v
    /// Reduce Tasks (parallelism threads)
    ///     |
    ///     v
    /// Collector Task (1 thread)
    /// ```
    pub fn execute_with_parallelism(
        self,
        parallelism: usize,
    ) -> anyhow::Result<Arc<Mutex<std::collections::HashMap<K, T>>>> {
        use std::thread;
        use streamcrab_core::channel::local_channel;
        use streamcrab_core::operator_chain::Operator;
        use streamcrab_core::partitioner::{HashPartitioner, Partitioner};
        use streamcrab_core::process::{ReduceFunction, ReduceOperator};
        use streamcrab_core::state::HashMapStateBackend;
        use streamcrab_core::types::StreamElement;

        let buffer_size = 1024;
        let results = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let results_clone = Arc::clone(&results);

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

        // Spawn Source Task
        let source_senders: Vec<_> = source_to_reduce_channels
            .iter()
            .map(|(sender, _)| sender.clone())
            .collect();

        let source_receivers: Vec<_> = source_to_reduce_channels
            .into_iter()
            .map(|(_, receiver)| receiver)
            .collect();

        let source_data = self.source_data;
        let key_fn = self.key_fn.clone();

        let source_handle = thread::spawn(move || -> anyhow::Result<()> {
            let partitioner = HashPartitioner::new(key_fn.clone());

            for item in source_data {
                // Determine target partition
                let partition = partitioner.partition(&item, parallelism);

                // Send to target reduce task
                let element = StreamElement::record(item);
                source_senders[partition].send(element)?;
            }

            // Send End markers
            for sender in &source_senders {
                sender.send(StreamElement::End)?;
            }

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
            let reduce_fn = self.reduce_fn.clone();
            let key_fn = self.key_fn.clone();

            let handle = thread::spawn(move || -> anyhow::Result<()> {
                // Create reduce operator
                struct UserReducer<F> {
                    reduce_fn: F,
                }

                impl<T, F> ReduceFunction<T> for UserReducer<F>
                where
                    T: StreamData,
                    F: Fn(T, T) -> T + Send,
                {
                    fn reduce(&mut self, value1: T, value2: T) -> anyhow::Result<T> {
                        Ok((self.reduce_fn)(value1, value2))
                    }
                }

                let reducer = UserReducer { reduce_fn };
                let backend = HashMapStateBackend::new();
                let mut operator = ReduceOperator::new(reducer, backend);

                loop {
                    // Receive from source
                    let element = receiver.recv()?;

                    match element {
                        StreamElement::Record(record) => {
                            let item = record.value;
                            let key = key_fn(&item);

                            // Process batch of 1 (simplified for now)
                            let input = vec![(key, item)];
                            let mut output = Vec::new();
                            operator.process_batch(&input, &mut output)?;

                            // Send results to collector
                            for result in output {
                                let out_element = StreamElement::record(result);
                                sender.send(out_element)?;
                            }
                        }
                        StreamElement::End => {
                            // Forward End marker
                            sender.send(StreamElement::End)?;
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
        let collector_handle = thread::spawn(move || -> anyhow::Result<()> {
            let mut ended_count = 0;
            let total_tasks = collector_receivers.len();

            while ended_count < total_tasks {
                // Round-robin read from all reduce tasks
                for receiver in &collector_receivers {
                    match receiver.try_recv() {
                        Ok(Some(element)) => match element {
                            StreamElement::Record(record) => {
                                let (key, value): (K, T) = record.value;
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

            Ok(())
        });

        // Wait for all tasks to complete
        source_handle.join().unwrap()?;
        for handle in reduce_handles {
            handle.join().unwrap()?;
        }
        collector_handle.join().unwrap()?;

        Ok(results)
    }
}
