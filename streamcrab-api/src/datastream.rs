use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use streamcrab_core::operator_chain::{Append, ChainEnd, FilterOp, FlatMapOp, MapOp, Operator};
use streamcrab_core::types::StreamData;

/// A stream of elements of type `T` with an operator chain of type `OpChain`.
///
/// Created by [`StreamExecutionEnvironment::from_iter`].
///
/// The `OpChain` type parameter tracks the chain of transformations applied to the stream.
/// This enables zero-cost abstraction: all operations are statically dispatched and can be
/// inlined by LLVM.
pub struct DataStream<T, OpChain = ChainEnd>
where
    T: StreamData + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
    pub(crate) op_chain: OpChain,
}

impl<T, OpChain> DataStream<T, OpChain>
where
    T: StreamData + Send + Clone + 'static,
    OpChain: Operator<T> + Send + 'static,
    OpChain::OUT: StreamData + Send + Clone + 'static,
{
    /// Apply a transformation to each element.
    ///
    /// Uses `Append` to extend the existing operator chain, enabling chaining:
    /// `stream.map(f1).map(f2)` becomes `Chain<Op1, Chain<Op2, ChainEnd>>`.
    pub fn map<U, F>(self, f: F) -> DataStream<T, <OpChain as Append<MapOp<F>>>::Result>
    where
        U: StreamData + Send + Clone + 'static,
        F: FnMut(&OpChain::OUT) -> U + Send + 'static,
        OpChain: Append<MapOp<F>>,
    {
        DataStream {
            source_data: self.source_data,
            op_chain: self.op_chain.append(MapOp::new(f)),
        }
    }

    /// Filter elements based on a predicate.
    ///
    /// Uses `Append` to extend the existing operator chain.
    pub fn filter<F>(self, f: F) -> DataStream<T, <OpChain as Append<FilterOp<F>>>::Result>
    where
        F: FnMut(&OpChain::OUT) -> bool + Send + 'static,
        OpChain::OUT: Clone,
        OpChain: Append<FilterOp<F>>,
    {
        DataStream {
            source_data: self.source_data,
            op_chain: self.op_chain.append(FilterOp::new(f)),
        }
    }

    /// Transform each element into multiple elements.
    ///
    /// Uses `Append` to extend the existing operator chain.
    pub fn flat_map<U, I, F>(self, f: F) -> DataStream<T, <OpChain as Append<FlatMapOp<F>>>::Result>
    where
        U: StreamData + Send + Clone + 'static,
        I: IntoIterator<Item = U>,
        F: FnMut(&OpChain::OUT) -> I + Send + 'static,
        OpChain: Append<FlatMapOp<F>>,
    {
        DataStream {
            source_data: self.source_data,
            op_chain: self.op_chain.append(FlatMapOp::new(f)),
        }
    }

    /// Partition the stream by key, returning a [`KeyedStream`].
    pub fn key_by<K, F>(self, key_fn: F) -> KeyedStream<K, T, OpChain, F>
    where
        K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
        F: Fn(&OpChain::OUT) -> K + Send + Sync + Clone + 'static,
    {
        KeyedStream {
            source_data: self.source_data,
            op_chain: self.op_chain,
            key_fn,
            _phantom: PhantomData,
        }
    }
}

/// A keyed stream that supports stateful operations like [`reduce`](Self::reduce).
///
/// Created by calling [`DataStream::key_by`]. Elements with the same key are
/// grouped together for aggregation.
pub struct KeyedStream<K, T, OpChain, KeyFn>
where
    K: StreamData + Send + 'static,
    T: StreamData + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
    pub(crate) op_chain: OpChain,
    pub(crate) key_fn: KeyFn,
    pub(crate) _phantom: PhantomData<K>,
}

impl<K, T, OpChain, KeyFn> KeyedStream<K, T, OpChain, KeyFn>
where
    K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
    T: StreamData + Send + Clone + 'static,
    OpChain: Operator<T> + Send + 'static,
    OpChain::OUT: StreamData + Send + Clone + 'static,
    KeyFn: Fn(&OpChain::OUT) -> K + Send + Sync + Clone + 'static,
{
    /// Reduce elements with the same key by repeatedly applying `reduce_fn` to the
    /// accumulated value and each new element.
    ///
    /// Returns a [`ReduceJob`] that can be executed with [`execute_with_parallelism`](ReduceJob::execute_with_parallelism).
    pub fn reduce<ReduceFn>(self, reduce_fn: ReduceFn) -> ReduceJob<K, T, OpChain, KeyFn, ReduceFn>
    where
        ReduceFn: Fn(OpChain::OUT, OpChain::OUT) -> OpChain::OUT + Send + Sync + Clone + 'static,
    {
        ReduceJob {
            source_data: self.source_data,
            op_chain: self.op_chain,
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
pub struct ReduceJob<K, T, OpChain, KeyFn, ReduceFn>
where
    K: StreamData + Send + 'static,
    T: StreamData + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
    pub(crate) op_chain: OpChain,
    pub(crate) key_fn: KeyFn,
    pub(crate) reduce_fn: ReduceFn,
    pub(crate) _phantom: PhantomData<K>,
}

impl<K, T, OpChain, KeyFn, ReduceFn> ReduceJob<K, T, OpChain, KeyFn, ReduceFn>
where
    K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
    T: StreamData + Send + std::fmt::Debug + Clone + 'static,
    OpChain: Operator<T> + Send + 'static,
    OpChain::OUT: StreamData + Send + std::fmt::Debug + Clone + 'static,
    KeyFn: Fn(&OpChain::OUT) -> K + Send + Sync + Clone + 'static,
    ReduceFn: Fn(OpChain::OUT, OpChain::OUT) -> OpChain::OUT + Send + Sync + Clone + 'static,
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
    ///     | Apply OpChain
    ///     | Hash Partition (by key)
    ///     v
    /// Reduce Tasks (parallelism threads)
    ///     |
    ///     v
    /// Collector Task (1 thread)
    /// ```
    pub fn execute_with_parallelism(
        mut self,
        parallelism: usize,
    ) -> anyhow::Result<Arc<Mutex<std::collections::HashMap<K, OpChain::OUT>>>> {
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

        // Step 1: Apply operator chain to transform source data
        let mut transformed_data = Vec::new();
        self.op_chain.process_batch(&self.source_data, &mut transformed_data)?;

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

        let key_fn = self.key_fn.clone();

        let source_handle = thread::spawn(move || -> anyhow::Result<()> {
            let partitioner = HashPartitioner::new(key_fn.clone());

            for item in transformed_data {
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
                                let (key, value) = record.value;
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
