use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use streamcrab_core::checkpoint::{
    CheckpointCoordinator, InMemoryCheckpointStorage, TaskCheckpointAck, TaskCheckpointEvent,
};
use streamcrab_core::operator_chain::{Append, ChainEnd, FilterOp, FlatMapOp, MapOp, Operator};
use streamcrab_core::task::{TaskId, VertexId};
use streamcrab_core::time::WatermarkStrategy;
use streamcrab_core::types::{CheckpointId, EventTime, StreamData};
use streamcrab_core::window::{TimeWindow, Trigger, WindowAssigner};

/// Parallel execution result with checkpoint completion details.
pub struct CheckpointedExecutionResult<R> {
    pub result: R,
    pub completed_checkpoints: Vec<CheckpointId>,
}

type SharedReduceResults<K, V> = Arc<Mutex<std::collections::HashMap<K, V>>>;

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
    /// Assign event-time timestamps and watermarks to this stream.
    ///
    /// This returns a wrapper stream that carries a [`WatermarkStrategy`].
    /// Windowing APIs use it to insert a timestamp-assigner execution stage
    /// and drive watermark generation.
    pub fn assign_timestamps_and_watermarks<Strategy>(
        self,
        strategy: Strategy,
    ) -> WatermarkedStream<T, OpChain, Strategy>
    where
        Strategy: WatermarkStrategy<OpChain::OUT> + 'static,
    {
        WatermarkedStream {
            source_data: self.source_data,
            op_chain: self.op_chain,
            strategy,
        }
    }

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

/// Timestamp-assigner stage used by window execution.
///
/// It is modelled as a dedicated execution-stage operator so timestamp extraction
/// and watermark generation are explicit in the pipeline.
struct TimestampAssignerOperator<Strategy> {
    strategy: Strategy,
}

impl<Strategy> TimestampAssignerOperator<Strategy> {
    fn new(strategy: Strategy) -> Self {
        Self { strategy }
    }

    fn extract_timestamp<T>(&self, element: &T) -> EventTime
    where
        Strategy: WatermarkStrategy<T>,
    {
        self.strategy.extract_timestamp(element)
    }

    fn create_watermark_generator<T>(&self) -> Box<dyn streamcrab_core::time::WatermarkGenerator>
    where
        Strategy: WatermarkStrategy<T>,
    {
        self.strategy.create_watermark_generator()
    }
}

/// A stream that has an event-time [`WatermarkStrategy`] assigned.
///
/// Created by calling [`DataStream::assign_timestamps_and_watermarks`].
pub struct WatermarkedStream<T, OpChain, Strategy>
where
    T: StreamData + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
    pub(crate) op_chain: OpChain,
    pub(crate) strategy: Strategy,
}

impl<T, OpChain, Strategy> WatermarkedStream<T, OpChain, Strategy>
where
    T: StreamData + Send + Clone + 'static,
    OpChain: Operator<T> + Send + 'static,
    OpChain::OUT: StreamData + Send + Clone + 'static,
    Strategy: WatermarkStrategy<OpChain::OUT> + 'static,
{
    /// Partition the stream by key, returning a keyed stream with event-time enabled.
    pub fn key_by<K, F>(self, key_fn: F) -> KeyedWatermarkedStream<K, T, OpChain, F, Strategy>
    where
        K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
        F: Fn(&OpChain::OUT) -> K + Send + Sync + Clone + 'static,
    {
        KeyedWatermarkedStream {
            source_data: self.source_data,
            op_chain: self.op_chain,
            key_fn,
            strategy: self.strategy,
            _phantom: PhantomData,
        }
    }
}

/// A keyed stream with an event-time watermark strategy.
pub struct KeyedWatermarkedStream<K, T, OpChain, KeyFn, Strategy>
where
    K: StreamData + Send + 'static,
    T: StreamData + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
    pub(crate) op_chain: OpChain,
    pub(crate) key_fn: KeyFn,
    pub(crate) strategy: Strategy,
    pub(crate) _phantom: PhantomData<K>,
}

impl<K, T, OpChain, KeyFn, Strategy> KeyedWatermarkedStream<K, T, OpChain, KeyFn, Strategy>
where
    K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
    T: StreamData + Send + Clone + 'static,
    OpChain: Operator<T> + Send + 'static,
    OpChain::OUT: StreamData + Send + Clone + 'static,
    KeyFn: Fn(&OpChain::OUT) -> K + Send + Sync + Clone + 'static,
    Strategy: WatermarkStrategy<OpChain::OUT> + 'static,
{
    /// Define event-time windows on this keyed stream.
    pub fn window<WA>(
        self,
        assigner: WA,
    ) -> WindowedStream<K, T, OpChain, KeyFn, Strategy, WA, WA::DefaultTrigger>
    where
        WA: WindowAssigner<OpChain::OUT> + 'static,
    {
        let trigger = assigner.default_trigger();
        WindowedStream {
            source_data: self.source_data,
            op_chain: self.op_chain,
            key_fn: self.key_fn,
            strategy: self.strategy,
            assigner,
            trigger,
            _phantom: PhantomData,
        }
    }
}

/// A keyed, watermarked stream with a window assigner attached.
pub struct WindowedStream<K, T, OpChain, KeyFn, Strategy, WA, Tr>
where
    K: StreamData + Send + 'static,
    T: StreamData + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
    pub(crate) op_chain: OpChain,
    pub(crate) key_fn: KeyFn,
    pub(crate) strategy: Strategy,
    pub(crate) assigner: WA,
    pub(crate) trigger: Tr,
    pub(crate) _phantom: PhantomData<K>,
}

impl<K, T, OpChain, KeyFn, Strategy, WA, Tr> WindowedStream<K, T, OpChain, KeyFn, Strategy, WA, Tr>
where
    K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
    T: StreamData + Send + Clone + 'static,
    OpChain: Operator<T> + Send + 'static,
    OpChain::OUT: StreamData + Send + Clone + 'static,
    KeyFn: Fn(&OpChain::OUT) -> K + Send + Sync + Clone + 'static,
    Strategy: WatermarkStrategy<OpChain::OUT> + 'static,
    WA: WindowAssigner<OpChain::OUT> + 'static,
    Tr: Trigger<OpChain::OUT, TimeWindow> + 'static,
{
    /// Override the default event-time trigger with a custom trigger.
    pub fn trigger<Tr2>(
        self,
        trigger: Tr2,
    ) -> WindowedStream<K, T, OpChain, KeyFn, Strategy, WA, Tr2>
    where
        Tr2: Trigger<OpChain::OUT, TimeWindow> + 'static,
    {
        WindowedStream {
            source_data: self.source_data,
            op_chain: self.op_chain,
            key_fn: self.key_fn,
            strategy: self.strategy,
            assigner: self.assigner,
            trigger,
            _phantom: PhantomData,
        }
    }

    /// Apply a custom [`streamcrab_core::window::WindowFunction`].
    pub fn apply<OUT, WFN>(
        self,
        window_fn: WFN,
    ) -> WindowJob<K, T, OpChain, KeyFn, Strategy, WA, Tr, OUT, WFN>
    where
        OUT: StreamData + Send + Clone + 'static,
        WFN: streamcrab_core::window::WindowFunction<K, OpChain::OUT, OUT> + Send + 'static,
    {
        WindowJob {
            source_data: self.source_data,
            op_chain: self.op_chain,
            key_fn: self.key_fn,
            strategy: self.strategy,
            assigner: self.assigner,
            trigger: self.trigger,
            window_fn,
            _phantom: PhantomData,
        }
    }

    /// Incrementally aggregate all elements within each (key, window).
    ///
    /// Unlike [`reduce`](Self::reduce), the accumulator type `ACC` can differ from the
    /// input type, and the final result type `OUT` can also differ (e.g. compute an
    /// average: `ACC = (sum, count)`, `OUT = f64`).
    ///
    /// The output element is `(K, OUT)` and the record timestamp is set
    /// to the window's `max_timestamp()`.
    #[allow(clippy::type_complexity)]
    pub fn aggregate<ACC, OUT, AGG>(
        self,
        agg: AGG,
    ) -> WindowJob<
        K,
        T,
        OpChain,
        KeyFn,
        Strategy,
        WA,
        Tr,
        (K, OUT),
        AggregateWindowFn<AGG, OpChain::OUT, ACC, OUT>,
    >
    where
        ACC: Send + 'static,
        OUT: streamcrab_core::types::StreamData + Send + Clone + 'static,
        AGG: streamcrab_core::window::AggregateFunction<OpChain::OUT, ACC, OUT> + Send + 'static,
    {
        WindowJob {
            source_data: self.source_data,
            op_chain: self.op_chain,
            key_fn: self.key_fn,
            strategy: self.strategy,
            assigner: self.assigner,
            trigger: self.trigger,
            window_fn: AggregateWindowFn {
                agg,
                _phantom: PhantomData,
            },
            _phantom: PhantomData,
        }
    }

    /// Reduce all elements within each (key, window) into a single value.
    ///
    /// The output element is `(K, OpChain::OUT)` and the record timestamp is set
    /// to the window's `max_timestamp()`.
    #[allow(clippy::type_complexity)]
    pub fn reduce<ReduceFn>(
        self,
        reduce_fn: ReduceFn,
    ) -> WindowJob<
        K,
        T,
        OpChain,
        KeyFn,
        Strategy,
        WA,
        Tr,
        (K, OpChain::OUT),
        ReduceWindowFn<K, OpChain::OUT, ReduceFn>,
    >
    where
        ReduceFn: Fn(OpChain::OUT, OpChain::OUT) -> OpChain::OUT + Send + Sync + Clone + 'static,
    {
        WindowJob {
            source_data: self.source_data,
            op_chain: self.op_chain,
            key_fn: self.key_fn,
            strategy: self.strategy,
            assigner: self.assigner,
            trigger: self.trigger,
            window_fn: ReduceWindowFn {
                reduce_fn,
                _phantom: PhantomData,
            },
            _phantom: PhantomData,
        }
    }
}

/// Executable windowed job.
pub struct WindowJob<K, T, OpChain, KeyFn, Strategy, WA, Tr, OUT, WFN>
where
    K: StreamData + Send + 'static,
    T: StreamData + Send + 'static,
    OUT: StreamData + Send + 'static,
{
    pub(crate) source_data: Vec<T>,
    pub(crate) op_chain: OpChain,
    pub(crate) key_fn: KeyFn,
    pub(crate) strategy: Strategy,
    pub(crate) assigner: WA,
    pub(crate) trigger: Tr,
    pub(crate) window_fn: WFN,
    pub(crate) _phantom: PhantomData<(K, OUT)>,
}

impl<K, T, OpChain, KeyFn, Strategy, WA, Tr, OUT, WFN>
    WindowJob<K, T, OpChain, KeyFn, Strategy, WA, Tr, OUT, WFN>
where
    K: StreamData + Send + Sync + std::fmt::Debug + std::hash::Hash + Eq + 'static,
    T: StreamData + Send + Clone + 'static,
    OpChain: Operator<T> + Send + 'static,
    OpChain::OUT: StreamData + Send + Clone + 'static,
    KeyFn: Fn(&OpChain::OUT) -> K + Send + Sync + Clone + 'static,
    Strategy: WatermarkStrategy<OpChain::OUT> + 'static,
    WA: WindowAssigner<OpChain::OUT> + 'static,
    Tr: Trigger<OpChain::OUT, TimeWindow> + 'static,
    OUT: StreamData + Send + Clone + 'static,
    WFN: streamcrab_core::window::WindowFunction<K, OpChain::OUT, OUT> + Send + 'static,
{
    /// Execute the windowed job in a single thread.
    ///
    /// This is sufficient for P2 correctness tests; parallel execution is added in later phases.
    pub fn execute(mut self) -> anyhow::Result<Vec<streamcrab_core::types::StreamRecord<OUT>>> {
        use std::time::{SystemTime, UNIX_EPOCH};
        use streamcrab_core::time::EVENT_TIME_MAX;
        use streamcrab_core::types::{StreamElement, StreamRecord, Watermark};
        use streamcrab_core::window::WindowOperator;

        // Step 1: Apply operator chain.
        let mut transformed: Vec<OpChain::OUT> = Vec::new();
        self.op_chain
            .process_batch(&self.source_data, &mut transformed)?;

        // Step 2: TimestampAssigner operator + watermark generator.
        let timestamp_assigner = TimestampAssignerOperator::new(self.strategy);
        let mut generator = timestamp_assigner.create_watermark_generator::<OpChain::OUT>();
        let mut last_emitted_wm = streamcrab_core::time::EVENT_TIME_MIN;

        let timestamp_assigner_ref = &timestamp_assigner;
        let timestamp_fn = move |e: &OpChain::OUT| timestamp_assigner_ref.extract_timestamp(e);

        let mut operator = WindowOperator::new(
            self.key_fn,
            timestamp_fn,
            self.assigner,
            self.trigger,
            self.window_fn,
        );

        let mut out_records: Vec<StreamRecord<OUT>> = Vec::new();
        let processing_time_ms = || -> i64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        };

        for item in transformed {
            let ts = timestamp_assigner.extract_timestamp(&item);

            // Drop late elements (v1: no allowed lateness).
            if ts <= last_emitted_wm {
                continue;
            }

            generator.on_event(ts);

            for elem in operator.process(StreamElement::timestamped_record(item, ts))? {
                if let StreamElement::Record(r) = elem {
                    out_records.push(r);
                }
            }

            if let Some(wm) = generator.current_watermark() {
                if wm.timestamp > last_emitted_wm {
                    last_emitted_wm = wm.timestamp;
                    for elem in operator.process(StreamElement::Watermark(wm))? {
                        if let StreamElement::Record(r) = elem {
                            out_records.push(r);
                        }
                    }
                }
            }

            for elem in operator.on_processing_time(processing_time_ms())? {
                if let StreamElement::Record(r) = elem {
                    out_records.push(r);
                }
            }
        }

        // Flush remaining windows.
        let final_wm = Watermark::new(EVENT_TIME_MAX);
        for elem in operator.process(StreamElement::Watermark(final_wm))? {
            if let StreamElement::Record(r) = elem {
                out_records.push(r);
            }
        }

        Ok(out_records)
    }

    /// Execute the windowed job with keyed hash partitioning and parallel window operators.
    ///
    /// Pipeline:
    /// ```text
    /// Source Task (1 thread)
    ///     | Apply OpChain
    ///     | Assign timestamps / generate watermarks
    ///     | Hash Partition (by key)
    ///     v
    /// Window Tasks (parallelism threads, one WindowOperator each)
    ///     |
    ///     v
    /// Collector (current thread)
    /// ```
    pub fn execute_with_parallelism(
        mut self,
        parallelism: usize,
    ) -> anyhow::Result<Vec<streamcrab_core::types::StreamRecord<OUT>>>
    where
        WA: Clone,
        Tr: Clone,
        WFN: Clone,
    {
        use std::thread;
        use streamcrab_core::channel::local_channel;
        use streamcrab_core::partitioner::{HashPartitioner, Partitioner};
        use streamcrab_core::time::EVENT_TIME_MAX;
        use streamcrab_core::types::{StreamElement, StreamRecord, Watermark};
        use streamcrab_core::window::WindowOperator;

        if parallelism == 0 {
            return Err(anyhow::anyhow!("parallelism must be greater than 0"));
        }

        let buffer_size = 1024;

        // Step 1: Apply operator chain to transform source data.
        let mut transformed_data: Vec<OpChain::OUT> = Vec::new();
        self.op_chain
            .process_batch(&self.source_data, &mut transformed_data)?;

        // Create channels: Source -> Window Tasks.
        let mut source_to_window_channels = Vec::new();
        for _ in 0..parallelism {
            source_to_window_channels.push(local_channel(buffer_size));
        }

        let source_senders: Vec<_> = source_to_window_channels
            .iter()
            .map(|(sender, _)| sender.clone())
            .collect();
        let source_receivers: Vec<_> = source_to_window_channels
            .into_iter()
            .map(|(_, receiver)| receiver)
            .collect();

        // Create channels: Window Tasks -> Collector.
        let mut window_to_collector_channels = Vec::new();
        for _ in 0..parallelism {
            window_to_collector_channels.push(local_channel(buffer_size));
        }

        let collector_senders: Vec<_> = window_to_collector_channels
            .iter()
            .map(|(sender, _)| sender.clone())
            .collect();
        let collector_receivers: Vec<_> = window_to_collector_channels
            .into_iter()
            .map(|(_, receiver)| receiver)
            .collect();

        // Spawn Source Task.
        let source_key_fn = self.key_fn.clone();
        let timestamp_assigner = TimestampAssignerOperator::new(self.strategy);
        let source_handle = thread::spawn(move || -> anyhow::Result<()> {
            let partitioner = HashPartitioner::new(source_key_fn);
            let mut generator = timestamp_assigner.create_watermark_generator::<OpChain::OUT>();
            let mut last_emitted_wm = streamcrab_core::time::EVENT_TIME_MIN;

            for item in transformed_data {
                let ts = timestamp_assigner.extract_timestamp(&item);

                // Drop late elements (v1: no allowed lateness).
                if ts <= last_emitted_wm {
                    continue;
                }

                generator.on_event(ts);

                let partition = partitioner.partition(&item, parallelism);
                source_senders[partition].send(StreamElement::timestamped_record(item, ts))?;

                if let Some(wm) = generator.current_watermark() {
                    if wm.timestamp > last_emitted_wm {
                        last_emitted_wm = wm.timestamp;
                        for sender in &source_senders {
                            sender.send(StreamElement::Watermark(wm))?;
                        }
                    }
                }
            }

            // Flush remaining windows and terminate all workers.
            let final_wm = Watermark::new(EVENT_TIME_MAX);
            for sender in &source_senders {
                sender.send(StreamElement::Watermark(final_wm))?;
                sender.send(StreamElement::End)?;
            }

            Ok(())
        });

        // Spawn Window Tasks.
        let mut worker_handles = Vec::new();
        let mut source_receivers_iter = source_receivers.into_iter();

        for (_task_id, sender) in collector_senders
            .iter()
            .cloned()
            .enumerate()
            .take(parallelism)
        {
            let receiver = source_receivers_iter.next().unwrap();
            let key_fn = self.key_fn.clone();
            let assigner = self.assigner.clone();
            let trigger = self.trigger.clone();
            let window_fn = self.window_fn.clone();

            let handle = thread::spawn(move || -> anyhow::Result<()> {
                use std::time::{SystemTime, UNIX_EPOCH};
                let mut operator = WindowOperator::new(
                    key_fn,
                    |_e: &OpChain::OUT| 0i64,
                    assigner,
                    trigger,
                    window_fn,
                );
                let processing_time_ms = || -> i64 {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0)
                };

                loop {
                    let element = receiver.recv()?;
                    let is_end = matches!(element, StreamElement::End);

                    for out in operator.process(element)? {
                        sender.send(out)?;
                    }

                    for out in operator.on_processing_time(processing_time_ms())? {
                        sender.send(out)?;
                    }

                    if is_end {
                        break;
                    }
                }

                Ok(())
            });

            worker_handles.push(handle);
        }

        // Collector (current thread).
        let mut out_records: Vec<StreamRecord<OUT>> = Vec::new();
        let mut ended_count = 0usize;
        let total_tasks = collector_receivers.len();

        while ended_count < total_tasks {
            for receiver in &collector_receivers {
                match receiver.try_recv()? {
                    Some(StreamElement::Record(record)) => out_records.push(record),
                    Some(StreamElement::End) => ended_count += 1,
                    Some(_) | None => {}
                }
            }
            thread::sleep(std::time::Duration::from_micros(100));
        }

        source_handle.join().unwrap()?;
        for handle in worker_handles {
            handle.join().unwrap()?;
        }

        Ok(out_records)
    }

    /// Execute the windowed job in parallel with manual checkpoint barriers.
    ///
    /// `barrier_after_records` controls when to inject checkpoints by transformed-record
    /// position. For example `[100, 500]` injects a checkpoint barrier after processing
    /// the 100th and 500th transformed records in the source stage.
    pub fn execute_with_parallelism_and_checkpoints(
        mut self,
        parallelism: usize,
        barrier_after_records: &[usize],
    ) -> anyhow::Result<CheckpointedExecutionResult<Vec<streamcrab_core::types::StreamRecord<OUT>>>>
    where
        WA: Clone,
        Tr: Clone,
        WFN: Clone,
    {
        use std::thread;
        use std::time::{SystemTime, UNIX_EPOCH};
        use streamcrab_core::channel::local_channel;
        use streamcrab_core::partitioner::{HashPartitioner, Partitioner};
        use streamcrab_core::time::EVENT_TIME_MAX;
        use streamcrab_core::types::{StreamElement, StreamRecord, Watermark};
        use streamcrab_core::window::WindowOperator;

        if parallelism == 0 {
            return Err(anyhow::anyhow!("parallelism must be greater than 0"));
        }

        let buffer_size = 1024;

        let mut transformed_data: Vec<OpChain::OUT> = Vec::new();
        self.op_chain
            .process_batch(&self.source_data, &mut transformed_data)?;

        let mut barrier_points = barrier_after_records.to_vec();
        barrier_points.sort_unstable();
        barrier_points.dedup();
        if let Some(invalid) = barrier_points.iter().find(|&&p| p > transformed_data.len()) {
            return Err(anyhow::anyhow!(
                "checkpoint trigger position {} exceeds record count {}",
                invalid,
                transformed_data.len()
            ));
        }

        let mut source_to_window_channels = Vec::new();
        for _ in 0..parallelism {
            source_to_window_channels.push(local_channel(buffer_size));
        }

        let source_senders: Vec<_> = source_to_window_channels
            .iter()
            .map(|(sender, _)| sender.clone())
            .collect();
        let source_receivers: Vec<_> = source_to_window_channels
            .into_iter()
            .map(|(_, receiver)| receiver)
            .collect();

        let mut window_to_collector_channels = Vec::new();
        for _ in 0..parallelism {
            window_to_collector_channels.push(local_channel(buffer_size));
        }

        let collector_senders: Vec<_> = window_to_collector_channels
            .iter()
            .map(|(sender, _)| sender.clone())
            .collect();
        let collector_receivers: Vec<_> = window_to_collector_channels
            .into_iter()
            .map(|(_, receiver)| receiver)
            .collect();

        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let mut coordinator_inner = CheckpointCoordinator::new(storage);
        coordinator_inner.retained_checkpoints = 64;
        let coordinator: Arc<CheckpointCoordinator<InMemoryCheckpointStorage>> =
            Arc::new(coordinator_inner);
        let expected_tasks: Vec<TaskId> = (0..parallelism)
            .map(|idx| TaskId::new(VertexId::new(2), idx))
            .collect();
        let now_ms = || -> i64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        };
        let mut checkpoint_plan = Vec::with_capacity(barrier_points.len());
        for trigger_pos in barrier_points {
            let barrier = coordinator.trigger_checkpoint(now_ms(), expected_tasks.clone())?;
            checkpoint_plan.push((trigger_pos, barrier));
        }
        let checkpoint_count = checkpoint_plan.len();
        let source_checkpoint_plan = checkpoint_plan.clone();
        let (ack_sender, ack_receiver) = crossbeam_channel::unbounded::<TaskCheckpointEvent>();

        let source_key_fn = self.key_fn.clone();
        let timestamp_assigner = TimestampAssignerOperator::new(self.strategy);
        let source_handle = thread::spawn(move || -> anyhow::Result<()> {
            let partitioner = HashPartitioner::new(source_key_fn);
            let mut generator = timestamp_assigner.create_watermark_generator::<OpChain::OUT>();
            let mut last_emitted_wm = streamcrab_core::time::EVENT_TIME_MIN;
            let mut processed = 0usize;
            let mut checkpoint_idx = 0usize;

            while checkpoint_idx < source_checkpoint_plan.len()
                && source_checkpoint_plan[checkpoint_idx].0 == processed
            {
                let barrier = source_checkpoint_plan[checkpoint_idx].1;
                for sender in &source_senders {
                    sender.send(StreamElement::CheckpointBarrier(barrier))?;
                }
                checkpoint_idx += 1;
            }

            for item in transformed_data {
                processed += 1;
                let ts = timestamp_assigner.extract_timestamp(&item);

                if ts > last_emitted_wm {
                    generator.on_event(ts);
                    let partition = partitioner.partition(&item, parallelism);
                    source_senders[partition].send(StreamElement::timestamped_record(item, ts))?;

                    if let Some(wm) = generator.current_watermark() {
                        if wm.timestamp > last_emitted_wm {
                            last_emitted_wm = wm.timestamp;
                            for sender in &source_senders {
                                sender.send(StreamElement::Watermark(wm))?;
                            }
                        }
                    }
                }

                while checkpoint_idx < source_checkpoint_plan.len()
                    && source_checkpoint_plan[checkpoint_idx].0 == processed
                {
                    let barrier = source_checkpoint_plan[checkpoint_idx].1;
                    for sender in &source_senders {
                        sender.send(StreamElement::CheckpointBarrier(barrier))?;
                    }
                    checkpoint_idx += 1;
                }
            }

            let final_wm = Watermark::new(EVENT_TIME_MAX);
            for sender in &source_senders {
                sender.send(StreamElement::Watermark(final_wm))?;
                sender.send(StreamElement::End)?;
            }

            Ok(())
        });

        let mut worker_handles = Vec::new();
        let mut source_receivers_iter = source_receivers.into_iter();

        for (task_id, sender) in collector_senders
            .iter()
            .cloned()
            .enumerate()
            .take(parallelism)
        {
            let receiver = source_receivers_iter.next().unwrap();
            let ack_sender = ack_sender.clone();
            let task_id = TaskId::new(VertexId::new(2), task_id);
            let key_fn = self.key_fn.clone();
            let assigner = self.assigner.clone();
            let trigger = self.trigger.clone();
            let window_fn = self.window_fn.clone();

            let handle = thread::spawn(move || -> anyhow::Result<()> {
                let mut operator = WindowOperator::new(
                    key_fn,
                    |_e: &OpChain::OUT| 0i64,
                    assigner,
                    trigger,
                    window_fn,
                );
                let processing_time_ms = || -> i64 {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0)
                };

                loop {
                    let element = receiver.recv()?;
                    match element {
                        StreamElement::CheckpointBarrier(barrier) => {
                            let snapshot = operator.snapshot_state()?;
                            ack_sender
                                .send(TaskCheckpointEvent::Ack(TaskCheckpointAck {
                                    checkpoint_id: barrier.checkpoint_id,
                                    task_id,
                                    state: snapshot,
                                }))
                                .map_err(|e| {
                                    anyhow::anyhow!("failed to send checkpoint ack: {e}")
                                })?;
                            sender.send(StreamElement::CheckpointBarrier(barrier))?;
                            for out in operator.on_processing_time(processing_time_ms())? {
                                sender.send(out)?;
                            }
                        }
                        other => {
                            let is_end = matches!(other, StreamElement::End);
                            for out in operator.process(other)? {
                                sender.send(out)?;
                            }
                            for out in operator.on_processing_time(processing_time_ms())? {
                                sender.send(out)?;
                            }
                            if is_end {
                                break;
                            }
                        }
                    }
                }

                Ok(())
            });

            worker_handles.push(handle);
        }

        // Drop local sender handle so ack thread can observe channel close on worker failure.
        drop(ack_sender);
        let ack_handle = if checkpoint_count == 0 {
            None
        } else {
            let coordinator: Arc<CheckpointCoordinator<InMemoryCheckpointStorage>> =
                Arc::clone(&coordinator);
            let expected_event_count = checkpoint_count * parallelism;
            Some(thread::spawn(move || -> anyhow::Result<()> {
                for _ in 0..expected_event_count {
                    let event = ack_receiver
                        .recv()
                        .map_err(|e| anyhow::anyhow!("checkpoint event channel closed: {e}"))?;
                    match event {
                        TaskCheckpointEvent::Ack(ack) => {
                            coordinator.acknowledge_checkpoint(ack)?;
                        }
                        TaskCheckpointEvent::Aborted(abort) => {
                            coordinator.abort_checkpoint(
                                abort.checkpoint_id,
                                abort.task_id,
                                &abort.reason,
                            )?;
                        }
                    }
                }
                Ok(())
            }))
        };

        let mut out_records: Vec<StreamRecord<OUT>> = Vec::new();
        let mut ended_count = 0usize;
        let total_tasks = collector_receivers.len();

        while ended_count < total_tasks {
            for receiver in &collector_receivers {
                match receiver.try_recv()? {
                    Some(StreamElement::Record(record)) => out_records.push(record),
                    Some(StreamElement::End) => ended_count += 1,
                    Some(_) | None => {}
                }
            }
            thread::sleep(std::time::Duration::from_micros(100));
        }

        source_handle.join().unwrap()?;
        for handle in worker_handles {
            handle.join().unwrap()?;
        }
        if let Some(ack_handle) = ack_handle {
            ack_handle.join().unwrap()?;
        }

        let completed_checkpoints = coordinator.completed_checkpoint_ids()?;
        Ok(CheckpointedExecutionResult {
            result: out_records,
            completed_checkpoints,
        })
    }
}

/// Window reduce implemented on top of [`streamcrab_core::window::WindowFunction`].
///
/// This type is part of the public API surface because it appears in the return
/// type of [`WindowedStream::reduce`].
#[derive(Clone)]
pub struct ReduceWindowFn<K, IN, F> {
    reduce_fn: F,
    _phantom: PhantomData<(K, IN)>,
}

impl<K, IN, F> streamcrab_core::window::WindowFunction<K, IN, (K, IN)> for ReduceWindowFn<K, IN, F>
where
    K: StreamData,
    IN: StreamData,
    F: Fn(IN, IN) -> IN + Send,
{
    fn apply(
        &mut self,
        key: &K,
        _window: &streamcrab_core::window::TimeWindow,
        elements: &[IN],
        output: &mut Vec<(K, IN)>,
    ) {
        if elements.is_empty() {
            return;
        }

        let mut acc = elements[0].clone();
        for e in &elements[1..] {
            acc = (self.reduce_fn)(acc, e.clone());
        }

        output.push((key.clone(), acc));
    }
}

/// Window aggregate implemented on top of [`streamcrab_core::window::AggregateFunction`].
///
/// Each element is folded into the accumulator as it arrives (incremental semantics).
/// The final `OUT` value is produced when the window fires.
///
/// Note: the underlying [`WindowOperator`](streamcrab_core::window::WindowOperator) still
/// buffers all elements for the current P2 implementation.  True O(1)-space aggregation
/// (maintaining only the accumulator) is planned as a later optimisation.
///
/// This type is part of the public API surface because it appears in the return
/// type of [`WindowedStream::aggregate`].
#[derive(Clone)]
pub struct AggregateWindowFn<AGG, IN, ACC, OUT> {
    agg: AGG,
    _phantom: PhantomData<(IN, ACC, OUT)>,
}

impl<K, IN, ACC, OUT, AGG> streamcrab_core::window::WindowFunction<K, IN, (K, OUT)>
    for AggregateWindowFn<AGG, IN, ACC, OUT>
where
    K: streamcrab_core::types::StreamData + Clone,
    IN: streamcrab_core::types::StreamData,
    OUT: streamcrab_core::types::StreamData,
    ACC: Send,
    AGG: streamcrab_core::window::AggregateFunction<IN, ACC, OUT> + Send,
{
    fn apply(
        &mut self,
        key: &K,
        _window: &streamcrab_core::window::TimeWindow,
        elements: &[IN],
        output: &mut Vec<(K, OUT)>,
    ) {
        let mut acc = self.agg.create_accumulator();
        for e in elements {
            self.agg.add(&mut acc, e);
        }
        output.push((key.clone(), self.agg.get_result(acc)));
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

struct FnReduceFunction<F> {
    reduce_fn: F,
}

impl<T, F> streamcrab_core::process::ReduceFunction<T> for FnReduceFunction<F>
where
    T: StreamData,
    F: Fn(T, T) -> T + Send,
{
    fn reduce(&mut self, value1: T, value2: T) -> anyhow::Result<T> {
        Ok((self.reduce_fn)(value1, value2))
    }
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
    ) -> anyhow::Result<SharedReduceResults<K, OpChain::OUT>> {
        if parallelism == 0 {
            return Err(anyhow::anyhow!("parallelism must be greater than 0"));
        }

        let buffer_size = 1024;
        let results = Arc::new(Mutex::new(std::collections::HashMap::new()));

        // Step 1: Apply operator chain to transform source data.
        let mut transformed_data = Vec::new();
        self.op_chain
            .process_batch(&self.source_data, &mut transformed_data)?;

        let (source_senders, source_receivers) =
            Self::create_channels::<OpChain::OUT>(parallelism, buffer_size);
        let (collector_senders, collector_receivers) =
            Self::create_channels::<(K, OpChain::OUT)>(parallelism, buffer_size);

        let source_handle = Self::spawn_source_task(
            transformed_data,
            self.key_fn.clone(),
            parallelism,
            source_senders,
        );
        let reduce_handles = Self::spawn_reduce_tasks(
            parallelism,
            source_receivers,
            collector_senders,
            self.key_fn,
            self.reduce_fn,
        );
        let collector_handle =
            Self::spawn_collector_task(collector_receivers, Arc::clone(&results));

        Self::join_worker(source_handle, "source")?;
        for handle in reduce_handles {
            Self::join_worker(handle, "reduce")?;
        }
        Self::join_worker(collector_handle, "collector")?;

        Ok(results)
    }

    /// Execute with manual checkpoint barrier injection at specific record counts.
    ///
    /// `barrier_after_records` contains processed-record offsets at which a checkpoint
    /// barrier should be broadcast to all reduce workers. For example `[100, 500]`
    /// injects barriers after the 100th and 500th transformed records.
    pub fn execute_with_parallelism_and_checkpoints(
        mut self,
        parallelism: usize,
        barrier_after_records: &[usize],
    ) -> anyhow::Result<CheckpointedExecutionResult<SharedReduceResults<K, OpChain::OUT>>> {
        use std::time::{SystemTime, UNIX_EPOCH};

        if parallelism == 0 {
            return Err(anyhow::anyhow!("parallelism must be greater than 0"));
        }

        let buffer_size = 1024;
        let results = Arc::new(Mutex::new(std::collections::HashMap::new()));

        let mut transformed_data = Vec::new();
        self.op_chain
            .process_batch(&self.source_data, &mut transformed_data)?;

        let mut barrier_points = barrier_after_records.to_vec();
        barrier_points.sort_unstable();
        barrier_points.dedup();
        if let Some(invalid) = barrier_points.iter().find(|&&p| p > transformed_data.len()) {
            return Err(anyhow::anyhow!(
                "checkpoint trigger position {} exceeds record count {}",
                invalid,
                transformed_data.len()
            ));
        }

        let (source_senders, source_receivers) =
            Self::create_channels::<OpChain::OUT>(parallelism, buffer_size);
        let (collector_senders, collector_receivers) =
            Self::create_channels::<(K, OpChain::OUT)>(parallelism, buffer_size);

        let storage = Arc::new(InMemoryCheckpointStorage::new());
        let mut coordinator_inner = CheckpointCoordinator::new(storage);
        coordinator_inner.retained_checkpoints = 64;
        let coordinator: Arc<CheckpointCoordinator<InMemoryCheckpointStorage>> =
            Arc::new(coordinator_inner);
        let expected_tasks: Vec<TaskId> = (0..parallelism)
            .map(|idx| TaskId::new(VertexId::new(1), idx))
            .collect();

        let now_ms = || -> i64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        };

        let mut checkpoint_plan = Vec::with_capacity(barrier_points.len());
        for trigger_pos in barrier_points {
            let barrier = coordinator.trigger_checkpoint(now_ms(), expected_tasks.clone())?;
            checkpoint_plan.push((trigger_pos, barrier));
        }

        let (ack_sender, ack_receiver) = crossbeam_channel::unbounded::<TaskCheckpointEvent>();

        let source_handle = Self::spawn_source_task_with_checkpoints(
            transformed_data,
            self.key_fn.clone(),
            parallelism,
            source_senders,
            checkpoint_plan.clone(),
        );
        let reduce_handles = Self::spawn_reduce_tasks_with_checkpoints(
            parallelism,
            source_receivers,
            collector_senders,
            self.key_fn,
            self.reduce_fn,
            ack_sender,
        );
        let collector_handle =
            Self::spawn_collector_task(collector_receivers, Arc::clone(&results));

        let ack_handle = if checkpoint_plan.is_empty() {
            None
        } else {
            let coordinator: Arc<CheckpointCoordinator<InMemoryCheckpointStorage>> =
                Arc::clone(&coordinator);
            let expected_event_count = checkpoint_plan.len() * parallelism;
            Some(std::thread::spawn(move || -> anyhow::Result<()> {
                for _ in 0..expected_event_count {
                    let event = ack_receiver
                        .recv()
                        .map_err(|e| anyhow::anyhow!("checkpoint event channel closed: {e}"))?;
                    match event {
                        TaskCheckpointEvent::Ack(ack) => {
                            coordinator.acknowledge_checkpoint(ack)?;
                        }
                        TaskCheckpointEvent::Aborted(abort) => {
                            coordinator.abort_checkpoint(
                                abort.checkpoint_id,
                                abort.task_id,
                                &abort.reason,
                            )?;
                        }
                    }
                }
                Ok(())
            }))
        };

        Self::join_worker(source_handle, "source")?;
        for handle in reduce_handles {
            Self::join_worker(handle, "reduce")?;
        }
        Self::join_worker(collector_handle, "collector")?;
        if let Some(ack_handle) = ack_handle {
            Self::join_worker(ack_handle, "checkpoint-ack")?;
        }

        let completed_checkpoints = coordinator.completed_checkpoint_ids()?;
        Ok(CheckpointedExecutionResult {
            result: results,
            completed_checkpoints,
        })
    }

    fn create_channels<U>(
        parallelism: usize,
        buffer_size: usize,
    ) -> (
        Vec<streamcrab_core::channel::LocalChannelSender<U>>,
        Vec<streamcrab_core::channel::LocalChannelReceiver<U>>,
    )
    where
        U: Send + 'static,
    {
        use streamcrab_core::channel::local_channel;

        let mut pairs = Vec::with_capacity(parallelism);
        for _ in 0..parallelism {
            pairs.push(local_channel(buffer_size));
        }

        let mut senders = Vec::with_capacity(parallelism);
        let mut receivers = Vec::with_capacity(parallelism);
        for (sender, receiver) in pairs {
            senders.push(sender);
            receivers.push(receiver);
        }
        (senders, receivers)
    }

    fn spawn_source_task(
        transformed_data: Vec<OpChain::OUT>,
        key_fn: KeyFn,
        parallelism: usize,
        source_senders: Vec<streamcrab_core::channel::LocalChannelSender<OpChain::OUT>>,
    ) -> std::thread::JoinHandle<anyhow::Result<()>> {
        use std::thread;
        use streamcrab_core::partitioner::{HashPartitioner, Partitioner};
        use streamcrab_core::types::StreamElement;

        thread::spawn(move || -> anyhow::Result<()> {
            let partitioner = HashPartitioner::new(key_fn);

            for item in transformed_data {
                let partition = partitioner.partition(&item, parallelism);
                source_senders[partition].send(StreamElement::record(item))?;
            }

            for sender in &source_senders {
                sender.send(StreamElement::End)?;
            }

            Ok(())
        })
    }

    fn spawn_source_task_with_checkpoints(
        transformed_data: Vec<OpChain::OUT>,
        key_fn: KeyFn,
        parallelism: usize,
        source_senders: Vec<streamcrab_core::channel::LocalChannelSender<OpChain::OUT>>,
        checkpoint_plan: Vec<(usize, streamcrab_core::types::Barrier)>,
    ) -> std::thread::JoinHandle<anyhow::Result<()>> {
        use std::thread;
        use streamcrab_core::partitioner::{HashPartitioner, Partitioner};
        use streamcrab_core::types::StreamElement;

        thread::spawn(move || -> anyhow::Result<()> {
            let partitioner = HashPartitioner::new(key_fn);
            let mut processed = 0usize;
            let mut checkpoint_idx = 0usize;

            while checkpoint_idx < checkpoint_plan.len()
                && checkpoint_plan[checkpoint_idx].0 == processed
            {
                let barrier = checkpoint_plan[checkpoint_idx].1;
                for sender in &source_senders {
                    sender.send(StreamElement::CheckpointBarrier(barrier))?;
                }
                checkpoint_idx += 1;
            }

            for item in transformed_data {
                let partition = partitioner.partition(&item, parallelism);
                source_senders[partition].send(StreamElement::record(item))?;
                processed += 1;

                while checkpoint_idx < checkpoint_plan.len()
                    && checkpoint_plan[checkpoint_idx].0 == processed
                {
                    let barrier = checkpoint_plan[checkpoint_idx].1;
                    for sender in &source_senders {
                        sender.send(StreamElement::CheckpointBarrier(barrier))?;
                    }
                    checkpoint_idx += 1;
                }
            }

            for sender in &source_senders {
                sender.send(StreamElement::End)?;
            }

            Ok(())
        })
    }

    fn spawn_reduce_tasks(
        parallelism: usize,
        source_receivers: Vec<streamcrab_core::channel::LocalChannelReceiver<OpChain::OUT>>,
        collector_senders: Vec<streamcrab_core::channel::LocalChannelSender<(K, OpChain::OUT)>>,
        key_fn: KeyFn,
        reduce_fn: ReduceFn,
    ) -> Vec<std::thread::JoinHandle<anyhow::Result<()>>> {
        use std::thread;
        use streamcrab_core::process::ReduceOperator;
        use streamcrab_core::state::HashMapStateBackend;
        use streamcrab_core::types::StreamElement;

        let mut handles = Vec::with_capacity(parallelism);
        let mut receivers_iter = source_receivers.into_iter();

        for sender in collector_senders.into_iter().take(parallelism) {
            let receiver = receivers_iter.next().unwrap();
            let key_fn = key_fn.clone();
            let reduce_fn = reduce_fn.clone();

            let handle = thread::spawn(move || -> anyhow::Result<()> {
                let reducer = FnReduceFunction { reduce_fn };
                let backend = HashMapStateBackend::new();
                let mut operator = ReduceOperator::new(reducer, backend);

                loop {
                    let element = receiver.recv()?;
                    match element {
                        StreamElement::Record(record) => {
                            let item = record.value;
                            let key = key_fn(&item);
                            let input = vec![(key, item)];
                            let mut output = Vec::new();
                            operator.process_batch(&input, &mut output)?;

                            for result in output {
                                sender.send(StreamElement::record(result))?;
                            }
                        }
                        StreamElement::End => {
                            sender.send(StreamElement::End)?;
                            break;
                        }
                        _ => {}
                    }
                }

                Ok(())
            });
            handles.push(handle);
        }

        handles
    }

    fn spawn_reduce_tasks_with_checkpoints(
        parallelism: usize,
        source_receivers: Vec<streamcrab_core::channel::LocalChannelReceiver<OpChain::OUT>>,
        collector_senders: Vec<streamcrab_core::channel::LocalChannelSender<(K, OpChain::OUT)>>,
        key_fn: KeyFn,
        reduce_fn: ReduceFn,
        ack_sender: crossbeam_channel::Sender<TaskCheckpointEvent>,
    ) -> Vec<std::thread::JoinHandle<anyhow::Result<()>>> {
        use std::thread;
        use streamcrab_core::process::ReduceOperator;
        use streamcrab_core::state::HashMapStateBackend;
        use streamcrab_core::types::StreamElement;

        let mut handles = Vec::with_capacity(parallelism);
        let mut receivers_iter = source_receivers.into_iter();

        for (subtask, sender) in collector_senders.into_iter().enumerate().take(parallelism) {
            let receiver = receivers_iter.next().unwrap();
            let key_fn = key_fn.clone();
            let reduce_fn = reduce_fn.clone();
            let ack_sender = ack_sender.clone();
            let task_id = TaskId::new(VertexId::new(1), subtask);

            let handle = thread::spawn(move || -> anyhow::Result<()> {
                let reducer = FnReduceFunction { reduce_fn };
                let backend = HashMapStateBackend::new();
                let mut operator = ReduceOperator::new(reducer, backend);

                loop {
                    let element = receiver.recv()?;
                    match element {
                        StreamElement::Record(record) => {
                            let item = record.value;
                            let key = key_fn(&item);
                            let input = vec![(key, item)];
                            let mut output = Vec::new();
                            operator.process_batch(&input, &mut output)?;

                            for result in output {
                                sender.send(StreamElement::record(result))?;
                            }
                        }
                        StreamElement::CheckpointBarrier(barrier) => {
                            let snapshot =
                                streamcrab_core::operator_chain::Operator::snapshot_state(
                                    &operator,
                                )?;
                            ack_sender
                                .send(TaskCheckpointEvent::Ack(TaskCheckpointAck {
                                    checkpoint_id: barrier.checkpoint_id,
                                    task_id,
                                    state: snapshot,
                                }))
                                .map_err(|e| {
                                    anyhow::anyhow!("failed to send checkpoint ack: {e}")
                                })?;
                            sender.send(StreamElement::CheckpointBarrier(barrier))?;
                        }
                        StreamElement::End => {
                            sender.send(StreamElement::End)?;
                            break;
                        }
                        _ => {}
                    }
                }

                Ok(())
            });
            handles.push(handle);
        }

        handles
    }

    fn spawn_collector_task(
        collector_receivers: Vec<streamcrab_core::channel::LocalChannelReceiver<(K, OpChain::OUT)>>,
        results: Arc<Mutex<std::collections::HashMap<K, OpChain::OUT>>>,
    ) -> std::thread::JoinHandle<anyhow::Result<()>> {
        use std::thread;
        use streamcrab_core::types::StreamElement;

        thread::spawn(move || -> anyhow::Result<()> {
            let mut ended_count = 0;
            let total_tasks = collector_receivers.len();

            while ended_count < total_tasks {
                for receiver in &collector_receivers {
                    match receiver.try_recv() {
                        Ok(Some(StreamElement::Record(record))) => {
                            let (key, value) = record.value;
                            results.lock().unwrap().insert(key, value);
                        }
                        Ok(Some(StreamElement::End)) => {
                            ended_count += 1;
                        }
                        Ok(Some(_)) | Ok(None) | Err(_) => {}
                    }
                }

                thread::sleep(std::time::Duration::from_micros(100));
            }

            Ok(())
        })
    }

    fn join_worker(
        handle: std::thread::JoinHandle<anyhow::Result<()>>,
        name: &str,
    ) -> anyhow::Result<()> {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("{name} thread panicked"))?
    }
}
