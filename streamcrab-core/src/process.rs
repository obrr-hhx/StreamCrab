//! Keyed process functions for stateful stream processing.
//!
//! This module provides the [`KeyedProcessFunction`] trait for implementing
//! stateful transformations on keyed streams.
//!
//! ## Key Concepts
//!
//! - **KeyedProcessFunction**: User-defined function that processes elements with access to keyed state
//! - **ProcessContext**: Provides access to state backend and output collection
//! - **Descriptor Pattern**: State handles (ValueStateHandle, ListStateHandle, MapStateHandle) are
//!   lightweight metadata objects that don't borrow the backend
//!
//! ## Example
//!
//! ```ignore
//! struct CountAggregator {
//!     count_state: ValueStateHandle<i64>,
//! }
//!
//! impl KeyedProcessFunction<String, Event, (String, i64)> for CountAggregator {
//!     fn process_element(
//!         &mut self,
//!         key: &String,
//!         value: Event,
//!         ctx: &mut ProcessContext<(String, i64)>,
//!     ) -> Result<()> {
//!         let count = self.count_state.get(ctx.state())?.unwrap_or(0);
//!         let new_count = count + 1;
//!         self.count_state.put(ctx.state_mut(), new_count)?;
//!         ctx.collect((key.clone(), new_count));
//!         Ok(())
//!     }
//! }
//! ```

use crate::state::KeyedStateBackend;
use crate::types::StreamData;
use anyhow::Result;

/// Keyed process function: stateful transformation on keyed streams.
///
/// This trait allows users to implement custom stateful logic with access to:
/// - The current key
/// - The input value
/// - Keyed state (via ProcessContext)
/// - Output collection (via ProcessContext)
///
/// **Key design**: Uses descriptor pattern for state access, avoiding borrow-checker conflicts.
///
/// **Generic parameter `B`**: The concrete state backend type. This allows the compiler to
/// inline state operations and avoid trait object overhead.
pub trait KeyedProcessFunction<K, IN, OUT, B>: Send
where
    K: StreamData,
    IN: StreamData,
    OUT: StreamData,
    B: KeyedStateBackend,
{
    /// Process a single element with access to keyed state.
    ///
    /// # Arguments
    ///
    /// - `key`: The key for this element (already extracted by key_by)
    /// - `value`: The input value
    /// - `ctx`: Context providing state access and output collection
    ///
    /// # Example
    ///
    /// ```ignore
    /// fn process_element(
    ///     &mut self,
    ///     key: &String,
    ///     value: i32,
    ///     ctx: &mut ProcessContext<i32, HashMapStateBackend>,
    /// ) -> Result<()> {
    ///     let sum = self.sum_state.get(ctx.state())?.unwrap_or(0);
    ///     self.sum_state.put(ctx.state_mut(), sum + value)?;
    ///     ctx.collect(sum + value);
    ///     Ok(())
    /// }
    /// ```
    fn process_element(
        &mut self,
        key: &K,
        value: IN,
        ctx: &mut ProcessContext<OUT, B>,
    ) -> Result<()>;
}

/// Context for process functions.
///
/// Provides:
/// - Access to keyed state backend (read and write)
/// - Output collection
///
/// **Design**: Holds a mutable reference to the state backend and an output buffer.
/// The descriptor pattern allows users to create state handles without borrowing conflicts.
///
/// **Generic parameter `B`**: The concrete state backend type (e.g., HashMapStateBackend).
/// This avoids trait object overhead and allows the compiler to inline state operations.
pub struct ProcessContext<'a, OUT, B: KeyedStateBackend> {
    /// State backend for keyed state operations
    state_backend: &'a mut B,
    /// Output buffer for collecting results
    output_buffer: &'a mut Vec<OUT>,
}

impl<'a, OUT, B: KeyedStateBackend> ProcessContext<'a, OUT, B> {
    /// Create a new process context.
    pub fn new(state_backend: &'a mut B, output_buffer: &'a mut Vec<OUT>) -> Self {
        Self {
            state_backend,
            output_buffer,
        }
    }

    /// Get immutable access to the state backend.
    ///
    /// Use this with state handles for read operations:
    /// ```ignore
    /// let count = count_handle.get(ctx.state())?;
    /// ```
    pub fn state(&self) -> &B {
        self.state_backend
    }

    /// Get mutable access to the state backend.
    ///
    /// Use this with state handles for write operations:
    /// ```ignore
    /// count_handle.put(ctx.state_mut(), new_count)?;
    /// ```
    pub fn state_mut(&mut self) -> &mut B {
        self.state_backend
    }

    /// Collect an output element.
    ///
    /// The element will be emitted downstream after processing completes.
    pub fn collect(&mut self, output: OUT) {
        self.output_buffer.push(output);
    }
}

// ============================================================================
// ReduceFunction: Simpler stateful aggregation
// ============================================================================

/// Reduce function: combines two values into one.
///
/// This is a simpler alternative to KeyedProcessFunction for basic aggregations.
/// The reduce function is associative: `reduce(reduce(a, b), c) == reduce(a, reduce(b, c))`.
///
/// ## Example
///
/// ```ignore
/// struct SumReducer;
///
/// impl ReduceFunction<i32> for SumReducer {
///     fn reduce(&mut self, value1: i32, value2: i32) -> Result<i32> {
///         Ok(value1 + value2)
///     }
/// }
/// ```
pub trait ReduceFunction<T>: Send
where
    T: StreamData,
{
    /// Combine two values into one.
    ///
    /// # Arguments
    ///
    /// - `value1`: The accumulated value (from state)
    /// - `value2`: The new incoming value
    ///
    /// # Returns
    ///
    /// The combined result, which will be stored back to state and emitted.
    fn reduce(&mut self, value1: T, value2: T) -> Result<T>;
}

// ============================================================================
// ProcessOperator: Wraps KeyedProcessFunction as an Operator
// ============================================================================

use crate::operator_chain::Operator;

/// Operator that wraps a KeyedProcessFunction.
///
/// This operator:
/// 1. Receives (key, value) pairs as input
/// 2. Sets the current key in the state backend
/// 3. Calls the user's process function
/// 4. Collects outputs
///
/// **Design**: Integrates KeyedProcessFunction into the operator chain framework.
pub struct ProcessOperator<K, IN, OUT, F, B>
where
    K: StreamData,
    IN: StreamData,
    OUT: StreamData,
    F: KeyedProcessFunction<K, IN, OUT, B>,
    B: KeyedStateBackend,
{
    /// User-defined process function
    process_fn: F,
    /// State backend for keyed state
    state_backend: B,
    /// Phantom data for type parameters
    _phantom: std::marker::PhantomData<(K, IN, OUT)>,
}

impl<K, IN, OUT, F, B> ProcessOperator<K, IN, OUT, F, B>
where
    K: StreamData,
    IN: StreamData,
    OUT: StreamData,
    F: KeyedProcessFunction<K, IN, OUT, B>,
    B: KeyedStateBackend,
{
    /// Create a new process operator.
    ///
    /// # Arguments
    ///
    /// - `process_fn`: User-defined process function
    /// - `state_backend`: State backend for keyed state operations
    pub fn new(process_fn: F, state_backend: B) -> Self {
        Self {
            process_fn,
            state_backend,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<K, IN, OUT, F, B> Operator<(K, IN)> for ProcessOperator<K, IN, OUT, F, B>
where
    K: StreamData,
    IN: StreamData,
    OUT: StreamData + Send,
    F: KeyedProcessFunction<K, IN, OUT, B>,
    B: KeyedStateBackend,
{
    type OUT = OUT;

    fn process_batch(&mut self, input: &[(K, IN)], output: &mut Vec<Self::OUT>) -> Result<()> {
        // Reserve capacity for output (worst case: one output per input)
        output.reserve(input.len());

        for (key, value) in input {
            // Serialize key for state backend
            let key_bytes = bincode::serialize(key)
                .map_err(|e| anyhow::anyhow!("Failed to serialize key: {}", e))?;

            // Set current key in state backend
            self.state_backend.set_current_key(key_bytes);

            // Create context for this element
            let mut ctx = ProcessContext::new(&mut self.state_backend, output);

            // Call user's process function
            // Note: value is cloned here because we need to pass ownership
            // Future optimization: use references or Copy types
            self.process_fn
                .process_element(key, value.clone(), &mut ctx)?;
        }

        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        self.state_backend.snapshot()
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        self.state_backend.restore(data)
    }
}

// ============================================================================
// ReduceOperator: Wraps ReduceFunction as an Operator
// ============================================================================

use crate::state::ValueStateHandle;

/// Operator that wraps a ReduceFunction.
///
/// This operator:
/// 1. Receives (key, value) pairs as input
/// 2. Reads the accumulated value from state (if exists)
/// 3. Calls reduce(old_value, new_value) or uses new_value if no old value
/// 4. Stores the result back to state
/// 5. Emits (key, result)
///
/// **Design**: Simpler than ProcessOperator, specialized for reduce operations.
pub struct ReduceOperator<K, T, F, B>
where
    K: StreamData,
    T: StreamData,
    F: ReduceFunction<T>,
    B: KeyedStateBackend,
{
    /// User-defined reduce function
    reduce_fn: F,
    /// State backend for keyed state
    state_backend: B,
    /// State handle for accumulated value
    value_state: ValueStateHandle<T>,
    /// Phantom data for key type
    _phantom: std::marker::PhantomData<K>,
}

impl<K, T, F, B> ReduceOperator<K, T, F, B>
where
    K: StreamData,
    T: StreamData,
    F: ReduceFunction<T>,
    B: KeyedStateBackend,
{
    /// Create a new reduce operator.
    ///
    /// # Arguments
    ///
    /// - `reduce_fn`: User-defined reduce function
    /// - `state_backend`: State backend for keyed state operations
    pub fn new(reduce_fn: F, state_backend: B) -> Self {
        Self {
            reduce_fn,
            state_backend,
            value_state: ValueStateHandle::new("reduce_value"),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Snapshot keyed reduce state.
    pub fn snapshot_state(&self) -> Result<Vec<u8>> {
        self.state_backend.snapshot()
    }

    /// Restore keyed reduce state from snapshot bytes.
    pub fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        self.state_backend.restore(data)
    }
}

impl<K, T, F, B> Operator<(K, T)> for ReduceOperator<K, T, F, B>
where
    K: StreamData,
    T: StreamData + Send,
    F: ReduceFunction<T>,
    B: KeyedStateBackend,
{
    type OUT = (K, T);

    fn process_batch(&mut self, input: &[(K, T)], output: &mut Vec<Self::OUT>) -> Result<()> {
        // Reserve capacity for output (one output per input)
        output.reserve(input.len());

        for (key, value) in input {
            // Serialize key for state backend
            let key_bytes = bincode::serialize(key)
                .map_err(|e| anyhow::anyhow!("Failed to serialize key: {}", e))?;

            // Set current key in state backend
            self.state_backend.set_current_key(key_bytes);

            // Read old accumulated value
            let old_value = self.value_state.get(&self.state_backend)?;

            // Compute new value
            let new_value = match old_value {
                Some(old) => {
                    // Reduce: combine old and new
                    self.reduce_fn.reduce(old, value.clone())?
                }
                None => {
                    // First value for this key
                    value.clone()
                }
            };

            // Store new value
            self.value_state
                .put(&mut self.state_backend, new_value.clone())?;

            // Emit result
            output.push((key.clone(), new_value));
        }

        Ok(())
    }

    fn snapshot_state(&self) -> Result<Vec<u8>> {
        ReduceOperator::snapshot_state(self)
    }

    fn restore_state(&mut self, data: &[u8]) -> Result<()> {
        ReduceOperator::restore_state(self, data)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
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
}
