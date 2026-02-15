use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use streamcrab_core::graph::{OperatorType, StreamGraph};
use streamcrab_core::runtime::{downstream_map, topo_sort};
use streamcrab_core::types::{BoxedValue, NodeId, StreamData};

use crate::datastream::{DataStream, OperatorFactory};

/// Shared mutable state for building the stream graph.
pub(crate) struct EnvInner {
    pub graph: StreamGraph,
    pub operators: Vec<OperatorFactory>,
}

/// The entry point for building a stream processing job.
///
/// Create an environment, add sources via [`from_iter`](Self::from_iter),
/// chain transformations on the returned [`DataStream`], and call
/// [`execute`](Self::execute) to run the pipeline.
pub struct StreamExecutionEnvironment {
    pub(crate) inner: Rc<RefCell<EnvInner>>,
}

impl StreamExecutionEnvironment {
    /// Create a new execution environment for a job with the given name.
    pub fn new(_job_name: &str) -> Self {
        Self {
            inner: Rc::new(RefCell::new(EnvInner {
                graph: StreamGraph::new(),
                operators: Vec::new(),
            })),
        }
    }

    /// Add a source that produces elements from an iterator.
    pub fn from_iter<T, I>(&self, iter: I) -> DataStream<T>
    where
        T: StreamData + std::fmt::Debug,
        I: IntoIterator<Item = T> + 'static,
    {
        let mut inner = self.inner.borrow_mut();
        let node_id = inner.graph.add_node(OperatorType::Source, 1);

        let items: Vec<T> = iter.into_iter().collect();
        inner.operators.push(OperatorFactory::Source {
            node_id,
            produce: Box::new(move || items.clone().into_iter().map(BoxedValue::new).collect()),
        });

        DataStream {
            env: Rc::clone(&self.inner),
            node_id,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Execute the pipeline (single-threaded, P0).
    ///
    /// Walks the graph in topological order, pushing data through each operator.
    pub fn execute(&self) -> anyhow::Result<()> {
        let inner = self.inner.borrow();
        let topo = topo_sort(&inner.graph);
        let adj = downstream_map(&inner.graph);

        // Operator lookup by node_id.
        let mut op_map: HashMap<NodeId, &OperatorFactory> = HashMap::new();
        for op in &inner.operators {
            op_map.insert(op.node_id(), op);
        }

        // Data buffers between nodes.
        let mut buffers: HashMap<NodeId, Vec<BoxedValue>> = HashMap::new();

        // Keyed state: key_bytes -> accumulated value (for Reduce).
        let mut current_keys: Vec<Vec<u8>> = Vec::new();
        let mut reduce_state: HashMap<Vec<u8>, BoxedValue> = HashMap::new();

        for &node_id in &topo {
            let input = buffers.remove(&node_id).unwrap_or_default();
            let op = op_map.get(&node_id).expect("missing operator for node");

            let output: Vec<BoxedValue> = match op {
                OperatorFactory::Source { produce, .. } => produce(),

                OperatorFactory::Map { apply, .. } => input.into_iter().map(apply).collect(),

                OperatorFactory::Filter { predicate, .. } => {
                    input.into_iter().filter(|v| predicate(v)).collect()
                }

                OperatorFactory::FlatMap { apply, .. } => {
                    input.into_iter().flat_map(apply).collect()
                }

                OperatorFactory::KeyBy { key_fn, .. } => {
                    current_keys.clear();
                    let mut out = Vec::with_capacity(input.len());
                    for v in input {
                        let key = key_fn(&v);
                        current_keys.push(key);
                        out.push(v);
                    }
                    out
                }

                OperatorFactory::Reduce { reduce_fn, .. } => {
                    let mut output = Vec::new();
                    for (i, v) in input.into_iter().enumerate() {
                        let key = &current_keys[i];
                        if let Some(acc) = reduce_state.remove(key) {
                            let result = reduce_fn(acc, v);
                            output.push(result.clone());
                            reduce_state.insert(key.clone(), result);
                        } else {
                            output.push(v.clone());
                            reduce_state.insert(key.clone(), v);
                        }
                    }
                    output
                }

                OperatorFactory::Sink { sink_fn, .. } => {
                    for v in &input {
                        sink_fn(v);
                    }
                    Vec::new()
                }
            };

            // Route output to downstream nodes.
            if let Some(targets) = adj.get(&node_id) {
                match targets.len() {
                    0 => {}
                    1 => {
                        buffers.entry(targets[0]).or_default().extend(output);
                    }
                    _ => {
                        for target in targets {
                            let buf = buffers.entry(*target).or_default();
                            for item in &output {
                                buf.push(item.clone());
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
