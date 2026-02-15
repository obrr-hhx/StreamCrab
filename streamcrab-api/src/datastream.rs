use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use streamcrab_core::graph::{OperatorType, Partition};
use streamcrab_core::types::{BoxedValue, NodeId, StreamData};

use crate::environment::EnvInner;

/// Factory stored during graph construction, later used by the runtime.
#[allow(clippy::type_complexity)]
pub enum OperatorFactory {
    Source {
        node_id: NodeId,
        produce: Box<dyn Fn() -> Vec<BoxedValue>>,
    },
    Map {
        node_id: NodeId,
        apply: Box<dyn Fn(BoxedValue) -> BoxedValue>,
    },
    Filter {
        node_id: NodeId,
        predicate: Box<dyn Fn(&BoxedValue) -> bool>,
    },
    FlatMap {
        node_id: NodeId,
        apply: Box<dyn Fn(BoxedValue) -> Vec<BoxedValue>>,
    },
    KeyBy {
        node_id: NodeId,
        key_fn: Box<dyn Fn(&BoxedValue) -> Vec<u8>>,
    },
    Reduce {
        node_id: NodeId,
        reduce_fn: Box<dyn Fn(BoxedValue, BoxedValue) -> BoxedValue>,
    },
    Sink {
        node_id: NodeId,
        sink_fn: Box<dyn Fn(&BoxedValue)>,
    },
}

impl OperatorFactory {
    /// Return the graph node ID associated with this operator.
    pub fn node_id(&self) -> NodeId {
        match self {
            Self::Source { node_id, .. }
            | Self::Map { node_id, .. }
            | Self::Filter { node_id, .. }
            | Self::FlatMap { node_id, .. }
            | Self::KeyBy { node_id, .. }
            | Self::Reduce { node_id, .. }
            | Self::Sink { node_id, .. } => *node_id,
        }
    }
}

/// A handle representing a stream of type `T` in the processing graph.
///
/// Provides a fluent builder API for chaining transformations:
/// [`map`](Self::map), [`filter`](Self::filter), [`flat_map`](Self::flat_map),
/// [`key_by`](Self::key_by), [`print`](Self::print), and [`collect`](Self::collect).
pub struct DataStream<T: StreamData> {
    pub(crate) env: Rc<RefCell<EnvInner>>,
    pub(crate) node_id: NodeId,
    pub(crate) _phantom: PhantomData<T>,
}

impl<T: StreamData + std::fmt::Debug> DataStream<T> {
    /// Apply a one-to-one transformation to each element.
    pub fn map<O, F>(self, f: F) -> DataStream<O>
    where
        O: StreamData + std::fmt::Debug,
        F: Fn(T) -> O + 'static,
    {
        let mut inner = self.env.borrow_mut();
        let new_id = inner.graph.add_node(OperatorType::Map, 1);
        inner
            .graph
            .add_edge(self.node_id, new_id, Partition::Forward);
        inner.operators.push(OperatorFactory::Map {
            node_id: new_id,
            apply: Box::new(move |val| {
                let input: T = val.downcast();
                BoxedValue::new(f(input))
            }),
        });
        drop(inner);
        DataStream {
            env: self.env,
            node_id: new_id,
            _phantom: PhantomData,
        }
    }

    /// Keep only elements for which the predicate returns `true`.
    pub fn filter<F>(self, f: F) -> DataStream<T>
    where
        F: Fn(&T) -> bool + 'static,
    {
        let mut inner = self.env.borrow_mut();
        let new_id = inner.graph.add_node(OperatorType::Filter, 1);
        inner
            .graph
            .add_edge(self.node_id, new_id, Partition::Forward);
        inner.operators.push(OperatorFactory::Filter {
            node_id: new_id,
            predicate: Box::new(move |val| {
                let input = val.downcast_ref::<T>().expect("type mismatch in filter");
                f(input)
            }),
        });
        drop(inner);
        DataStream {
            env: self.env,
            node_id: new_id,
            _phantom: PhantomData,
        }
    }

    /// Apply a one-to-many transformation, producing zero or more output elements per input.
    pub fn flat_map<O, F>(self, f: F) -> DataStream<O>
    where
        O: StreamData + std::fmt::Debug,
        F: Fn(T) -> Vec<O> + 'static,
    {
        let mut inner = self.env.borrow_mut();
        let new_id = inner.graph.add_node(OperatorType::FlatMap, 1);
        inner
            .graph
            .add_edge(self.node_id, new_id, Partition::Forward);
        inner.operators.push(OperatorFactory::FlatMap {
            node_id: new_id,
            apply: Box::new(move |val| {
                let input: T = val.downcast();
                f(input).into_iter().map(BoxedValue::new).collect()
            }),
        });
        drop(inner);
        DataStream {
            env: self.env,
            node_id: new_id,
            _phantom: PhantomData,
        }
    }

    /// Partition the stream by key, returning a [`KeyedStream`] that supports stateful operations.
    pub fn key_by<K, F>(self, f: F) -> KeyedStream<T>
    where
        K: StreamData,
        F: Fn(&T) -> K + 'static,
    {
        let mut inner = self.env.borrow_mut();
        let new_id = inner.graph.add_node(OperatorType::KeyBy, 1);
        inner.graph.add_edge(self.node_id, new_id, Partition::Hash);
        inner.operators.push(OperatorFactory::KeyBy {
            node_id: new_id,
            key_fn: Box::new(move |val| {
                let input = val.downcast_ref::<T>().expect("type mismatch in key_by");
                let key = f(input);
                bincode::serialize(&key).expect("key serialization failed")
            }),
        });
        drop(inner);
        KeyedStream {
            env: self.env,
            node_id: new_id,
            _phantom: PhantomData,
        }
    }

    /// Print each element to stdout using its `Debug` representation. Terminal sink.
    pub fn print(self) {
        let mut inner = self.env.borrow_mut();
        let new_id = inner.graph.add_node(OperatorType::Sink, 1);
        inner
            .graph
            .add_edge(self.node_id, new_id, Partition::Forward);
        inner.operators.push(OperatorFactory::Sink {
            node_id: new_id,
            sink_fn: Box::new(|val| {
                let input = val.downcast_ref::<T>().expect("type mismatch in print");
                println!("{input:?}");
            }),
        });
    }

    /// Collect all output into a shared Vec for testing/inspection.
    pub fn collect(self) -> Rc<RefCell<Vec<T>>> {
        let results: Rc<RefCell<Vec<T>>> = Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);

        let mut inner = self.env.borrow_mut();
        let new_id = inner.graph.add_node(OperatorType::Sink, 1);
        inner
            .graph
            .add_edge(self.node_id, new_id, Partition::Forward);
        inner.operators.push(OperatorFactory::Sink {
            node_id: new_id,
            sink_fn: Box::new(move |val| {
                let input = val.downcast_ref::<T>().expect("type mismatch in collect");
                results_clone.borrow_mut().push(input.clone());
            }),
        });

        results
    }
}

/// A keyed stream that supports stateful operations like [`reduce`](Self::reduce).
///
/// Created by calling [`DataStream::key_by`]. Elements with the same key are
/// grouped together for aggregation.
pub struct KeyedStream<T: StreamData> {
    pub(crate) env: Rc<RefCell<EnvInner>>,
    pub(crate) node_id: NodeId,
    pub(crate) _phantom: PhantomData<T>,
}

impl<T: StreamData + std::fmt::Debug> KeyedStream<T> {
    /// Reduce elements with the same key by repeatedly applying `f` to the
    /// accumulated value and each new element. Emits an updated result on every input.
    pub fn reduce<F>(self, f: F) -> DataStream<T>
    where
        F: Fn(T, T) -> T + 'static,
    {
        let mut inner = self.env.borrow_mut();
        let new_id = inner.graph.add_node(OperatorType::Reduce, 1);
        inner
            .graph
            .add_edge(self.node_id, new_id, Partition::Forward);
        inner.operators.push(OperatorFactory::Reduce {
            node_id: new_id,
            reduce_fn: Box::new(move |a, b| {
                let va: T = a.downcast();
                let vb: T = b.downcast();
                BoxedValue::new(f(va, vb))
            }),
        });
        drop(inner);
        DataStream {
            env: self.env,
            node_id: new_id,
            _phantom: PhantomData,
        }
    }
}
