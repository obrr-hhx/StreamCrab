use std::collections::HashMap;

use crate::types::NodeId;

/// How data is partitioned between upstream and downstream operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partition {
    /// One-to-one, same parallelism required.
    Forward,
    /// Hash-partition by key.
    Hash,
    /// Send to all downstream instances.
    Broadcast,
}

/// The type of operator at a graph node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorType {
    Source,
    Map,
    Filter,
    FlatMap,
    KeyBy,
    Reduce,
    Sink,
}

/// A node in the stream processing DAG.
#[derive(Debug, Clone)]
pub struct StreamNode {
    pub id: NodeId,
    pub operator_type: OperatorType,
    pub parallelism: usize,
}

/// An edge connecting two nodes in the DAG.
#[derive(Debug, Clone)]
pub struct StreamEdge {
    pub source: NodeId,
    pub target: NodeId,
    pub partition: Partition,
}

/// The logical DAG representing the stream processing topology.
#[derive(Debug, Default)]
pub struct StreamGraph {
    pub nodes: HashMap<NodeId, StreamNode>,
    pub edges: Vec<StreamEdge>,
    next_id: NodeId,
}

impl StreamGraph {
    /// Create an empty stream graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a node and return its assigned ID.
    pub fn add_node(&mut self, operator_type: OperatorType, parallelism: usize) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;
        self.nodes.insert(
            id,
            StreamNode {
                id,
                operator_type,
                parallelism,
            },
        );
        id
    }

    /// Add an edge between two existing nodes.
    pub fn add_edge(&mut self, source: NodeId, target: NodeId, partition: Partition) {
        self.edges.push(StreamEdge {
            source,
            target,
            partition,
        });
    }

    /// Get downstream node IDs for a given node.
    pub fn downstream(&self, node_id: NodeId) -> Vec<NodeId> {
        self.edges
            .iter()
            .filter(|e| e.source == node_id)
            .map(|e| e.target)
            .collect()
    }

    /// Get upstream node IDs for a given node.
    pub fn upstream(&self, node_id: NodeId) -> Vec<NodeId> {
        self.edges
            .iter()
            .filter(|e| e.target == node_id)
            .map(|e| e.source)
            .collect()
    }

    /// Find all source nodes (no upstream edges).
    pub fn sources(&self) -> Vec<NodeId> {
        self.nodes
            .keys()
            .copied()
            .filter(|id| self.upstream(*id).is_empty())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_linear_graph() {
        let mut g = StreamGraph::new();
        let src = g.add_node(OperatorType::Source, 1);
        let map = g.add_node(OperatorType::Map, 1);
        let sink = g.add_node(OperatorType::Sink, 1);

        g.add_edge(src, map, Partition::Forward);
        g.add_edge(map, sink, Partition::Forward);

        assert_eq!(g.nodes.len(), 3);
        assert_eq!(g.edges.len(), 2);
        assert_eq!(g.downstream(src), vec![map]);
        assert_eq!(g.downstream(map), vec![sink]);
        assert_eq!(g.upstream(sink), vec![map]);
        assert_eq!(g.sources(), vec![src]);
    }

    #[test]
    fn test_hash_partition_edge() {
        let mut g = StreamGraph::new();
        let src = g.add_node(OperatorType::Source, 1);
        let key = g.add_node(OperatorType::KeyBy, 4);
        g.add_edge(src, key, Partition::Hash);

        assert_eq!(g.edges[0].partition, Partition::Hash);
    }
}
