//! # JobGraph
//!
//! Physical execution plan with operator chaining optimization.
//!
//! The JobGraph is derived from the StreamGraph by merging operators that can be chained together.
//! Chaining reduces serialization overhead and improves performance.

use std::collections::HashMap;

use crate::graph::{OperatorType, Partition, StreamGraph};
use crate::types::NodeId;

/// A unique identifier for a job vertex.
pub type VertexId = usize;

/// A descriptor for an operator in a chain.
#[derive(Debug, Clone)]
pub struct OperatorDescriptor {
    pub node_id: NodeId,
    pub operator_type: OperatorType,
}

/// A vertex in the JobGraph, potentially containing multiple chained operators.
#[derive(Debug, Clone)]
pub struct JobVertex {
    pub id: VertexId,
    /// Operators chained together in this vertex (execution order).
    pub chained_operators: Vec<OperatorDescriptor>,
    pub parallelism: usize,
}

/// An edge in the JobGraph connecting two vertices.
#[derive(Debug, Clone)]
pub struct JobEdge {
    pub source: VertexId,
    pub target: VertexId,
    pub partition: Partition,
}

/// The physical execution plan with operator chaining applied.
#[derive(Debug, Default)]
pub struct JobGraph {
    pub vertices: HashMap<VertexId, JobVertex>,
    pub edges: Vec<JobEdge>,
    next_vertex_id: VertexId,
}

impl JobGraph {
    /// Create an empty JobGraph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a vertex and return its ID.
    pub fn add_vertex(
        &mut self,
        chained_operators: Vec<OperatorDescriptor>,
        parallelism: usize,
    ) -> VertexId {
        let id = self.next_vertex_id;
        self.next_vertex_id += 1;
        self.vertices.insert(
            id,
            JobVertex {
                id,
                chained_operators,
                parallelism,
            },
        );
        id
    }

    /// Add an edge between two vertices.
    pub fn add_edge(&mut self, source: VertexId, target: VertexId, partition: Partition) {
        self.edges.push(JobEdge {
            source,
            target,
            partition,
        });
    }
}

/// Check if two operators can be chained together.
///
/// Chaining conditions (all must be true):
/// 1. Downstream has exactly one input (no multi-input operators)
/// 2. Forward partitioning (one-to-one connection)
/// 3. Same parallelism
/// 4. Not a keyBy boundary (KeyBy operator cannot be chained with upstream/downstream)
pub(crate) fn can_chain(stream_graph: &StreamGraph, upstream: NodeId, downstream: NodeId) -> bool {
    // Condition 1: Downstream must have exactly one input
    let downstream_inputs = stream_graph.upstream(downstream);
    if downstream_inputs.len() != 1 {
        return false;
    }

    // Find the edge between upstream and downstream
    let edge = stream_graph
        .edges
        .iter()
        .find(|e| e.source == upstream && e.target == downstream);

    if let Some(edge) = edge {
        // Condition 2: Must be Forward partitioning
        if edge.partition != Partition::Forward {
            return false;
        }

        // Condition 3: Same parallelism
        let upstream_node = &stream_graph.nodes[&upstream];
        let downstream_node = &stream_graph.nodes[&downstream];
        if upstream_node.parallelism != downstream_node.parallelism {
            return false;
        }

        // Condition 4: KeyBy is a chaining boundary
        if upstream_node.operator_type == OperatorType::KeyBy
            || downstream_node.operator_type == OperatorType::KeyBy
        {
            return false;
        }

        true
    } else {
        false
    }
}

/// Build a JobGraph from a StreamGraph by applying operator chaining.
///
/// Uses a greedy algorithm: starting from sources, chain as many downstream operators as possible.
pub fn build_job_graph(stream_graph: &StreamGraph) -> JobGraph {
    let mut job_graph = JobGraph::new();
    let mut node_to_vertex: HashMap<NodeId, VertexId> = HashMap::new();
    let mut visited: std::collections::HashSet<NodeId> = std::collections::HashSet::new();

    // Start from source nodes and build chains
    let sources = stream_graph.sources();
    for source in sources {
        build_chain_from(
            stream_graph,
            source,
            &mut job_graph,
            &mut node_to_vertex,
            &mut visited,
        );
    }

    // Handle any remaining nodes (shouldn't happen in a well-formed graph)
    for node_id in stream_graph.nodes.keys() {
        if !visited.contains(node_id) {
            build_chain_from(
                stream_graph,
                *node_id,
                &mut job_graph,
                &mut node_to_vertex,
                &mut visited,
            );
        }
    }

    // Map edges from StreamGraph to JobGraph
    for edge in &stream_graph.edges {
        let source_vertex = node_to_vertex[&edge.source];
        let target_vertex = node_to_vertex[&edge.target];

        // Only add edge if it connects different vertices (not chained)
        if source_vertex != target_vertex {
            job_graph.add_edge(source_vertex, target_vertex, edge.partition.clone());
        }
    }

    job_graph
}

/// Build a chain starting from a given node.
fn build_chain_from(
    stream_graph: &StreamGraph,
    start_node: NodeId,
    job_graph: &mut JobGraph,
    node_to_vertex: &mut HashMap<NodeId, VertexId>,
    visited: &mut std::collections::HashSet<NodeId>,
) {
    if visited.contains(&start_node) {
        return;
    }

    let mut chain = vec![OperatorDescriptor {
        node_id: start_node,
        operator_type: stream_graph.nodes[&start_node].operator_type.clone(),
    }];

    visited.insert(start_node);
    let mut current = start_node;

    // Greedily chain downstream operators
    loop {
        let downstream_nodes = stream_graph.downstream(current);

        // Try to chain with the first (and should be only) downstream if possible
        if downstream_nodes.len() == 1 {
            let next = downstream_nodes[0];

            if !visited.contains(&next) && can_chain(stream_graph, current, next) {
                chain.push(OperatorDescriptor {
                    node_id: next,
                    operator_type: stream_graph.nodes[&next].operator_type.clone(),
                });
                visited.insert(next);
                current = next;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    // Create a JobVertex for this chain
    let parallelism = stream_graph.nodes[&start_node].parallelism;
    let vertex_id = job_graph.add_vertex(chain.clone(), parallelism);

    // Map all nodes in the chain to this vertex
    for op in &chain {
        node_to_vertex.insert(op.node_id, vertex_id);
    }

    // Recursively process downstream nodes that weren't chained
    for downstream in stream_graph.downstream(current) {
        build_chain_from(stream_graph, downstream, job_graph, node_to_vertex, visited);
    }
}

#[cfg(test)]
#[path = "tests/job_graph_tests.rs"]
mod tests;
