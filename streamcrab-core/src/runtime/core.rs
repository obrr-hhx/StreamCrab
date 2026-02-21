use std::collections::HashMap;

use crate::graph::StreamGraph;
use crate::types::NodeId;

/// Topological sort of the stream graph (Kahn's algorithm).
pub fn topo_sort(graph: &StreamGraph) -> Vec<NodeId> {
    let mut in_degree: HashMap<NodeId, usize> = HashMap::new();
    let mut adj: HashMap<NodeId, Vec<NodeId>> = HashMap::new();

    for &id in graph.nodes.keys() {
        in_degree.entry(id).or_insert(0);
        adj.entry(id).or_default();
    }
    for edge in &graph.edges {
        *in_degree.entry(edge.target).or_insert(0) += 1;
        adj.entry(edge.source).or_default().push(edge.target);
    }

    let mut queue: Vec<NodeId> = in_degree
        .iter()
        .filter(|entry| *entry.1 == 0)
        .map(|entry| *entry.0)
        .collect();
    queue.sort();

    let mut result = Vec::new();
    while let Some(node) = queue.pop() {
        result.push(node);
        if let Some(neighbors) = adj.get(&node) {
            for &next in neighbors {
                let deg = in_degree.get_mut(&next).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push(next);
                    queue.sort();
                }
            }
        }
    }
    result
}

/// Build adjacency list (downstream map) from the graph.
pub fn downstream_map(graph: &StreamGraph) -> HashMap<NodeId, Vec<NodeId>> {
    let mut adj: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for &id in graph.nodes.keys() {
        adj.entry(id).or_default();
    }
    for edge in &graph.edges {
        adj.entry(edge.source).or_default().push(edge.target);
    }
    adj
}

#[cfg(test)]
#[path = "tests/core_tests.rs"]
mod tests;
