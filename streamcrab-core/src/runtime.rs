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
mod tests {
    use super::*;
    use crate::graph::{OperatorType, Partition, StreamGraph};

    #[test]
    fn test_topo_sort_linear() {
        let mut g = StreamGraph::new();
        let a = g.add_node(OperatorType::Source, 1);
        let b = g.add_node(OperatorType::Map, 1);
        let c = g.add_node(OperatorType::Sink, 1);
        g.add_edge(a, b, Partition::Forward);
        g.add_edge(b, c, Partition::Forward);

        let order = topo_sort(&g);
        assert_eq!(order, vec![a, b, c]);
    }

    #[test]
    fn test_topo_sort_diamond() {
        let mut g = StreamGraph::new();
        let src = g.add_node(OperatorType::Source, 1);
        let left = g.add_node(OperatorType::Map, 1);
        let right = g.add_node(OperatorType::Filter, 1);
        let sink = g.add_node(OperatorType::Sink, 1);
        g.add_edge(src, left, Partition::Forward);
        g.add_edge(src, right, Partition::Forward);
        g.add_edge(left, sink, Partition::Forward);
        g.add_edge(right, sink, Partition::Forward);

        let order = topo_sort(&g);
        assert_eq!(order[0], src);
        assert_eq!(*order.last().unwrap(), sink);
        assert_eq!(order.len(), 4);
    }
}
