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
