use crate::graph::{OperatorType, Partition, StreamGraph};

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
