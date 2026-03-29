use crate::graph::{OperatorType, Partition, StreamGraph};
use crate::state::StateMode;

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
    assert_eq!(g.nodes[&src].state_mode, StateMode::Local);
    assert!(g.nodes[&src].elastic_config.is_none());
}

#[test]
fn test_hash_partition_edge() {
    let mut g = StreamGraph::new();
    let src = g.add_node(OperatorType::Source, 1);
    let key = g.add_node(OperatorType::KeyBy, 4);
    g.add_edge(src, key, Partition::Hash);

    assert_eq!(g.edges[0].partition, Partition::Hash);
}

#[test]
fn test_stream_node_runtime_config_is_mutable() {
    let mut g = StreamGraph::new();
    let node = g.add_node(OperatorType::Reduce, 4);

    g.set_state_mode(
        node,
        StateMode::Tiered {
            state_service: "127.0.0.1:7000".to_string(),
        },
    );
    g.set_elastic_config(
        node,
        Some(crate::elastic::ElasticConfig {
            min_parallelism: 2,
            max_parallelism: 8,
            scale_policy: crate::elastic::ScalePolicy::Manual,
            cooldown: std::time::Duration::from_secs(60),
        }),
    );

    assert!(matches!(
        g.nodes[&node].state_mode,
        StateMode::Tiered { ref state_service } if state_service == "127.0.0.1:7000"
    ));
    assert!(g.nodes[&node].elastic_config.is_some());
}
