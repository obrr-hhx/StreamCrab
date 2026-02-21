use crate::graph::job_graph::{JobGraph, OperatorDescriptor, build_job_graph, can_chain};
use crate::graph::{OperatorType, Partition, StreamGraph};

#[test]
fn test_job_vertex_creation() {
    let mut job_graph = JobGraph::new();
    let ops = vec![
        OperatorDescriptor {
            node_id: 0,
            operator_type: OperatorType::Source,
        },
        OperatorDescriptor {
            node_id: 1,
            operator_type: OperatorType::Map,
        },
    ];
    let vertex_id = job_graph.add_vertex(ops.clone(), 4);

    let vertex = &job_graph.vertices[&vertex_id];
    assert_eq!(vertex.parallelism, 4);
    assert_eq!(vertex.chained_operators.len(), 2);
}

#[test]
fn test_chaining_forward_same_parallelism() {
    // Source → Map → Filter (all Forward, parallelism=1)
    // Should chain into 1 JobVertex
    let mut stream_graph = StreamGraph::new();
    let source = stream_graph.add_node(OperatorType::Source, 1);
    let map = stream_graph.add_node(OperatorType::Map, 1);
    let filter = stream_graph.add_node(OperatorType::Filter, 1);

    stream_graph.add_edge(source, map, Partition::Forward);
    stream_graph.add_edge(map, filter, Partition::Forward);

    let job_graph = build_job_graph(&stream_graph);

    // Should be 1 vertex with 3 chained operators
    assert_eq!(job_graph.vertices.len(), 1);
    assert_eq!(job_graph.edges.len(), 0);

    let vertex = job_graph.vertices.values().next().unwrap();
    assert_eq!(vertex.chained_operators.len(), 3);
    assert_eq!(
        vertex.chained_operators[0].operator_type,
        OperatorType::Source
    );
    assert_eq!(vertex.chained_operators[1].operator_type, OperatorType::Map);
    assert_eq!(
        vertex.chained_operators[2].operator_type,
        OperatorType::Filter
    );
}

#[test]
fn test_no_chaining_across_keyby() {
    // Source → Map → KeyBy → Reduce
    // Should be 2 vertices: [Source, Map] and [KeyBy, Reduce]
    // Actually KeyBy itself is a boundary, so: [Source, Map], [KeyBy], [Reduce]
    let mut stream_graph = StreamGraph::new();
    let source = stream_graph.add_node(OperatorType::Source, 1);
    let map = stream_graph.add_node(OperatorType::Map, 1);
    let keyby = stream_graph.add_node(OperatorType::KeyBy, 4);
    let reduce = stream_graph.add_node(OperatorType::Reduce, 4);

    stream_graph.add_edge(source, map, Partition::Forward);
    stream_graph.add_edge(map, keyby, Partition::Hash);
    stream_graph.add_edge(keyby, reduce, Partition::Forward);

    let job_graph = build_job_graph(&stream_graph);

    // Should be 3 vertices: [Source, Map], [KeyBy], [Reduce]
    assert_eq!(job_graph.vertices.len(), 3);
    assert_eq!(job_graph.edges.len(), 2);
}

#[test]
fn test_no_chaining_different_parallelism() {
    // Source(p=1) → Map(p=4)
    // Cannot chain due to different parallelism
    let mut stream_graph = StreamGraph::new();
    let source = stream_graph.add_node(OperatorType::Source, 1);
    let map = stream_graph.add_node(OperatorType::Map, 4);

    stream_graph.add_edge(source, map, Partition::Hash);

    let job_graph = build_job_graph(&stream_graph);

    assert_eq!(job_graph.vertices.len(), 2);
    assert_eq!(job_graph.edges.len(), 1);
}

#[test]
fn test_no_chaining_hash_partition() {
    // Source → Map (Hash partition)
    // Cannot chain due to Hash partition
    let mut stream_graph = StreamGraph::new();
    let source = stream_graph.add_node(OperatorType::Source, 4);
    let map = stream_graph.add_node(OperatorType::Map, 4);

    stream_graph.add_edge(source, map, Partition::Hash);

    let job_graph = build_job_graph(&stream_graph);

    assert_eq!(job_graph.vertices.len(), 2);
    assert_eq!(job_graph.edges.len(), 1);
}

#[test]
fn test_can_chain_function() {
    let mut stream_graph = StreamGraph::new();
    let source = stream_graph.add_node(OperatorType::Source, 1);
    let map = stream_graph.add_node(OperatorType::Map, 1);
    let keyby = stream_graph.add_node(OperatorType::KeyBy, 4);

    stream_graph.add_edge(source, map, Partition::Forward);
    stream_graph.add_edge(map, keyby, Partition::Hash);

    // Source → Map: can chain (Forward, same parallelism)
    assert!(can_chain(&stream_graph, source, map));

    // Map → KeyBy: cannot chain (KeyBy is boundary)
    assert!(!can_chain(&stream_graph, map, keyby));
}
