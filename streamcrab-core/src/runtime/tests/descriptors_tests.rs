use super::*;

#[test]
fn test_job_plan_roundtrip() {
    let plan = JobPlan {
        job_name: "demo".to_string(),
        parallelism: 4,
        operators: vec![
            OperatorDescriptor::Source {
                source_id: "src".to_string(),
            },
            OperatorDescriptor::Map {
                udf_id: "map_v1".to_string(),
                config: vec![1, 2, 3],
            },
            OperatorDescriptor::Sink {
                sink_id: "sink".to_string(),
            },
        ],
        edges: vec![
            EdgePlan {
                source_node_id: 0,
                target_node_id: 1,
                partition: PartitionDescriptor::Forward,
            },
            EdgePlan {
                source_node_id: 1,
                target_node_id: 2,
                partition: PartitionDescriptor::Hash,
            },
        ],
    };

    let bytes = plan.to_bytes().unwrap();
    let restored = JobPlan::from_bytes(&bytes).unwrap();
    assert_eq!(restored, plan);
}
