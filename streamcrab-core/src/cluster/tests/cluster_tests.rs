use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tonic::Request;

use crate::cluster::rpc::job_manager_service_client::JobManagerServiceClient;
use crate::cluster::rpc::task_manager_service_server::TaskManagerService;
use crate::cluster::{
    ClusterConfig, HeartbeatConfig, JobManager, OperatorFactory, Resources, RoundRobinScheduler,
    TaskManager, TaskManagerRpc,
};
use crate::network::{FrameType, decode_stream_element, encode_stream_element, write_frame};
use crate::runtime::descriptors::{JobPlan, OperatorDescriptor, PartitionDescriptor};
use crate::types::StreamElement;

#[test]
fn test_proto_types_are_generated() {
    let req = crate::cluster::rpc::RegisterTmRequest {
        tm_id: "tm-1".to_string(),
        address: "127.0.0.1:7001".to_string(),
        num_slots: 4,
        resources: None,
    };
    assert_eq!(req.tm_id, "tm-1");

    let submit = crate::cluster::rpc::SubmitJobRequest {
        job_plan: vec![1, 2, 3],
        parallelism: 2,
    };
    assert_eq!(submit.parallelism, 2);
}

#[test]
fn test_scheduler_round_robin_with_slot_limit() {
    let jm = JobManager::new(ClusterConfig::default());
    jm.register_task_manager(
        "tm-a".to_string(),
        "127.0.0.1:7101".to_string(),
        1,
        Resources::default(),
    );
    jm.register_task_manager(
        "tm-b".to_string(),
        "127.0.0.1:7102".to_string(),
        2,
        Resources::default(),
    );

    let tasks = vec!["t1".to_string(), "t2".to_string(), "t3".to_string()];
    let mut scheduler = RoundRobinScheduler::new();
    let assigned = scheduler
        .schedule(
            &tasks,
            &jm.list_task_managers()
                .into_iter()
                .map(|s| {
                    (
                        s.tm_id.clone(),
                        crate::cluster::TaskManagerInfo::new(
                            s.tm_id,
                            s.address,
                            s.num_slots as usize,
                            Resources::default(),
                        ),
                    )
                })
                .collect(),
        )
        .unwrap();
    assert_eq!(assigned.len(), 3);
}

#[test]
fn test_scheduler_rejects_insufficient_slots() {
    let mut task_managers = std::collections::HashMap::new();
    task_managers.insert(
        "tm-1".to_string(),
        crate::cluster::TaskManagerInfo::new(
            "tm-1".to_string(),
            "127.0.0.1:7201".to_string(),
            1,
            Resources::default(),
        ),
    );

    let tasks = vec!["t1".to_string(), "t2".to_string()];
    let mut scheduler = RoundRobinScheduler::new();
    let err = scheduler.schedule(&tasks, &task_managers).unwrap_err();
    assert!(err.to_string().contains("insufficient slots"));
}

#[test]
fn test_job_manager_register_heartbeat_and_evict_stale() {
    let config = ClusterConfig {
        heartbeat: HeartbeatConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(5),
        },
    };
    let jm = JobManager::new(config);
    assert!(jm.register_task_manager(
        "tm-1".to_string(),
        "127.0.0.1:7301".to_string(),
        2,
        Resources::default(),
    ));
    assert_eq!(jm.task_manager_count(), 1);
    assert!(jm.heartbeat("tm-1"));

    std::thread::sleep(Duration::from_millis(8));
    let removed = jm.evict_stale_task_managers();
    assert_eq!(removed, vec!["tm-1".to_string()]);
    assert_eq!(jm.task_manager_count(), 0);
}

#[tokio::test]
async fn test_task_manager_register_retry_fails_on_unreachable_jm() {
    let tm = TaskManager::new(
        "tm-1".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:7401".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
        },
    );

    let err = tm
        .register_with_retry(2, Duration::from_millis(5))
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[tokio::test]
async fn test_task_manager_heartbeat_loop_ticks_even_when_jm_down() {
    let tm = Arc::new(TaskManager::new(
        "tm-2".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:7501".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
        },
    ));

    let (shutdown_tx, handle) = Arc::clone(&tm).spawn_heartbeat_loop();
    tokio::time::sleep(Duration::from_millis(35)).await;
    shutdown_tx.send(true).unwrap();
    handle.await.unwrap();
    assert!(tm.heartbeat_attempts() >= 2);
}

#[tokio::test]
async fn test_task_manager_rpc_deploy_and_cancel() {
    let tm = Arc::new(TaskManager::new(
        "tm-3".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:7601".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc = TaskManagerRpc::new(Arc::clone(&tm));

    let deploy = crate::cluster::rpc::TaskDeploymentDescriptor {
        task_id: "task-1".to_string(),
        vertex_id: 1,
        subtask_index: 0,
        operator_bytes: vec![],
        input_channels: vec![],
        output_channels: vec![],
        output_peer_tms: vec![],
    };
    let res = rpc.deploy_task(Request::new(deploy)).await.unwrap();
    assert!(res.into_inner().success);
    assert_eq!(tm.deployed_task_count(), 1);

    rpc.cancel_task(Request::new(crate::cluster::rpc::CancelTaskRequest {
        task_id: "task-1".to_string(),
    }))
    .await
    .unwrap();
    assert_eq!(tm.deployed_task_count(), 0);
}

#[tokio::test]
async fn test_task_manager_redeploy_same_task_replaces_channel_mapping() {
    let tm = Arc::new(TaskManager::new(
        "tm-redeploy".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:7602".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc = TaskManagerRpc::new(Arc::clone(&tm));

    rpc.deploy_task(Request::new(
        crate::cluster::rpc::TaskDeploymentDescriptor {
            task_id: "task-same".to_string(),
            vertex_id: 1,
            subtask_index: 0,
            operator_bytes: sample_job_plan_bytes(1),
            input_channels: vec![201],
            output_channels: vec![301],
            output_peer_tms: vec!["tm-peer".to_string()],
        },
    ))
    .await
    .unwrap();

    rpc.deploy_task(Request::new(
        crate::cluster::rpc::TaskDeploymentDescriptor {
            task_id: "task-same".to_string(),
            vertex_id: 1,
            subtask_index: 0,
            operator_bytes: vec![],
            input_channels: vec![202],
            output_channels: vec![],
            output_peer_tms: vec![],
        },
    ))
    .await
    .unwrap();

    let old_channel_err = tm.route_incoming_frame(
        encode_stream_element(201, StreamElement::record(vec![1u8])).unwrap(),
    );
    assert!(old_channel_err.is_err());

    tm.route_incoming_frame(encode_stream_element(202, StreamElement::record(vec![2u8])).unwrap())
        .unwrap();
    assert_eq!(
        tm.try_recv_bridged_input("task-same", 202).unwrap(),
        Some(StreamElement::record(vec![2u8]))
    );

    let reused_old_channel = rpc
        .deploy_task(Request::new(
            crate::cluster::rpc::TaskDeploymentDescriptor {
                task_id: "task-other".to_string(),
                vertex_id: 2,
                subtask_index: 0,
                operator_bytes: vec![],
                input_channels: vec![201],
                output_channels: vec![],
                output_peer_tms: vec![],
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(reused_old_channel.success);
}

#[test]
fn test_operator_factory_rejects_unknown_udf() {
    let factory = OperatorFactory::new();
    let plan = JobPlan {
        job_name: "test".to_string(),
        parallelism: 1,
        operators: vec![OperatorDescriptor::Map {
            udf_id: "udf::missing".to_string(),
            config: vec![],
        }],
        edges: vec![],
    };
    let err = factory.validate_job_plan(&plan).unwrap_err();
    assert!(err.to_string().contains("unknown udf_id"));
}

#[test]
fn test_operator_factory_accepts_builtin_udf() {
    let factory = OperatorFactory::new();
    let plan = JobPlan {
        job_name: "test".to_string(),
        parallelism: 1,
        operators: vec![OperatorDescriptor::Map {
            udf_id: "builtin::identity".to_string(),
            config: vec![],
        }],
        edges: vec![],
    };
    factory.validate_job_plan(&plan).unwrap();
}

#[tokio::test]
async fn test_task_manager_remote_bridge_preserves_order_and_channel_mapping() {
    let tm = Arc::new(TaskManager::new(
        "tm-4".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:7701".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc = TaskManagerRpc::new(Arc::clone(&tm));

    rpc.deploy_task(Request::new(
        crate::cluster::rpc::TaskDeploymentDescriptor {
            task_id: "task-bridge".to_string(),
            vertex_id: 1,
            subtask_index: 0,
            operator_bytes: vec![],
            input_channels: vec![11],
            output_channels: vec![21],
            output_peer_tms: vec!["tm-x".to_string()],
        },
    ))
    .await
    .unwrap();

    let sequence = vec![
        StreamElement::barrier_with_timestamp(3, 300),
        StreamElement::watermark(301),
        StreamElement::End,
    ];

    for element in sequence.clone() {
        let frame = encode_stream_element(11, element).unwrap();
        tm.ingest_remote_input_frame("task-bridge", frame).unwrap();
    }

    assert_eq!(
        tm.try_recv_bridged_input("task-bridge", 11).unwrap(),
        Some(sequence[0].clone())
    );
    assert_eq!(
        tm.try_recv_bridged_input("task-bridge", 11).unwrap(),
        Some(sequence[1].clone())
    );
    assert_eq!(
        tm.try_recv_bridged_input("task-bridge", 11).unwrap(),
        Some(sequence[2].clone())
    );
    assert_eq!(tm.try_recv_bridged_input("task-bridge", 11).unwrap(), None);

    let out = tm
        .encode_bridged_output("task-bridge", 0, StreamElement::End)
        .unwrap();
    assert_eq!(out.channel_id, 21);
    assert_eq!(out.frame_type, FrameType::End);
    let (_, decoded) = decode_stream_element(out).unwrap();
    assert_eq!(decoded, StreamElement::End);
}

#[tokio::test]
async fn test_task_manager_incoming_frame_pump_routes_into_task_bridge() {
    let tm = Arc::new(TaskManager::new(
        "tm-5".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:7801".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc = TaskManagerRpc::new(Arc::clone(&tm));

    rpc.deploy_task(Request::new(
        crate::cluster::rpc::TaskDeploymentDescriptor {
            task_id: "task-pump".to_string(),
            vertex_id: 2,
            subtask_index: 0,
            operator_bytes: vec![],
            input_channels: vec![31],
            output_channels: vec![41],
            output_peer_tms: vec!["tm-y".to_string()],
        },
    ))
    .await
    .unwrap();

    let (tx, rx) = mpsc::channel(8);
    let handle = Arc::clone(&tm).spawn_incoming_frame_pump(rx);

    for element in [
        StreamElement::barrier_with_timestamp(9, 900),
        StreamElement::watermark(901),
        StreamElement::End,
    ] {
        tx.send(encode_stream_element(31, element).unwrap())
            .await
            .unwrap();
    }
    drop(tx);
    handle.await.unwrap();

    assert_eq!(
        tm.try_recv_bridged_input("task-pump", 31).unwrap(),
        Some(StreamElement::barrier_with_timestamp(9, 900))
    );
    assert_eq!(
        tm.try_recv_bridged_input("task-pump", 31).unwrap(),
        Some(StreamElement::watermark(901))
    );
    assert_eq!(
        tm.try_recv_bridged_input("task-pump", 31).unwrap(),
        Some(StreamElement::End)
    );
}

#[tokio::test]
async fn test_task_manager_rejects_duplicate_input_channel_assignment() {
    let tm = Arc::new(TaskManager::new(
        "tm-6".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:7901".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc = TaskManagerRpc::new(Arc::clone(&tm));

    rpc.deploy_task(Request::new(
        crate::cluster::rpc::TaskDeploymentDescriptor {
            task_id: "task-a".to_string(),
            vertex_id: 3,
            subtask_index: 0,
            operator_bytes: vec![],
            input_channels: vec![55],
            output_channels: vec![],
            output_peer_tms: vec![],
        },
    ))
    .await
    .unwrap();

    let err = rpc
        .deploy_task(Request::new(
            crate::cluster::rpc::TaskDeploymentDescriptor {
                task_id: "task-b".to_string(),
                vertex_id: 3,
                subtask_index: 1,
                operator_bytes: vec![],
                input_channels: vec![55],
                output_channels: vec![],
                output_peer_tms: vec![],
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::AlreadyExists);
}

#[tokio::test]
async fn test_task_manager_connect_data_plane_peer_routes_socket_frames() {
    let tm = Arc::new(TaskManager::new(
        "tm-7".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:8001".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc = TaskManagerRpc::new(Arc::clone(&tm));
    rpc.deploy_task(Request::new(
        crate::cluster::rpc::TaskDeploymentDescriptor {
            task_id: "task-socket".to_string(),
            vertex_id: 4,
            subtask_index: 0,
            operator_bytes: vec![],
            input_channels: vec![71],
            output_channels: vec![],
            output_peer_tms: vec![],
        },
    ))
    .await
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (_reader, mut writer) = stream.into_split();
        write_frame(
            &mut writer,
            &encode_stream_element(71, StreamElement::barrier_with_timestamp(12, 1200)).unwrap(),
        )
        .await
        .unwrap();
        write_frame(
            &mut writer,
            &encode_stream_element(71, StreamElement::watermark(1201)).unwrap(),
        )
        .await
        .unwrap();
        write_frame(
            &mut writer,
            &encode_stream_element(71, StreamElement::End).unwrap(),
        )
        .await
        .unwrap();
    });

    Arc::clone(&tm)
        .connect_data_plane_peer("peer-upstream".to_string(), addr)
        .await
        .unwrap();
    server.await.unwrap();

    let first = wait_bridged_input(&tm, "task-socket", 71).await;
    let second = wait_bridged_input(&tm, "task-socket", 71).await;
    let third = wait_bridged_input(&tm, "task-socket", 71).await;

    assert_eq!(first, StreamElement::barrier_with_timestamp(12, 1200));
    assert_eq!(second, StreamElement::watermark(1201));
    assert_eq!(third, StreamElement::End);
}

#[tokio::test]
async fn test_task_manager_forwards_bridged_output_to_peer_task() {
    let tm_up = Arc::new(TaskManager::new(
        "tm-up".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:8101".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let tm_down = Arc::new(TaskManager::new(
        "tm-down".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:8102".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc_up = TaskManagerRpc::new(Arc::clone(&tm_up));
    let rpc_down = TaskManagerRpc::new(Arc::clone(&tm_down));

    rpc_up
        .deploy_task(Request::new(
            crate::cluster::rpc::TaskDeploymentDescriptor {
                task_id: "task-up".to_string(),
                vertex_id: 10,
                subtask_index: 0,
                operator_bytes: vec![],
                input_channels: vec![],
                output_channels: vec![91],
                output_peer_tms: vec!["tm-down".to_string()],
            },
        ))
        .await
        .unwrap();
    rpc_down
        .deploy_task(Request::new(
            crate::cluster::rpc::TaskDeploymentDescriptor {
                task_id: "task-down".to_string(),
                vertex_id: 11,
                subtask_index: 0,
                operator_bytes: vec![],
                input_channels: vec![91],
                output_channels: vec![],
                output_peer_tms: vec![],
            },
        ))
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tm_down_accept = Arc::clone(&tm_down);
    let accept_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tm_down_accept
            .attach_incoming_data_plane_stream("tm-up".to_string(), stream)
            .await
            .unwrap();
    });

    Arc::clone(&tm_up)
        .connect_data_plane_peer("tm-down".to_string(), addr)
        .await
        .unwrap();
    accept_handle.await.unwrap();

    tm_up
        .forward_bridged_output_to_peer("task-up", 0, "tm-down", StreamElement::record(vec![7u8]))
        .await
        .unwrap();
    tm_up
        .forward_bridged_output_to_peer("task-up", 0, "tm-down", StreamElement::watermark(500))
        .await
        .unwrap();
    tm_up
        .forward_bridged_output_to_peer(
            "task-up",
            0,
            "tm-down",
            StreamElement::barrier_with_timestamp(5, 500),
        )
        .await
        .unwrap();
    tm_up
        .forward_bridged_output_to_peer("task-up", 0, "tm-down", StreamElement::End)
        .await
        .unwrap();

    let mut received = vec![
        wait_bridged_input(&tm_down, "task-down", 91).await,
        wait_bridged_input(&tm_down, "task-down", 91).await,
        wait_bridged_input(&tm_down, "task-down", 91).await,
        wait_bridged_input(&tm_down, "task-down", 91).await,
    ];

    assert_contains_and_remove(&mut received, &StreamElement::record(vec![7u8]));
    assert_contains_and_remove(&mut received, &StreamElement::watermark(500));
    assert_contains_and_remove(
        &mut received,
        &StreamElement::barrier_with_timestamp(5, 500),
    );
    assert_contains_and_remove(&mut received, &StreamElement::End);
    assert!(received.is_empty());
}

#[tokio::test]
async fn test_task_manager_deploy_starts_runner_and_forwards_automatically() {
    let tm_up = Arc::new(TaskManager::new(
        "tm-runner-up".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:8301".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let tm_down = Arc::new(TaskManager::new(
        "tm-runner-down".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:8302".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc_up = TaskManagerRpc::new(Arc::clone(&tm_up));
    let rpc_down = TaskManagerRpc::new(Arc::clone(&tm_down));

    rpc_up
        .deploy_task(Request::new(
            crate::cluster::rpc::TaskDeploymentDescriptor {
                task_id: "task-runner-up".to_string(),
                vertex_id: 20,
                subtask_index: 0,
                operator_bytes: sample_job_plan_bytes(1),
                input_channels: vec![99],
                output_channels: vec![91],
                output_peer_tms: vec!["tm-runner-down".to_string()],
            },
        ))
        .await
        .unwrap();
    rpc_down
        .deploy_task(Request::new(
            crate::cluster::rpc::TaskDeploymentDescriptor {
                task_id: "task-runner-down".to_string(),
                vertex_id: 21,
                subtask_index: 0,
                operator_bytes: vec![],
                input_channels: vec![91],
                output_channels: vec![],
                output_peer_tms: vec![],
            },
        ))
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tm_down_accept = Arc::clone(&tm_down);
    let accept_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tm_down_accept
            .attach_incoming_data_plane_stream("tm-runner-up".to_string(), stream)
            .await
            .unwrap();
    });
    Arc::clone(&tm_up)
        .connect_data_plane_peer("tm-runner-down".to_string(), addr)
        .await
        .unwrap();
    accept_handle.await.unwrap();

    tm_up
        .ingest_remote_input_frame(
            "task-runner-up",
            encode_stream_element(99, StreamElement::record(vec![42u8])).unwrap(),
        )
        .unwrap();

    let received = wait_bridged_input(&tm_down, "task-runner-down", 91).await;
    assert_eq!(received, StreamElement::record(vec![42u8]));
}

#[tokio::test]
async fn test_task_manager_checkpoint_notify_records_completion() {
    let tm = Arc::new(TaskManager::new(
        "tm-8".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:8201".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc = TaskManagerRpc::new(Arc::clone(&tm));

    rpc.deploy_task(Request::new(
        crate::cluster::rpc::TaskDeploymentDescriptor {
            task_id: "task-checkpoint".to_string(),
            vertex_id: 12,
            subtask_index: 0,
            operator_bytes: vec![],
            input_channels: vec![],
            output_channels: vec![],
            output_peer_tms: vec![],
        },
    ))
    .await
    .unwrap();

    let err = rpc
        .trigger_checkpoint(Request::new(crate::cluster::rpc::CheckpointBarrier {
            checkpoint_id: 99,
            timestamp: 1000,
            job_id: String::new(),
            task_ids: vec![],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert_eq!(tm.triggered_checkpoint_ids(), vec![99]);

    rpc.notify_checkpoint_complete(Request::new(crate::cluster::rpc::CheckpointComplete {
        checkpoint_id: 99,
    }))
    .await
    .unwrap();
    assert_eq!(tm.completed_checkpoint_ids(), vec![99]);
}

#[tokio::test]
async fn test_cluster_checkpoint_end_to_end_with_two_tms() {
    let jm_addr = reserve_local_addr().await;
    let jm_endpoint = format!("http://{}", jm_addr);
    let jm = Arc::new(JobManager::new(ClusterConfig::default()));
    let jm_handle = tokio::spawn(Arc::clone(&jm).serve(jm_addr));

    let tm1_addr = reserve_local_addr().await;
    let tm2_addr = reserve_local_addr().await;
    let tm1 = Arc::new(TaskManager::new(
        "tm-e2e-1".to_string(),
        jm_endpoint.clone(),
        tm1_addr.to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let tm2 = Arc::new(TaskManager::new(
        "tm-e2e-2".to_string(),
        jm_endpoint.clone(),
        tm2_addr.to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let tm1_handle = tokio::spawn(Arc::clone(&tm1).serve(tm1_addr));
    let tm2_handle = tokio::spawn(Arc::clone(&tm2).serve(tm2_addr));

    tm1.register_with_retry(20, Duration::from_millis(20))
        .await
        .unwrap();
    tm2.register_with_retry(20, Duration::from_millis(20))
        .await
        .unwrap();

    let mut client = JobManagerServiceClient::connect(jm_endpoint.clone())
        .await
        .unwrap();
    let submit = client
        .submit_job(Request::new(crate::cluster::rpc::SubmitJobRequest {
            job_plan: sample_job_plan_bytes(2),
            parallelism: 2,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(submit.accepted);
    let job_id = submit.job_id.clone();
    assert!(!job_id.is_empty());

    wait_until(Duration::from_secs(2), || {
        tm1.deployed_task_count() + tm2.deployed_task_count() == 2
    })
    .await;

    let cp = client
        .trigger_checkpoint(Request::new(
            crate::cluster::rpc::TriggerCheckpointRequest { job_id },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(cp.accepted);
    assert!(cp.checkpoint_id > 0);

    wait_until(Duration::from_secs(2), || {
        tm1.completed_checkpoint_ids().contains(&cp.checkpoint_id)
            && tm2.completed_checkpoint_ids().contains(&cp.checkpoint_id)
    })
    .await;

    tm1_handle.abort();
    tm2_handle.abort();
    jm_handle.abort();
}

#[tokio::test]
async fn test_cluster_global_rollback_on_tm_failure() {
    let jm_addr = reserve_local_addr().await;
    let jm_endpoint = format!("http://{}", jm_addr);
    let jm = Arc::new(JobManager::new(ClusterConfig {
        heartbeat: HeartbeatConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(40),
        },
    }));
    let jm_handle = tokio::spawn(Arc::clone(&jm).serve(jm_addr));

    let tm_alive_addr = reserve_local_addr().await;
    let tm_fail_addr = reserve_local_addr().await;
    let tm_alive = Arc::new(TaskManager::new(
        "tm-recover-1".to_string(),
        jm_endpoint.clone(),
        tm_alive_addr.to_string(),
        4,
        Resources::default(),
        HeartbeatConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(40),
        },
    ));
    let tm_fail = Arc::new(TaskManager::new(
        "tm-recover-2".to_string(),
        jm_endpoint.clone(),
        tm_fail_addr.to_string(),
        1,
        Resources::default(),
        HeartbeatConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_millis(40),
        },
    ));
    let tm_alive_handle = tokio::spawn(Arc::clone(&tm_alive).serve(tm_alive_addr));
    let tm_fail_handle = tokio::spawn(Arc::clone(&tm_fail).serve(tm_fail_addr));

    tm_alive
        .register_with_retry(20, Duration::from_millis(20))
        .await
        .unwrap();
    tm_fail
        .register_with_retry(20, Duration::from_millis(20))
        .await
        .unwrap();

    let mut client = JobManagerServiceClient::connect(jm_endpoint.clone())
        .await
        .unwrap();
    let submit = client
        .submit_job(Request::new(crate::cluster::rpc::SubmitJobRequest {
            job_plan: sample_job_plan_bytes(2),
            parallelism: 2,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(submit.accepted);
    let job_id = submit.job_id;

    wait_until(Duration::from_secs(2), || {
        tm_alive.deployed_task_count() + tm_fail.deployed_task_count() == 2
    })
    .await;

    tm_fail_handle.abort();
    tokio::time::sleep(Duration::from_millis(60)).await;
    tm_alive.send_heartbeat().await.unwrap();

    wait_until(Duration::from_secs(2), || {
        tm_alive.deployed_task_count() >= 2
    })
    .await;

    let status = client
        .get_job_status(Request::new(crate::cluster::rpc::GetJobStatusRequest {
            job_id: job_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        status.status,
        i32::from(crate::cluster::JobStatus::Running),
        "job should recover to running"
    );

    tm_alive_handle.abort();
    jm_handle.abort();
}

#[tokio::test]
async fn test_cluster_multi_job_task_ids_do_not_collide_on_same_tm() {
    let jm_addr = reserve_local_addr().await;
    let jm_endpoint = format!("http://{}", jm_addr);
    let jm = Arc::new(JobManager::new(ClusterConfig::default()));
    let jm_handle = tokio::spawn(Arc::clone(&jm).serve(jm_addr));

    let tm_addr = reserve_local_addr().await;
    let tm = Arc::new(TaskManager::new(
        "tm-multi".to_string(),
        jm_endpoint.clone(),
        tm_addr.to_string(),
        8,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let tm_handle = tokio::spawn(Arc::clone(&tm).serve(tm_addr));
    tm.register_with_retry(20, Duration::from_millis(20))
        .await
        .unwrap();

    let mut client = JobManagerServiceClient::connect(jm_endpoint.clone())
        .await
        .unwrap();

    let job1 = client
        .submit_job(Request::new(crate::cluster::rpc::SubmitJobRequest {
            job_plan: sample_job_plan_bytes(2),
            parallelism: 2,
        }))
        .await
        .unwrap()
        .into_inner()
        .job_id;
    let job2 = client
        .submit_job(Request::new(crate::cluster::rpc::SubmitJobRequest {
            job_plan: sample_job_plan_bytes(2),
            parallelism: 2,
        }))
        .await
        .unwrap()
        .into_inner()
        .job_id;

    wait_until(Duration::from_secs(2), || tm.deployed_task_count() == 4).await;

    let cp1 = client
        .trigger_checkpoint(Request::new(
            crate::cluster::rpc::TriggerCheckpointRequest { job_id: job1 },
        ))
        .await
        .unwrap()
        .into_inner();
    let cp2 = client
        .trigger_checkpoint(Request::new(
            crate::cluster::rpc::TriggerCheckpointRequest { job_id: job2 },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(cp1.accepted);
    assert!(cp2.accepted);
    assert_ne!(cp1.checkpoint_id, cp2.checkpoint_id);

    wait_until(Duration::from_secs(2), || {
        tm.completed_checkpoint_ids().contains(&cp1.checkpoint_id)
            && tm.completed_checkpoint_ids().contains(&cp2.checkpoint_id)
    })
    .await;

    tm_handle.abort();
    jm_handle.abort();
}

#[tokio::test]
async fn test_cluster_interleaved_high_frequency_checkpoints_multi_job() {
    let jm_addr = reserve_local_addr().await;
    let jm_endpoint = format!("http://{}", jm_addr);
    let jm = Arc::new(JobManager::new(ClusterConfig::default()));
    let jm_handle = tokio::spawn(Arc::clone(&jm).serve(jm_addr));

    let tm1_addr = reserve_local_addr().await;
    let tm2_addr = reserve_local_addr().await;
    let tm1 = Arc::new(TaskManager::new(
        "tm-hf-1".to_string(),
        jm_endpoint.clone(),
        tm1_addr.to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let tm2 = Arc::new(TaskManager::new(
        "tm-hf-2".to_string(),
        jm_endpoint.clone(),
        tm2_addr.to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let tm1_handle = tokio::spawn(Arc::clone(&tm1).serve(tm1_addr));
    let tm2_handle = tokio::spawn(Arc::clone(&tm2).serve(tm2_addr));

    tm1.register_with_retry(20, Duration::from_millis(20))
        .await
        .unwrap();
    tm2.register_with_retry(20, Duration::from_millis(20))
        .await
        .unwrap();

    let mut client = JobManagerServiceClient::connect(jm_endpoint.clone())
        .await
        .unwrap();
    let job1 = client
        .submit_job(Request::new(crate::cluster::rpc::SubmitJobRequest {
            job_plan: sample_job_plan_bytes(2),
            parallelism: 2,
        }))
        .await
        .unwrap()
        .into_inner()
        .job_id;
    let job2 = client
        .submit_job(Request::new(crate::cluster::rpc::SubmitJobRequest {
            job_plan: sample_job_plan_bytes(2),
            parallelism: 2,
        }))
        .await
        .unwrap()
        .into_inner()
        .job_id;

    wait_until(Duration::from_secs(2), || {
        tm1.deployed_task_count() + tm2.deployed_task_count() == 4
    })
    .await;

    let mut checkpoint_ids = Vec::new();
    for _ in 0..12 {
        let cp1 = client
            .trigger_checkpoint(Request::new(
                crate::cluster::rpc::TriggerCheckpointRequest {
                    job_id: job1.clone(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let cp2 = client
            .trigger_checkpoint(Request::new(
                crate::cluster::rpc::TriggerCheckpointRequest {
                    job_id: job2.clone(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(cp1.accepted);
        assert!(cp2.accepted);
        checkpoint_ids.push(cp1.checkpoint_id);
        checkpoint_ids.push(cp2.checkpoint_id);
    }

    wait_until(Duration::from_secs(2), || {
        jm.pending_checkpoint_count().unwrap() == 0
    })
    .await;
    assert!(jm.aborted_checkpoint_ids().unwrap().is_empty());

    wait_until(Duration::from_secs(2), || {
        checkpoint_ids
            .iter()
            .all(|id| tm1.completed_checkpoint_ids().contains(id))
            && checkpoint_ids
                .iter()
                .all(|id| tm2.completed_checkpoint_ids().contains(id))
    })
    .await;

    tm1_handle.abort();
    tm2_handle.abort();
    jm_handle.abort();
}

#[tokio::test]
async fn test_trigger_checkpoint_dispatch_failure_aborts_pending_checkpoint() {
    let jm = JobManager::new(ClusterConfig::default());
    let job_id = jm.submit_job(1).unwrap();

    let err = jm
        .trigger_checkpoint_and_dispatch(&job_id)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no deployed tasks"));
    assert_eq!(jm.pending_checkpoint_count().unwrap(), 0);
    assert_eq!(jm.aborted_checkpoint_ids().unwrap().len(), 1);
}

#[tokio::test]
async fn test_report_failure_marks_only_target_job() {
    let jm_addr = reserve_local_addr().await;
    let jm_endpoint = format!("http://{}", jm_addr);
    let jm = Arc::new(JobManager::new(ClusterConfig::default()));
    let jm_handle = tokio::spawn(Arc::clone(&jm).serve(jm_addr));

    let tm_addr = reserve_local_addr().await;
    let tm = Arc::new(TaskManager::new(
        "tm-failure".to_string(),
        jm_endpoint.clone(),
        tm_addr.to_string(),
        8,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let tm_handle = tokio::spawn(Arc::clone(&tm).serve(tm_addr));
    tm.register_with_retry(20, Duration::from_millis(20))
        .await
        .unwrap();

    let mut client = JobManagerServiceClient::connect(jm_endpoint.clone())
        .await
        .unwrap();
    let job1 = client
        .submit_job(Request::new(crate::cluster::rpc::SubmitJobRequest {
            job_plan: sample_job_plan_bytes(2),
            parallelism: 2,
        }))
        .await
        .unwrap()
        .into_inner()
        .job_id;
    let job2 = client
        .submit_job(Request::new(crate::cluster::rpc::SubmitJobRequest {
            job_plan: sample_job_plan_bytes(2),
            parallelism: 2,
        }))
        .await
        .unwrap()
        .into_inner()
        .job_id;

    wait_until(Duration::from_secs(2), || tm.deployed_task_count() == 4).await;

    client
        .report_failure(Request::new(crate::cluster::rpc::FailureReport {
            tm_id: "tm-failure".to_string(),
            task_id: format!("{job1}::vertex_0_0"),
            reason: "unit-test".to_string(),
        }))
        .await
        .unwrap();

    let status1 = client
        .get_job_status(Request::new(crate::cluster::rpc::GetJobStatusRequest {
            job_id: job1.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    let status2 = client
        .get_job_status(Request::new(crate::cluster::rpc::GetJobStatusRequest {
            job_id: job2.clone(),
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(status1.status, i32::from(crate::cluster::JobStatus::Failed));
    assert_eq!(
        status2.status,
        i32::from(crate::cluster::JobStatus::Running)
    );

    tm_handle.abort();
    jm_handle.abort();
}

#[tokio::test]
async fn test_task_manager_data_plane_reconnect_preserves_stream_continuity() {
    let tm_up = Arc::new(TaskManager::new(
        "tm-reconnect-up".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:8401".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let tm_down = Arc::new(TaskManager::new(
        "tm-reconnect-down".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:8402".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc_up = TaskManagerRpc::new(Arc::clone(&tm_up));
    let rpc_down = TaskManagerRpc::new(Arc::clone(&tm_down));

    rpc_up
        .deploy_task(Request::new(
            crate::cluster::rpc::TaskDeploymentDescriptor {
                task_id: "task-reconnect-up".to_string(),
                vertex_id: 30,
                subtask_index: 0,
                operator_bytes: vec![],
                input_channels: vec![],
                output_channels: vec![321],
                output_peer_tms: vec!["tm-reconnect-down".to_string()],
            },
        ))
        .await
        .unwrap();
    rpc_down
        .deploy_task(Request::new(
            crate::cluster::rpc::TaskDeploymentDescriptor {
                task_id: "task-reconnect-down".to_string(),
                vertex_id: 31,
                subtask_index: 0,
                operator_bytes: vec![],
                input_channels: vec![321],
                output_channels: vec![],
                output_peer_tms: vec![],
            },
        ))
        .await
        .unwrap();

    establish_data_plane_link(
        &tm_up,
        &tm_down,
        "tm-reconnect-down".to_string(),
        "tm-reconnect-up".to_string(),
    )
    .await;
    tm_up
        .forward_bridged_output_to_peer(
            "task-reconnect-up",
            0,
            "tm-reconnect-down",
            StreamElement::record(vec![1u8]),
        )
        .await
        .unwrap();
    tm_up
        .forward_bridged_output_to_peer(
            "task-reconnect-up",
            0,
            "tm-reconnect-down",
            StreamElement::watermark(100),
        )
        .await
        .unwrap();

    let mut before_reconnect = vec![
        wait_bridged_input(&tm_down, "task-reconnect-down", 321).await,
        wait_bridged_input(&tm_down, "task-reconnect-down", 321).await,
    ];
    assert_contains_and_remove(&mut before_reconnect, &StreamElement::record(vec![1u8]));
    assert_contains_and_remove(&mut before_reconnect, &StreamElement::watermark(100));
    assert!(before_reconnect.is_empty());

    establish_data_plane_link(
        &tm_up,
        &tm_down,
        "tm-reconnect-down".to_string(),
        "tm-reconnect-up".to_string(),
    )
    .await;
    tm_up
        .forward_bridged_output_to_peer(
            "task-reconnect-up",
            0,
            "tm-reconnect-down",
            StreamElement::barrier_with_timestamp(7, 700),
        )
        .await
        .unwrap();
    tm_up
        .forward_bridged_output_to_peer(
            "task-reconnect-up",
            0,
            "tm-reconnect-down",
            StreamElement::record(vec![2u8]),
        )
        .await
        .unwrap();
    tm_up
        .forward_bridged_output_to_peer(
            "task-reconnect-up",
            0,
            "tm-reconnect-down",
            StreamElement::End,
        )
        .await
        .unwrap();

    let mut after_reconnect = vec![
        wait_bridged_input(&tm_down, "task-reconnect-down", 321).await,
        wait_bridged_input(&tm_down, "task-reconnect-down", 321).await,
        wait_bridged_input(&tm_down, "task-reconnect-down", 321).await,
    ];
    assert_contains_and_remove(
        &mut after_reconnect,
        &StreamElement::barrier_with_timestamp(7, 700),
    );
    assert_contains_and_remove(&mut after_reconnect, &StreamElement::record(vec![2u8]));
    assert_contains_and_remove(&mut after_reconnect, &StreamElement::End);
    assert!(after_reconnect.is_empty());
}

#[tokio::test]
async fn test_task_manager_hot_switch_keeps_old_and_new_inflight_frames() {
    let tm = Arc::new(TaskManager::new(
        "tm-hot-switch".to_string(),
        "http://127.0.0.1:9".to_string(),
        "127.0.0.1:8501".to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc = TaskManagerRpc::new(Arc::clone(&tm));
    rpc.deploy_task(Request::new(
        crate::cluster::rpc::TaskDeploymentDescriptor {
            task_id: "task-hot-switch".to_string(),
            vertex_id: 40,
            subtask_index: 0,
            operator_bytes: vec![],
            input_channels: vec![451],
            output_channels: vec![],
            output_peer_tms: vec![],
        },
    ))
    .await
    .unwrap();

    let mut old_writer = attach_incoming_writer(&tm, "peer-hot-switch".to_string(), "first").await;
    write_frame(
        &mut old_writer,
        &encode_stream_element(451, StreamElement::record(vec![11u8])).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        wait_bridged_input(&tm, "task-hot-switch", 451).await,
        StreamElement::record(vec![11u8])
    );

    let mut new_writer = attach_incoming_writer(&tm, "peer-hot-switch".to_string(), "second").await;
    assert!(
        tm.incoming_pump_count_for_peer("peer-hot-switch") >= 2,
        "expected both old and new pump alive during hot switch"
    );

    write_frame(
        &mut new_writer,
        &encode_stream_element(451, StreamElement::barrier_with_timestamp(11, 1100)).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        tm.try_recv_bridged_input("task-hot-switch", 451).unwrap(),
        None,
        "pending generation must not forward before old generation closes"
    );
    write_frame(
        &mut old_writer,
        &encode_stream_element(451, StreamElement::watermark(1100)).unwrap(),
    )
    .await
    .unwrap();
    old_writer.shutdown().await.unwrap();
    write_frame(
        &mut new_writer,
        &encode_stream_element(451, StreamElement::End).unwrap(),
    )
    .await
    .unwrap();

    let mut got = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while got.len() < 3 && tokio::time::Instant::now() < deadline {
        if let Some(elem) = tm.try_recv_bridged_input("task-hot-switch", 451).unwrap() {
            got.push(elem);
            continue;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(got.len(), 3, "expected 3 elements after generation switch");
    assert_contains_and_remove(&mut got, &StreamElement::watermark(1100));
    assert_contains_and_remove(&mut got, &StreamElement::barrier_with_timestamp(11, 1100));
    assert_contains_and_remove(&mut got, &StreamElement::End);
    assert!(got.is_empty());
}

#[tokio::test]
async fn test_task_manager_trigger_checkpoint_deduplicates_task_ids() {
    let jm_addr = reserve_local_addr().await;
    let jm_endpoint = format!("http://{}", jm_addr);
    let jm = Arc::new(JobManager::new(ClusterConfig::default()));
    let jm_handle = tokio::spawn(Arc::clone(&jm).serve(jm_addr));

    let tm_addr = reserve_local_addr().await;
    let tm = Arc::new(TaskManager::new(
        "tm-dedup".to_string(),
        jm_endpoint.clone(),
        tm_addr.to_string(),
        2,
        Resources::default(),
        HeartbeatConfig::default(),
    ));
    let rpc_tm = TaskManagerRpc::new(Arc::clone(&tm));
    let tm_handle = tokio::spawn(Arc::clone(&tm).serve(tm_addr));
    tm.register_with_retry(20, Duration::from_millis(20))
        .await
        .unwrap();

    let mut client = JobManagerServiceClient::connect(jm_endpoint.clone())
        .await
        .unwrap();
    let submit = client
        .submit_job(Request::new(crate::cluster::rpc::SubmitJobRequest {
            job_plan: sample_job_plan_bytes(1),
            parallelism: 1,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(submit.accepted);
    let job_id = submit.job_id;

    wait_until(Duration::from_secs(2), || tm.deployed_task_count() == 1).await;

    let checkpoint_id = jm.trigger_checkpoint(&job_id).unwrap();
    let task_id = format!("{job_id}::vertex_0_0");
    rpc_tm
        .trigger_checkpoint(Request::new(crate::cluster::rpc::CheckpointBarrier {
            checkpoint_id,
            timestamp: 1234,
            job_id: job_id.clone(),
            task_ids: vec![task_id.clone(), task_id],
        }))
        .await
        .unwrap();

    wait_until(Duration::from_secs(2), || {
        tm.completed_checkpoint_ids().contains(&checkpoint_id)
    })
    .await;

    tm_handle.abort();
    jm_handle.abort();
}

fn assert_contains_and_remove(
    elements: &mut Vec<StreamElement<Vec<u8>>>,
    target: &StreamElement<Vec<u8>>,
) {
    let index = elements
        .iter()
        .position(|e| e == target)
        .unwrap_or_else(|| panic!("target element not found: {:?}", target));
    elements.remove(index);
}

async fn wait_bridged_input(
    tm: &TaskManager,
    task_id: &str,
    channel_id: u32,
) -> StreamElement<Vec<u8>> {
    for _ in 0..40 {
        if let Some(elem) = tm.try_recv_bridged_input(task_id, channel_id).unwrap() {
            return elem;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting bridged input for task={task_id} channel={channel_id}");
}

fn sample_job_plan_bytes(parallelism: u32) -> Vec<u8> {
    JobPlan {
        job_name: "e2e".to_string(),
        parallelism,
        operators: vec![
            OperatorDescriptor::Source {
                source_id: "source-1".to_string(),
            },
            OperatorDescriptor::Map {
                udf_id: "builtin::identity".to_string(),
                config: vec![],
            },
            OperatorDescriptor::Sink {
                sink_id: "sink-1".to_string(),
            },
        ],
        edges: vec![
            crate::runtime::descriptors::EdgePlan {
                source_node_id: 0,
                target_node_id: 1,
                partition: PartitionDescriptor::Forward,
            },
            crate::runtime::descriptors::EdgePlan {
                source_node_id: 1,
                target_node_id: 2,
                partition: PartitionDescriptor::Forward,
            },
        ],
    }
    .to_bytes()
    .unwrap()
}

async fn reserve_local_addr() -> std::net::SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn establish_data_plane_link(
    tm_up: &Arc<TaskManager>,
    tm_down: &Arc<TaskManager>,
    down_peer_id_on_up: String,
    up_peer_id_on_down: String,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tm_down_accept = Arc::clone(tm_down);
    let accept_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tm_down_accept
            .attach_incoming_data_plane_stream(up_peer_id_on_down, stream)
            .await
            .unwrap();
    });
    Arc::clone(tm_up)
        .connect_data_plane_peer(down_peer_id_on_up, addr)
        .await
        .unwrap();
    accept_handle.await.unwrap();
}

async fn attach_incoming_writer(
    tm: &Arc<TaskManager>,
    peer_id: String,
    _tag: &str,
) -> tokio::net::tcp::OwnedWriteHalf {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tm_accept = Arc::clone(tm);
    let accept_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tm_accept
            .attach_incoming_data_plane_stream(peer_id, stream)
            .await
            .unwrap();
    });
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (_, writer) = stream.into_split();
    accept_handle.await.unwrap();
    writer
}

async fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let start = tokio::time::Instant::now();
    loop {
        if predicate() {
            return;
        }
        if start.elapsed() > timeout {
            panic!("wait_until timeout after {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
