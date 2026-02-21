use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use streamcrab_core::cluster::rpc::job_manager_service_client::JobManagerServiceClient;
use streamcrab_core::cluster::{
    ClusterConfig, HeartbeatConfig, JobManager, Resources, TaskManager, rpc,
};
use tonic::Request;

#[derive(Parser, Debug)]
#[command(name = "streamcrab")]
#[command(about = "StreamCrab cluster CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Jobmanager {
        #[arg(long, default_value = "127.0.0.1:6123")]
        listen: SocketAddr,
        #[arg(long, default_value_t = 5000)]
        heartbeat_interval_ms: u64,
        #[arg(long, default_value_t = 30000)]
        heartbeat_timeout_ms: u64,
    },
    Taskmanager {
        #[arg(long)]
        jm: String,
        #[arg(long, default_value = "127.0.0.1:6124")]
        listen: SocketAddr,
        #[arg(long, default_value_t = 4)]
        slots: usize,
        #[arg(long, default_value = "tm-local")]
        tm_id: String,
        #[arg(long, default_value_t = 5000)]
        heartbeat_interval_ms: u64,
        #[arg(long, default_value_t = 30000)]
        heartbeat_timeout_ms: u64,
    },
    Submit {
        #[arg(long)]
        jm: String,
        #[arg(long, default_value_t = 1)]
        parallelism: u32,
        #[arg(long)]
        job_plan: Option<std::path::PathBuf>,
    },
    TriggerCheckpoint {
        #[arg(long)]
        jm: String,
        #[arg(long)]
        job_id: String,
    },
    Status {
        #[arg(long)]
        jm: String,
        #[arg(long)]
        job_id: String,
    },
    ListTaskmanagers {
        #[arg(long)]
        jm: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Jobmanager {
            listen,
            heartbeat_interval_ms,
            heartbeat_timeout_ms,
        } => {
            let config = ClusterConfig {
                heartbeat: HeartbeatConfig {
                    interval: Duration::from_millis(heartbeat_interval_ms),
                    timeout: Duration::from_millis(heartbeat_timeout_ms),
                },
            };
            let jm = Arc::new(JobManager::new(config));
            println!("jobmanager listening on {}", listen);
            jm.serve(listen).await?;
        }
        Commands::Taskmanager {
            jm,
            listen,
            slots,
            tm_id,
            heartbeat_interval_ms,
            heartbeat_timeout_ms,
        } => {
            if listen.port() == 0 {
                anyhow::bail!(
                    "taskmanager --listen does not support port 0, please provide explicit port"
                );
            }
            let heartbeat = HeartbeatConfig {
                interval: Duration::from_millis(heartbeat_interval_ms),
                timeout: Duration::from_millis(heartbeat_timeout_ms),
            };
            let tm = Arc::new(TaskManager::new(
                tm_id,
                normalize_endpoint(&jm),
                listen.to_string(),
                slots,
                Resources::default(),
                heartbeat,
            ));
            println!("taskmanager listening on {}", listen);
            let serve_handle = tokio::spawn({
                let tm = Arc::clone(&tm);
                async move { tm.serve(listen).await }
            });

            if let Err(err) = tm.register_with_retry(10, Duration::from_millis(500)).await {
                serve_handle.abort();
                return Err(err);
            }
            let (shutdown, heartbeat_handle) = Arc::clone(&tm).spawn_heartbeat_loop();

            let serve_result = match serve_handle.await {
                Ok(v) => v,
                Err(join_err) => Err(join_err.into()),
            };
            let _ = shutdown.send(true);
            let _ = heartbeat_handle.await;
            serve_result?;
        }
        Commands::Submit {
            jm,
            parallelism,
            job_plan,
        } => {
            let mut client = JobManagerServiceClient::connect(normalize_endpoint(&jm)).await?;
            let job_plan = if let Some(path) = job_plan {
                std::fs::read(&path)?
            } else {
                vec![]
            };
            let response = client
                .submit_job(Request::new(rpc::SubmitJobRequest {
                    job_plan,
                    parallelism,
                }))
                .await?
                .into_inner();
            println!(
                "submit result: accepted={} job_id={} error={}",
                response.accepted, response.job_id, response.error_message
            );
        }
        Commands::TriggerCheckpoint { jm, job_id } => {
            let mut client = JobManagerServiceClient::connect(normalize_endpoint(&jm)).await?;
            let response = client
                .trigger_checkpoint(Request::new(rpc::TriggerCheckpointRequest { job_id }))
                .await?
                .into_inner();
            println!(
                "trigger checkpoint: accepted={} checkpoint_id={} message={}",
                response.accepted, response.checkpoint_id, response.message
            );
        }
        Commands::Status { jm, job_id } => {
            let mut client = JobManagerServiceClient::connect(normalize_endpoint(&jm)).await?;
            let response = client
                .get_job_status(Request::new(rpc::GetJobStatusRequest { job_id }))
                .await?
                .into_inner();
            println!(
                "job status: job_id={} status={} message={}",
                response.job_id, response.status, response.message
            );
        }
        Commands::ListTaskmanagers { jm } => {
            let mut client = JobManagerServiceClient::connect(normalize_endpoint(&jm)).await?;
            let response = client
                .list_task_managers(Request::new(rpc::ListTaskManagersRequest {}))
                .await?
                .into_inner();
            println!("taskmanagers={}", response.task_managers.len());
            for tm in response.task_managers {
                println!(
                    "tm_id={} addr={} slots={} last_hb_ms={}",
                    tm.tm_id, tm.address, tm.num_slots, tm.last_heartbeat_unix_ms
                );
            }
        }
    }
    Ok(())
}

fn normalize_endpoint(input: &str) -> String {
    if input.starts_with("http://") || input.starts_with("https://") {
        input.to_string()
    } else {
        format!("http://{}", input)
    }
}
