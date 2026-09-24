//! Submit a configured strategy run, a portfolio run, or a parameter search described by a TOML run file to the backtest service, wait for it, and write its complete output as JSON.
//!
//! Run it with `strategy_backtest --run run.toml --out result.json`. A search also writes its comparison table next to the output with a `.csv` extension.

use std::path::PathBuf;
use std::time::Duration;

use backtest_server::strategy_client::{StrategyClientRequest, load_run_file};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use clap::Parser;
use qs_backtest_api::provider::xrpc::BacktestXrpcClient;
use qs_backtest_api::{
    BacktestClient, BacktestStrategyClient, GetResultArtifactChunkRequest, ResultArtifactRefMsg,
    SearchResultMsg,
};
use qs_service::ServiceEndpoint;
use qs_service_xrpc::XrpcTransportConfig;

#[derive(Parser, Debug)]
#[command(
    name = "strategy_backtest",
    about = "Run a configured strategy, a portfolio, or a parameter search through the backtest service"
)]
struct Args {
    /// TOML run file naming the strategy document, the `[[instances]]` of a portfolio, or the template and space documents, and the data scope.
    #[arg(long)]
    run: PathBuf,

    /// Shared memory base name (must match server config).
    #[arg(long, default_value = "backtest")]
    shm_name: String,

    /// Transport endpoint. When omitted, `--shm-name` is interpreted as `shm://NAME`.
    #[arg(long)]
    endpoint: Option<ServiceEndpoint>,

    /// File receiving the complete JSON output; a summary is printed either way.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Status polling interval in milliseconds.
    #[arg(long, default_value_t = 500)]
    poll_millis: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let request = load_run_file(&args.run)?;
    let endpoint = match args.endpoint {
        Some(endpoint) => endpoint,
        None => format!("shm://{}", args.shm_name).parse()?,
    };
    let client = BacktestXrpcClient::connect(
        &endpoint,
        "strategy-backtest",
        &XrpcTransportConfig::default(),
    )
    .await?;

    let search = matches!(request, StrategyClientRequest::Search(_));
    let submitted = match request {
        StrategyClientRequest::Run(request) => client.submit_configured_strategy(request).await?,
        StrategyClientRequest::Portfolio(request) => client.submit_portfolio(request).await?,
        StrategyClientRequest::Search(request) => client.submit_search(request).await?,
    };
    let job_id = submitted.job_id.ok_or_else(|| {
        submitted
            .error
            .unwrap_or_else(|| "submission was rejected".into())
    })?;
    eprintln!("submitted job {job_id}");

    loop {
        let status = client.status(&job_id).await?;
        match status.status.as_str() {
            "Completed" => break,
            "Failed" | "Cancelled" | "NotFound" => {
                return Err(format!(
                    "job {job_id} ended as {}: {}",
                    status.status,
                    status.error.unwrap_or_default()
                )
                .into());
            }
            _ => {
                eprintln!(
                    "  {} {}/{}",
                    status.progress.stage,
                    status.progress.processed_events,
                    status.progress.total_events
                );
                tokio::time::sleep(Duration::from_millis(args.poll_millis)).await;
            }
        }
    }

    if search {
        let response = client.search_result(&job_id).await?;
        let reference = response.artifact.ok_or_else(|| {
            response
                .error
                .unwrap_or_else(|| "search has no output".into())
        })?;
        let bytes = download(&client, &reference).await?;
        let output: SearchResultMsg = serde_json::from_slice(&bytes)?;
        let summary = &output.summary;
        println!(
            "searched {} points over {} data: {} rows, {} completed, {} failed",
            summary.points_total,
            summary.data_mode,
            summary.rows,
            summary.completed_rows,
            summary.failed_rows
        );
        if let Some(out) = &args.out {
            std::fs::write(out, &bytes)?;
            std::fs::write(out.with_extension("csv"), &output.table_csv)?;
            println!(
                "wrote {} and {}",
                out.display(),
                out.with_extension("csv").display()
            );
        }
    } else {
        let response = client.result(&job_id).await?;
        let bytes = match (
            &response.artifact,
            response.inline_complete,
            &response.result,
        ) {
            (Some(reference), _, _) => download(&client, reference).await?,
            (None, true, Some(result)) => serde_json::to_vec_pretty(result)?,
            _ => {
                return Err(response
                    .error
                    .unwrap_or_else(|| "job has no complete result".into())
                    .into());
            }
        };
        let result: serde_json::Value = serde_json::from_slice(&bytes)?;
        println!(
            "total pnl {} over {} trades",
            result["total_pnl"], result["total_trades"]
        );
        if let Some(portfolio) = result.get("portfolio") {
            let instances = portfolio["instances"].as_array().map_or(0, Vec::len);
            let rejected = portfolio["supervisor"]["events"]
                .as_array()
                .map_or(0, |events| {
                    events
                        .iter()
                        .filter(|event| event["verdict"]["verdict"] == "reject")
                        .count()
                });
            println!("{instances} instances, {rejected} entries rejected by the supervisor");
        }
        if let Some(out) = &args.out {
            std::fs::write(out, &bytes)?;
            println!("wrote {}", out.display());
        }
    }
    Ok(())
}

async fn download(
    client: &BacktestXrpcClient,
    reference: &ResultArtifactRefMsg,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut bytes = Vec::with_capacity(reference.byte_len as usize);
    loop {
        let chunk = client
            .get_result_artifact_chunk(GetResultArtifactChunkRequest {
                artifact_id: reference.artifact_id.clone(),
                offset: bytes.len() as u64,
            })
            .await?;
        if !chunk.success {
            return Err(chunk
                .error
                .unwrap_or_else(|| "artifact download failed".into())
                .into());
        }
        bytes.extend(BASE64_STANDARD.decode(chunk.data_base64)?);
        if chunk.eof {
            return Ok(bytes);
        }
    }
}
