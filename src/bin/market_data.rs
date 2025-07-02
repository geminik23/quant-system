use clap::Parser;
use quant::{
    Result,
    core::CommandRunner,
    rpc::{RpcService, run_server_forever},
    utils::{channel_handler, load_config},
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::sync::mpsc;

#[derive(Debug, Deserialize)]
struct Config {}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    // /// Path to the config file (JSON)
    // #[arg(short, long, value_name = "FILE PATH")]
    // config: PathBuf,

    /// Server host
    #[arg(short, long, default_value = "localhost", value_name = "HOST")]
    host: String,

    /// Server port
    #[arg(short, long, default_value = "8980", value_name = "PORT")]
    port: u16,

    /// Enable debug mode
    #[arg(short, long, action = clap::ArgAction::SetTrue, value_name = "DEBUG")]
    debug: bool,
}

#[derive(Debug, Serialize, Deserialize)]
enum LocalCommand {
    Ping(String),
}

#[derive(Clone)]
pub struct Handler;

#[async_trait::async_trait]
impl quant::core::CommandHandler<LocalCommand> for Handler {
    async fn handle(&mut self, cmd: LocalCommand) {
        match cmd {
            LocalCommand::Ping(uid) => println!("Handle Ping {uid}"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    log::info!("Starting service...");
    quant::utils::setup();


    let cli = Cli::parse();

    // let config = load_config::<Config>(&cli.config)?;

    // TODO: do something with the config

    let (cmd_tx, cmd_rx) = mpsc::channel::<LocalCommand>(100);

    let service = RpcService::new();
    service.register_handler("command", channel_handler::<LocalCommand>(cmd_tx));

    tokio::spawn(async move {
        let addr = format!("{}:{}", cli.host, cli.port);
        let _ = run_server_forever(&addr, service).await;
    });

    let app = CommandRunner::new(cmd_rx, Handler);
    app.run().await;

    Ok(())
}
