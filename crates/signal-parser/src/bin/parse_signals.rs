//! CLI binary: reads raw Telegram JSONL, parses via configured channel parsers,
//! and outputs parsed signal JSONL to stdout or a file.
//!
//! This is a thin wrapper around `OfflineRunner` for TOML-configured parsers.

use clap::Parser;

use signal_parser::{OfflineArgs, OfflineRunner, SignalParserError, load_parsers};

#[derive(Parser)]
#[command(
    name = "parse_signals",
    about = "Parse raw Telegram JSONL into trade signals"
)]
struct Cli {
    /// Path to the raw messages JSONL file (or "-" for stdin).
    #[arg(short, long)]
    input: String,

    /// Path to the parsers TOML config file.
    #[arg(short, long)]
    parsers_config: String,

    /// Output file path (default: stdout).
    #[arg(short, long)]
    output: Option<String>,
}

fn main() -> Result<(), SignalParserError> {
    tracing_subscriber::fmt().init();

    let cli = Cli::parse();

    // Load parser registry from TOML config.
    let registry = load_parsers(&cli.parsers_config)?;

    // Delegate to OfflineRunner with pre-built args.
    OfflineRunner::new(registry).run_with_args(OfflineArgs {
        input: cli.input,
        output: cli.output,
    })
}
