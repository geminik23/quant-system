//! CLI binary: reads raw Telegram JSONL, parses via configured channel parsers,
//! and outputs parsed signal JSONL to stdout or a file.

use std::io::{self, BufRead, Write};

use clap::Parser;

use signal_parser::{RawTgMessage, SignalParserError, load_parsers, parse_messages};

#[derive(Parser)]
#[command(
    name = "parse_signals",
    about = "Parse raw Telegram JSONL into trade signals"
)]
struct Cli {
    /// Path to the raw messages JSONL file.
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

    // Read raw messages from JSONL input.
    let file = std::fs::File::open(&cli.input)?;
    let reader = io::BufReader::new(file);
    let mut messages = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg: RawTgMessage = serde_json::from_str(trimmed)
            .map_err(|e| SignalParserError::Config(format!("line {}: {e}", i + 1)))?;
        messages.push(msg);
    }

    tracing::info!(count = messages.len(), "loaded raw messages");

    // Parse all messages through the registry pipeline.
    let entries = parse_messages(&registry, &messages)?;

    tracing::info!(count = entries.len(), "parsed signal entries");

    // Write output as JSONL.
    let mut writer: Box<dyn Write> = match &cli.output {
        Some(path) => Box::new(io::BufWriter::new(std::fs::File::create(path)?)),
        None => Box::new(io::BufWriter::new(io::stdout().lock())),
    };

    for entry in &entries {
        serde_json::to_writer(&mut writer, entry)?;
        writeln!(writer)?;
    }

    writer.flush()?;
    Ok(())
}
