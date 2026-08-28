# Documentation

Use this index to enter the workspace by goal rather than by crate name.

## Start here

| Goal | Guide |
|---|---|
| Complete a repository-owned backtest | [Getting started](getting-started.md) |
| Import data and understand replay behavior | [Backtesting](backtesting.md) |
| Normalize and durably apply source events | [Signal ingestion](signal-ingestion.md) |
| Run the CTrader quote service | [Market data](market-data.md) |
| Understand crate and service ownership | [Architecture](architecture.md) |
| Review intended project direction | [Roadmap](roadmap.md) |
| Construct strict signal JSONL | [RawSignal reference](reference/raw-signal.md) |

## Component references

- [`qs-data-preprocess` CLI and storage guide](../crates/data-preprocess/GUIDE.md)
- [`qs-market-data` operations and client contract](../crates/market-data/GUIDE.md)
- [`qs-backtest-server` example configuration](../crates/backtest-server/config.example.toml)
- [`qs-signal-parser` Telegram parser configuration](../crates/signal-parser/parsers.example.toml)

Rust API details remain in each crate's Rustdoc.
