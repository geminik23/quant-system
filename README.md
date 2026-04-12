# quant-system

A modular workspace for algorithmic trading — real-time market data, strategy execution, and analysis.

## Crates

| Crate | Description |
|-------|-------------|
| [`qs-core`](crates/core/) | Core trade engine — position management with composable rules (stoploss, trailing stop, take-profit, breakeven, time exit). Synchronous, side-effect-free; shared foundation for live trading and backtesting. |
| [`qs-backtest`](crates/backtest/) | Backtesting engine — strategy-driven and signal-replay modes over historical market data. Simulates fills, tracks P&L, and produces aggregate statistics (win rate, drawdown, profit factor). |
| [`qs-market-data`](crates/market-data/) | Real-time price streaming from CTrader FIX API, exposed to local clients via shared memory IPC (`xrpc-rs`). Supports per-client subscriptions, price alerts, and automatic reconnection. |
| [`qs-data-preprocess`](crates/data-preprocess/) | Historical market data storage and preprocessing CLI. Imports tick and OHLCV bar CSVs into a local DuckDB database with exchange partitioning, deduplication, and query/management commands. |
| [`qs-symbols`](crates/symbols/) | Symbol registry — TOML-driven canonical name normalization, price precision metadata (pip/digit), and lot specification. Shared across all crates to replace hardcoded symbol mappings. |
| [`qs-backtest-server`](crates/backtest-server/) | Backtest RPC server over shared memory (xrpc-rs). Accepts signals + profile + date range, loads Parquet data, runs backtests, and returns serialized results. Multi-client via acceptor pattern. |

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
