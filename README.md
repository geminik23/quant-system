# quant-system

A modular Rust workspace for historical market data, deterministic backtesting, structured trading signals, and real-time market-data distribution.

The current server execution path consumes normalized `RawSignalMsg` actions and historical tick or bar data through strict shared-memory RPC. Real-time market data is available, but generic strategy orchestration, live order execution, source-neutral ingestion, and complete cryptocurrency economics are not current production features.

## Capability status

| Capability | Status | Current boundary |
|---|---|---|
| Historical tick and OHLCV storage | Implemented | Parquet/Polars by default; DuckDB is optional |
| Deterministic signal backtesting | Implemented | Strict `FutureQuoteV1` RPC over stored tick or close-only bar data |
| Strategy simulation library | Implemented | Generic in-memory library API; not exposed as the production RPC path |
| Structured signal execution | Implemented | `RawSignalMsg` Entry and management actions |
| Telegram parsing | Implemented | Telegram-specific message, channel, history, and reply semantics |
| Real-time market data | Implemented | CTrader FIX bid/ask distribution over shared-memory IPC |
| Generic ingestion and strategy runtime | Not implemented | The current parser and server contracts do not provide a source-neutral live strategy runtime |
| Live order execution | Not implemented | Venue-neutral order gateways and trading-platform adapters are not included |
| General cryptocurrency trading | Not implemented | Instrument economics, accounting, and venue behavior are not yet generalized for crypto trading |

## Architecture

```text
historical CSV
     |
     v
qs-data-preprocess -----> partitioned Parquet tick/bar data
                                      |
external parser                       v
or qs-signal-parser ---> RawSignalMsg ---> qs-backtest-server (strict RPC)
                                                  |
                                                  v
                                      deterministic FutureQuote replay
                                                  |
                                                  v
                                      inline or artifact result

CTrader FIX ---> qs-market-data ---> real-time bid/ask SHM consumers
                                      (market data only; no live orders)
```

`RawSignalMsg` is the compatibility boundary between signal producers and the backtest service. Signal parsing is not required when actions are produced by a manual tool, another service, or an external parser.

## Workspace crates

| Crate | Responsibility |
|---|---|
| [`qs-core`](crates/core/) | Synchronous trade engine and composable stoploss, trailing-stop, take-profit, breakeven, and time-exit rules |
| [`qs-backtest`](crates/backtest/) | Strategy and raw-signal simulation, FutureQuote execution, sizing, accounting, metrics, and reporting |
| [`qs-data-preprocess`](crates/data-preprocess/) | Historical tick/OHLCV import, partitioned Parquet storage, bounded queries, and optional DuckDB storage |
| [`qs-symbols`](crates/symbols/) | TOML symbol registry, aliases, price/lot metadata, and explicit P&L/base/quote currencies |
| [`qs-backtest-server`](crates/backtest-server/) | Multi-client shared-memory RPC, sync/async/multi-profile replay, jobs, cancellation, and result artifacts |
| [`qs-signal-parser`](crates/signal-parser/) | Telegram-focused offline/online parsing that emits generic raw-signal actions |
| [`qs-market-data`](crates/market-data/) | Real-time CTrader FIX bid/ask distribution, subscriptions, alerts, and reconnection |

## Quick start

### Prerequisites

- a Rust toolchain compatible with the workspace;
- Linux shared memory (`/dev/shm`) for the RPC services;
- tick or bar CSV data supported by `qs-data-preprocess`;
- timestamps and symbols in the signal file that match the imported data.

### 1. Import historical data

The default backend writes partitioned Parquet data under `market_data/`.

```bash
cargo run -p qs-data-preprocess --bin data-preprocess -- \
  --data-dir market_data \
  input tick \
  --exchange icmarkets \
  --symbol XAUUSD \
  /path/to/ticks.csv
```

For bar input, use `input bar --timeframe 1h`. See the [`qs-data-preprocess` guide](crates/data-preprocess/README.md) for supported CSV formats, time-zone handling, queries, and removal commands.

### 2. Prepare a raw-signal JSONL file

Each line is one tagged `RawSignalMsg`. An Entry requires a finite positive `risk`; `size` is not an Entry field.

```json
{"action":"Entry","ts":"2026-03-10T10:00:00","symbol":"XAUUSD","side":"Buy","order_type":"Market","price":null,"risk":1.0,"stoploss":2010.0,"targets":[2040.0,2060.0],"group":"example","trade_id":"example-1"}
```

Entries and later management actions can be mixed in the same JSONL stream.

### 3. Start the backtest server

Copy the example configuration and adjust the data, symbol-registry, profile, and artifact paths if needed.

```bash
cp crates/backtest-server/config.example.toml backtest-server.toml

cargo run -p qs-backtest-server --bin backtest_server -- \
  --config backtest-server.toml
```

### 4. Run a backtest

In another terminal:

```bash
cargo run -p qs-backtest-server --bin tg_backtest -- \
  --input signals.jsonl \
  --shm-name backtest \
  --all-symbols \
  --exchange icmarkets \
  --data-type tick \
  --balance 10000 \
  --account-currency USD \
  --base-lot 0.02 \
  --output result.json
```

When at least one Entry is present, select exactly one account sizing basis: `--base-lot`, `--risk-per-trade`, or `--risk-percent`. `--account-currency` is also required. Run either binary with `--help` for the complete option list.

## Current server contract

Production backtest execution registers only:

- `run_backtest` for synchronous single-profile execution;
- `run_backtest_multi` for multi-profile comparison;
- `submit_backtest` for asynchronous jobs.

Requests do not carry an API schema magic number or use version-suffixed method names. Recursive strict decoding rejects unknown fields throughout nested configuration, profile, and signal objects. The server and `tg_backtest` use streaming `FutureQuoteV1` exclusively and accept only the strict raw-signal path.

`BacktestRunner::new` and materialized `DataFeed` APIs remain available for direct library simulations. They are compatibility surfaces and are not reachable through the production backtest RPC or `tg_backtest`.

Persisted FutureQuote artifacts and downloadable result JSON references carry an independent `format_version` starting at `1`. This format marker is not an RPC API version.

## Production backtesting guarantees

- Replay planning filters signals before loading data, derives active symbols from retained Entries, and reports requested but inactive symbols as idle.
- Active primary and required conversion series are read through bounded ascending Parquet streams. Timestamp batches are deterministic, and FX is processed before primary events at a shared timestamp.
- All monetary accounting is performed in the requested account currency using causal same-exchange identity, direct, inverse, or deterministic two-leg FX routes.
- Exact online equity, drawdown, exposure, and campaign MAE/MFE are independent of the returned MTM curve size.
- MTM output supports `none`, `bounded`, and `full`. New clients default to a deterministic bound of 4096 points; valid bounded sizes are 8 through 16384.
- Results support `auto`, `inline`, and `artifact` delivery. `auto` returns results inline up to the configured 12 MiB default and otherwise uses a length- and SHA-256-verified artifact.
- Async jobs provide progress, cooperative cancellation, bounded retention, and scheduled cleanup. Results include the effective data scope, currency plan, profile, sizing identity, and other reproducibility metadata.

## Entry risk, sizing, and management

Each wire Entry carries `risk`, which remains a multiplier until exactly one sizing policy resolves it to a final lot quantity:

- `FixedLot { lots }` scales configured lots and may open without a stop;
- `FixedRiskAmount { amount }` risks an account-currency amount and requires a protective stop;
- `BalanceRiskPercent { percent }` risks a percentage of realized balance and requires a protective stop.

The risk multiplier is applied once before integer lot-step flooring and minimum/maximum validation. Target weights are allocated after the final lot steps are known. Pending Entries are profiled and sized when placed, and their quantity remains frozen until fill or cancellation.

Raw signals can also close or partially close positions, modify stops and targets, add or remove rules, scale in, cancel pending orders, and operate on symbol or group scopes. TOML management profiles provide reusable Entry management policy without changing the source signal.

## Signal parsing and evaluation

The current `qs-signal-parser` implementation is Telegram-specific. It preserves channel/message identity, source ordering, history, and reply-root resolution, then emits generic `RawSignal` actions. General source events, decoder/parser/normalizer separation, idempotency, and non-Telegram adapters are not yet implemented.

Backtest evaluation can include typed source-coverage counts, deterministic position samples, and supported symbol/side/group/close-reason breakdowns. Unsupported tag-based selection is rejected explicitly because completed positions do not yet retain tags.

## Known boundaries of the current backtest path

1. Several complex RPC artifacts still cross the wire as JSON `Value` objects rather than fully typed wire models.
2. Bars are replayed as close-only, zero-spread quotes; exact intrabar execution is not simulated.
3. `ScaleIn` carries an explicit final size. Policy-sized scale-in is not implemented, and FutureQuote rejects scale-in after a campaign begins closing.
4. During cancellation, one bounded low-level Polars Parquet read may complete atomically before cancellation is observed.
5. Explicit `full` MTM retains the complete curve in memory before artifact serialization. Bounded output is the recommended default.
6. Live order execution, restart-safe live strategy state, general crypto accounting, and source-neutral ingestion are not current runtime features.

## Development and validation

Use the full workspace checks before release:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-features --all-targets
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
