# quant-system

A modular Rust workspace for historical market data, deterministic backtesting, structured trading signals, and real-time market-data distribution.

The framework currently focuses on normalized signal replay over stored tick or bar data. It also provides real-time CTrader market-data distribution and reusable trading-domain libraries, while strategy orchestration and live order execution remain future work.

## Capability status

| Capability | Status | Current boundary |
|---|---|---|
| Historical tick and OHLCV storage | Implemented | Partitioned Parquet/Polars by default; DuckDB is optional |
| Deterministic signal backtesting | Implemented | Stored tick or close-only bar data through streaming FutureQuote replay |
| Structured signal execution | Implemented | Entry and position-management actions through `RawSignalMsg` |
| Strategy simulation library | Implemented | In-memory library API; not exposed as the production service path |
| Telegram parsing | Implemented | Optional Telegram-specific adapter that emits generic raw-signal actions |
| Real-time market data | Implemented | CTrader FIX bid/ask distribution through the market-data service |
| Source-neutral strategy runtime | Not implemented | Generic ingestion and live strategy orchestration are planned separately |
| Live order execution and general crypto economics | Not implemented | Venue adapters and non-FX economic models are not production features yet |

## Quick start

### Prerequisites

- Rust 1.88 or newer;
- Linux shared memory (`/dev/shm`) when using the default `shm://` endpoints;
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

For bar input, use `input bar --timeframe 1h`. See the [`qs-data-preprocess` guide](https://github.com/geminik23/quant-system/blob/main/crates/data-preprocess/GUIDE.md) for supported CSV formats, time-zone handling, queries, and removal commands.

### 2. Prepare a raw-signal JSONL file

Each line is one tagged `RawSignalMsg`. An Entry requires a finite positive `risk`; `size` is not an Entry field.

```json
{"action":"Entry","ts":"2026-03-10T10:00:00","symbol":"XAUUSD","side":"Buy","order_type":"Market","price":null,"risk":1.0,"stoploss":2010.0,"targets":[2040.0,2060.0],"group":"example","trade_id":"example-1"}
```

Entries and later management actions can be mixed in the same JSONL stream. Signal parsing is optional when a manual tool, another service, or an external parser already produces normalized actions.

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
  --endpoint shm://backtest \
  --all-symbols \
  --exchange icmarkets \
  --data-type tick \
  --balance 10000 \
  --account-currency USD \
  --base-lot 0.02 \
  --output result.json
```

When at least one Entry is present, select exactly one sizing basis: `--base-lot`, `--risk-per-trade`, or `--risk-percent`. `--account-currency` is also required. Run either binary with `--help` for the complete option list.

## Architecture

```text
historical CSV
     |
     v
qs-data-preprocess -----> partitioned Parquet tick/bar data
                                      |
external producer                     v
or qs-signal-parser ---> RawSignalMsg ---> Backtest Service
                                                  |
                                                  v
                                      deterministic replay
                                                  |
                                                  v
                                      inline or artifact result

CTrader FIX ---> Market Data Service ---> real-time bid/ask consumers
                    |
                    v
          shm://, unix://, or tcp://127.0.0.1
```

`RawSignalMsg` is the compatibility boundary between signal producers and the current backtest service. The service APIs are transport-neutral; shared memory is the default local endpoint, while Unix sockets and loopback TCP are available when deployment requirements differ.

## Workspace components

| Area | Crates | Responsibility |
|---|---|---|
| Trading domain | [`quant-system-core`](https://github.com/geminik23/quant-system/tree/main/crates/core) (`qs_core` library), [`qs-symbols`](https://github.com/geminik23/quant-system/tree/main/crates/symbols) | Trade engine, normalized actions, management policies, sizing, currency conversion, and symbol metadata |
| Historical replay | [`qs-backtest`](https://github.com/geminik23/quant-system/tree/main/crates/backtest), [`qs-backtest-server`](https://github.com/geminik23/quant-system/tree/main/crates/backtest-server) | Deterministic replay, accounting, metrics, profiles, retained jobs, and result delivery |
| Historical data | [`qs-data-preprocess`](https://github.com/geminik23/quant-system/tree/main/crates/data-preprocess) | CSV import, partitioned storage, bounded queries, and data management |
| Service contracts | [`qs-service`](https://github.com/geminik23/quant-system/tree/main/crates/service), [`qs-backtest-api`](https://github.com/geminik23/quant-system/tree/main/crates/backtest-api), [`qs-market-data-api`](https://github.com/geminik23/quant-system/tree/main/crates/market-data-api) | Provider-neutral endpoints, failures, DTOs, events, and typed client ports |
| Internal transport provider | [`qs-service-xrpc`](https://github.com/geminik23/quant-system/tree/main/crates/service-xrpc) | Channel/SHM/Unix/TCP runtime behind the logical service APIs |
| Signal ingestion | [`qs-signal-parser`](https://github.com/geminik23/quant-system/tree/main/crates/signal-parser) | Telegram-focused offline and online parsing into generic raw-signal actions |
| Real-time market data | [`qs-market-data`](https://github.com/geminik23/quant-system/tree/main/crates/market-data) | CTrader FIX quotes, subscriptions, alerts, and reconnection |

## Operational behavior

The backtest CLI uses retained-job streaming by default. Progress and heartbeat events keep long-running work observable without imposing a total job deadline, and reconnecting clients resume the same job instead of submitting it again. Polling and finite synchronous execution remain available as fallbacks.

Results can be returned inline or as verified artifacts. Market-to-market output is bounded by default so large replays do not require returning an unbounded curve.

TCP endpoints are unauthenticated and restricted to loopback by default. Use SHM or Unix sockets for trusted local deployments unless a private-network TCP deployment is explicitly configured.

## Current limitations

- Bars are replayed as close-only, zero-spread quotes, so exact intrabar execution is not simulated.
- The current parser is Telegram-specific; generic source events, durable source-neutral ingestion, and non-Telegram adapters are not implemented yet.
- Live order execution, restart-safe live strategy state, and trading-platform order adapters are not included.
- Cryptocurrency instruments do not yet have general spot, derivative, fee, funding, margin, or liquidation economics.

## Development

Run the full workspace checks before release:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-features --all-targets
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](https://github.com/geminik23/quant-system/blob/main/LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](https://github.com/geminik23/quant-system/blob/main/LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
