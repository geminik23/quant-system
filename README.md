# quant-system

A Rust workspace for deterministic historical replay and real-time market-data infrastructure.

`quant-system` is intended for Rust developers and quantitative researchers who want to import historical market data, replay normalized trading actions, embed trading-domain and backtest libraries, or operate a local CTrader quote service. The workspace is under active `0.2.x` development.

It is not a complete automated trading platform. It does not currently execute live broker orders, provide restart-safe live strategy orchestration, or implement general cryptocurrency economics.

## Choose a workflow

| Goal | Start here | Readiness |
|---|---|---|
| Run a deterministic signal backtest | [Five-minute quick start](docs/getting-started.md) | Available; a synthetic fixture is included |
| Import and manage historical data | [`qs-data-preprocess` guide](crates/data-preprocess/GUIDE.md) | Available for supported tick and bar exports |
| Embed the pure trade engine | [`quant-system-core`](crates/core) | Library-only |
| Build an in-process strategy simulation | [`qs-backtest`](crates/backtest) | Library-only |
| Parse Telegram message exports | [Signal ingestion guide](docs/signal-ingestion.md) | Provider-specific adapter |
| Build source-neutral ingestion | [Signal ingestion guide](docs/signal-ingestion.md) | Source-event, normalization, and durable state library APIs |
| Operate a CTrader quote service | [Market-data guide](docs/market-data.md) | Requires CTrader FIX credentials |

## Five-minute backtest

### Prerequisites

- Rust 1.88 or newer;
- Linux shared memory (`/dev/shm`) for the provided `shm://` example;
- two terminals after the data import finishes.

Import the repository-owned EURUSD fixture:

```bash
cargo run -p qs-data-preprocess --bin data-preprocess -- \
  --data-dir target/quickstart/market_data \
  input tick \
  --exchange demo \
  --symbol EURUSD \
  --tz-offset +00:00 \
  examples/backtest-quickstart/EURUSD_ticks.csv
```

Start the backtest server:

```bash
cargo run -p qs-backtest-server --bin backtest_server -- \
  --config examples/backtest-quickstart/backtest-server.toml
```

In another terminal, submit the matching signal stream:

```bash
cargo run -p qs-backtest-server --bin tg_backtest -- \
  --input examples/backtest-quickstart/signals.jsonl \
  --endpoint shm://backtest-quickstart \
  --all-symbols \
  --exchange demo \
  --data-type tick \
  --balance 10000 \
  --account-currency USD \
  --base-lot 0.02 \
  --output target/quickstart/result.json
```

The fixture opens a EURUSD long position and closes it one minute later. See the [getting-started guide](docs/getting-started.md) for expected results, endpoint alternatives, and troubleshooting.

## Architecture at a glance

```text
historical tick/bar export -> qs-data-preprocess -> partitioned Parquet
                                                        |
external producer or qs-signal-parser -> RawSignal -----+
                                                        v
                                               Backtest Service
                                                        |
                                                        v
                                             deterministic replay
                                                        |
                                                        v
                                            result or artifact

CTrader FIX -> Market Data Service -> snapshots, subscriptions, and alerts
```

`RawSignal` is the compatibility boundary between signal producers and replay. See [Architecture](docs/architecture.md) for crate ownership and service boundaries.

## Current boundaries

- Bars are replayed as close-only, zero-spread quotes, so exact intrabar execution is not simulated.
- Source-neutral event, stateless normalization, and durable source-application contracts are available as library APIs, including restart-safe idempotency, committed lifecycle state, checkpoints, causal replay inputs, and transactional publication outbox state. Generic hosted runners, source adapters, external sink workers, and the committed-batch trading bridge are not implemented.
- Live order execution, restart-safe strategy state, and broker order adapters are not included.
- Registered cryptocurrency symbols are metadata-only for replay; spot, derivative, fee, funding, margin, and liquidation models are not implemented.
- Internal service TCP endpoints have no built-in authentication or TLS and are restricted to loopback by default.
- Historical import accepts the documented MetaTrader-style tab-delimited tick and bar formats, not arbitrary CSV layouts.

## Documentation

- [Documentation index](docs/README.md)
- [Getting started](docs/getting-started.md)
- [Backtesting](docs/backtesting.md)
- [Signal ingestion](docs/signal-ingestion.md)
- [Market data](docs/market-data.md)
- [Architecture](docs/architecture.md)
- [Roadmap](docs/roadmap.md)
- [RawSignal reference](docs/reference/raw-signal.md)

## Development

```bash
cargo fmt --all -- --check
cargo test --workspace --all-features --all-targets
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
