# quant-system

A Rust workspace for deterministic historical replay and real-time market-data infrastructure.

`quant-system` is intended for Rust developers and quantitative researchers who want to import historical market data, replay normalized trading actions against explicit instrument specifications, embed trading-domain and backtest libraries, or operate a local CTrader quote service. The workspace is preparing the synchronized `0.4.2` release. The published `0.4.1` packages do not include the subsequent instrument-admission and service cost-key corrections in current source; `0.4.2` publication has not been performed as part of this version update.

It is not a complete automated trading platform. It does not currently execute live broker orders, provide restart-safe live strategy orchestration, or implement general cryptocurrency economics.

## Choose a workflow

| Goal | Start here | Readiness |
|---|---|---|
| Run a deterministic signal backtest | [Five-minute quick start](docs/getting-started.md) | Available; a synthetic fixture is included |
| Import, resample, and manage historical data | [`qs-data-preprocess` guide](crates/data-preprocess/GUIDE.md) | Available for supported tick and bar exports; stored bars can be built from stored ticks |
| Embed the pure trade engine or strict raw-signal contracts | [`quant-system-core`](crates/core) | Library-only |
| Compile and evaluate reusable configured strategy behavior | [`qs-strategy`](crates/strategy) | Library-only; synchronous core |
| Prepare broker-neutral execution requests and project reports | [`qs-execution`](crates/execution) | Library-only; no broker adapter or live scheduler |
| Build an in-process historical strategy simulation | [`qs-backtest`](crates/backtest) | Library-only |
| Replay stored Parquet market data through the engine | [`qs-market-loader`](crates/market-loader) | Library-only |
| Search parameters or bounded typed structures and compare explicit split roles | [`qs-research` guide](docs/research.md) | Library or service; configured/direct factories, tick or stored-bar input |
| Run a configured strategy or a search through the running service | [Backtesting guide](docs/backtesting.md#configured-strategies-through-the-service) | Available through `strategy_backtest` and the typed client |
| Parse Telegram message exports | [Signal ingestion guide](docs/signal-ingestion.md) | Compatibility CLI and public adapter library |
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
instrument catalog or symbol compatibility snapshot ----+
                                                        |
external producer or qs-signal-parser -> RawSignal -----+
                                                        v
                                               Backtest Service
                                                        |
                                                        v
                                             deterministic replay
                                                        |
                                                        v
                               result with pinned instrument manifest

RawSignal + current application facts -> qs-execution -> broker-neutral requests/reports

CTrader FIX -> Market Data Service -> snapshots, subscriptions, and alerts
```

`RawSignal` remains the compatibility boundary accepted by current replay endpoints. `qs-instruments` provides source-neutral asset IDs, broker- or exchange-qualified instrument identities, exact decimal grids, effective-dated specifications, and immutable catalog snapshots. CTrader is modeled as a trading platform rather than an instrument listing venue. Source-neutral ingestion libraries, strict JSONL codecs, Telegram adapters, and an authenticated webhook provider edge are also available; see [Signal ingestion](docs/signal-ingestion.md) and [Architecture](docs/architecture.md).

## Current boundaries

- Historical replay and strategy research are available; live broker submission and restart-safe live orchestration are not included.
- Bar replay approximates intrabar order. Use ticks when stop, target, and management ordering matters.
- Ingestion admission is not committed normalization or trading activity; the committed-batch trading bridge is not implemented.
- BTCUSD/ETHUSD linear simulations use operator assumptions, not certified broker contracts or general cryptocurrency economics.
- Internal TCP services have no built-in authentication or TLS and default to loopback.

See [Current capabilities and boundaries](docs/current-boundaries.md) for detailed strategy, replay, cost, ingestion, market-data, and execution contracts.

## Documentation

- [Documentation index](docs/README.md)
- [Getting started](docs/getting-started.md)
- [Backtesting](docs/backtesting.md)
- [Signal ingestion](docs/signal-ingestion.md)
- [Market data](docs/market-data.md)
- [Architecture](docs/architecture.md)
- [Current capabilities and boundaries](docs/current-boundaries.md)
- [Roadmap](docs/roadmap.md)
- [RawSignal reference](docs/reference/raw-signal.md)

## Development

Start with the owning package's regression tests. For a backtest service change:

```bash
cargo test -p qs-backtest-server --all-targets --locked
cargo clippy -p qs-backtest-server --all-targets --locked -- -D warnings
```

Run broader workspace checks when the change crosses ownership boundaries or before release:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-features --all-targets
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

Long historical tick replays are dedicated workload acceptance, not part of these automated regression commands. Use bounded representative replays to validate adapter wiring during development, and reserve full-period runs for an explicitly scheduled acceptance with recorded source, data and resource budgets. A shorter window or bar replay must not be reported as the original full-period tick result.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
