# quant-system

A Rust workspace for deterministic historical replay and real-time market-data infrastructure.

`quant-system` is intended for Rust developers and quantitative researchers who want to import historical market data, replay normalized trading actions against explicit instrument specifications, embed trading-domain and backtest libraries, or operate a local CTrader quote service. The workspace is under active `0.3.x` development.

It is not a complete automated trading platform. It does not currently execute live broker orders, provide restart-safe live strategy orchestration, or implement general cryptocurrency economics.

## Choose a workflow

| Goal | Start here | Readiness |
|---|---|---|
| Run a deterministic signal backtest | [Five-minute quick start](docs/getting-started.md) | Available; a synthetic fixture is included |
| Import and manage historical data | [`qs-data-preprocess` guide](crates/data-preprocess/GUIDE.md) | Available for supported tick and bar exports |
| Embed the pure trade engine or strict raw-signal contracts | [`quant-system-core`](crates/core) | Library-only |
| Compile and evaluate reusable configured strategy behavior | [`qs-strategy`](crates/strategy) | Library-only; synchronous core |
| Build an in-process historical strategy simulation | [`qs-backtest`](crates/backtest) | Library-only |
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

CTrader FIX -> Market Data Service -> snapshots, subscriptions, and alerts
```

`RawSignal` remains the compatibility boundary accepted by current replay endpoints. `qs-instruments` provides source-neutral asset IDs, broker- or exchange-qualified instrument identities, exact decimal grids, effective-dated specifications, and immutable catalog snapshots. CTrader is modeled as a trading platform rather than an instrument listing venue. Source-neutral ingestion libraries, strict JSONL codecs, Telegram adapters, and an authenticated webhook provider edge are also available; see [Signal ingestion](docs/signal-ingestion.md) and [Architecture](docs/architecture.md).

## Current boundaries

- Bars are replayed as close-only, zero-spread quotes, so exact intrabar execution is not simulated.
- Source-neutral ingestion is available as embeddable library APIs for JSONL, Telegram, and authenticated webhook sources. A webhook `202 Accepted` response confirms admission only; it does not confirm normalization, committed-batch publication, or trading activity. Hosted application processing is not restart-safe, and the committed-batch trading bridge is not implemented.
- `qs-strategy` provides a reusable synchronous configured strategy core with recursively strict unversioned configuration, bounded logical bar sources, source-specific input requirements, an explicit immutable material library, typed bounded expressions, deterministic material and finite-state evaluation, total vacant/pending/open trade-slot facts, generic decisions and notes, and validated command-correlated strict `RawSignal` values. It remains library-only and owns no historical feeds, services, live runtime, persistence, or management-profile composition.
- `qs-backtest` provides validated historical strategy contracts and a configured-strategy adapter over the existing FutureQuote scheduler. The adapter performs complete logical-source binding, exact tick-count volume projection, named-input projection, total trade-slot projection, ordered command provenance and committed feedback, final feedback processing, decision and note mapping, and unprofiled Entry reuse while rejecting supplied management profiles before feed consumption.
- Configured historical execution is available through materialized and streaming in-process library APIs. Current conformance verifies neutral no-op, EMA crossover, EMA/ATR lifecycle, pending cancellation, custom material reuse, direct-signal economic parity, aligned-EOD materialized/streaming parity, final feedback handling, and strict research-output deserialization.
- Current backtest service endpoints accept strict `RawSignal`; configured-strategy server or RPC execution is not included. No portfolio supervisor, execution gateway, live venue implementation, or automatic committed-batch trading bridge is included.
- Live order execution, restart-safe strategy state, and broker order adapters are not included.
- The instrument catalog can describe cryptocurrency assets and model identifiers, but replay does not implement cryptocurrency spot, derivative, fee, funding, margin, or liquidation economics. Registry-backed cryptocurrency rows remain rejected before data access.
- Shipped backtest clients use provider-neutral retained-job, artifact, synchronous-execution, and discovery capabilities through the typed xrpc facade; RPC method names and provider error mapping remain inside the API provider module.
- Market-data snapshots and streams use service quote-observation timestamps rather than unavailable CTrader source timestamps. Reconnect invalidates prior-session quote cache entries, source-state events carry transition timestamps, and the combined event stream exposes detected receiver lag or subscription rejection without claiming replay or exactly-once delivery.
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
