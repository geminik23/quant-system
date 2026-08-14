# Getting started

This guide runs one deterministic EURUSD backtest using only repository-owned inputs. It imports four synthetic ticks, starts the local backtest service, opens a long position, closes it one minute later, and writes the complete result to `target/quickstart/result.json`.

## Prerequisites

- Rust 1.88 or newer;
- Linux shared memory at `/dev/shm` for the provided endpoint;
- two terminals after import.

All commands assume the repository root as the working directory.

## 1. Import the synthetic ticks

```bash
cargo run -p qs-data-preprocess --bin data-preprocess -- \
  --data-dir target/quickstart/market_data \
  input tick \
  --exchange demo \
  --symbol EURUSD \
  --tz-offset +00:00 \
  examples/backtest-quickstart/EURUSD_ticks.csv
```

The explicit UTC offset keeps the imported timestamps aligned with `signals.jsonl`. The import creates the required Parquet partitions under `target/quickstart/market_data`.

Inspect the imported rows if needed:

```bash
cargo run -p qs-data-preprocess --bin data-preprocess -- \
  --data-dir target/quickstart/market_data \
  view tick \
  --exchange demo \
  --symbol EURUSD
```

## 2. Start the service

```bash
cargo run -p qs-backtest-server --bin backtest_server -- \
  --config examples/backtest-quickstart/backtest-server.toml
```

Leave the server running. The example uses `shm://backtest-quickstart` and writes retained artifacts under `target/quickstart/artifacts`. Its instrument configuration compiles the existing symbol registry into a guarded compatibility catalog under the repository-owned `repository-default` listing namespace and records `quickstart-parquet` as the historical data source. The imported `demo` exchange remains a physical Parquet partition coordinate rather than a broker, listing venue, or trading platform.

## 3. Submit the signals

In another terminal:

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

The signal stream contains an Entry at `10:00:00` and a Close at `10:01:00`. Under FutureQuote replay, the buy fills at the ask and the close fills at the bid. With the included quotes and a `0.02` lot size, the expected realized P&L is approximately `1.60 USD` before any user-supplied costs.

Inspect the full JSON result at `target/quickstart/result.json`. Execution metadata includes a typed instrument manifest with the compatibility catalog version, the resolved `repository-default/fx_cfd/EURUSD` identity and specification revision, the effective specification, and the `demo`/`eurusd` stored-series coordinates. Stop the server with Ctrl-C when finished.

## Troubleshooting

### Endpoint cannot be created

The provided configuration requires Linux shared memory. To use a Unix socket, copy the configuration and change its endpoint to an absolute `unix:///...` path, then pass the same endpoint to `tg_backtest`. Loopback TCP is also available through an endpoint such as `tcp://127.0.0.1:41001`.

### No market data found

Confirm that the import and client both use exchange `demo`, symbol `EURUSD`, and `target/quickstart/market_data`. Re-run the view command to verify the rows.

### Entry sizing is rejected

An Entry requires `--account-currency` and exactly one sizing basis: `--base-lot`, `--risk-per-trade`, or `--risk-percent`. This example uses fixed lot sizing so a stop is not required.

## Next steps

- Use [Backtesting](backtesting.md) to import your own data and select sizing or management profiles.
- Use the [RawSignal reference](reference/raw-signal.md) to add management actions.
- Review [Architecture](architecture.md) before embedding service or domain crates.
