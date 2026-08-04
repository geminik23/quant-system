# Backtesting

The historical path combines data import, strict normalized signals, deterministic replay, account sizing, and result delivery.

```text
supported tick/bar export
  -> qs-data-preprocess
  -> partitioned Parquet
  -> backtest server replay plan
  -> FutureQuote execution and accounting
  -> inline result or verified artifact
```

Start with the [repository-owned example](getting-started.md) before using external data.

## Historical data

`qs-data-preprocess` imports the documented MetaTrader-style tick and bar exports, converts timestamps to UTC, and stores normalized partitions in Parquet by default.

See the [data-preprocess guide](../crates/data-preprocess/GUIDE.md) for exact columns, time zones, storage backends, queries, and data-management commands.

## Signal input

The service consumes one strict `RawSignal` JSON object per line. Parsing is optional: any trusted producer can write the supported wire shape directly. Entry and later management actions can be mixed in one stream.

Every Entry must contain a finite positive `risk` multiplier. It does not contain a final `size`. The client resolves Entry quantity using exactly one of:

- `--base-lot`;
- `--risk-per-trade`;
- `--risk-percent`.

`--account-currency` is required when an Entry is present. Monetary risk sizing also requires a protective stop. `ScaleIn.size` remains a concrete final quantity and is not interpreted as an Entry risk multiplier.

See the [RawSignal reference](reference/raw-signal.md) for action shapes.

## Replay semantics

The production service uses deterministic FutureQuote replay.

- Tick replay uses stored bid and ask values.
- Bar replay converts each bar close into a zero-spread synthetic quote.
- Actions become eligible according to signal timestamp and configured latency.
- Market entries use the appropriate future quote side.

Close-only bars cannot reconstruct an intrabar price path. Use tick data when exact ordering of stop, target, and management events matters.

## Service execution

Configure the data root, symbol registry, management profiles, retained jobs, and result artifacts through [`config.example.toml`](../crates/backtest-server/config.example.toml).

The client streams retained-job progress by default. Polling and finite synchronous execution remain available as explicit alternatives. Results may be returned inline or stored as verified artifacts when they exceed the inline limit.

## Library execution

`qs-backtest` also exposes in-process APIs:

- implement `Strategy` for strategy-driven replay;
- provide timestamped signals for predefined replay;
- supply a `DataFeed` implementation;
- consume structured reports and artifacts without starting a service.

This library path is separate from the production service contract.

## Operational boundaries

- Signal symbols and timestamps must overlap imported data.
- Symbol metadata controls lot steps, currencies, and supported economics.
