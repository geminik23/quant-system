# Backtesting

The historical path combines data import, strict normalized signals, deterministic replay, account sizing, and result delivery.

```text
supported tick/bar export
  -> qs-data-preprocess
  -> partitioned Parquet
  -> instrument catalog resolution and replay plan
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

## Instrument identity and economics

The server can load an optional strict instrument catalog through the `[instruments]` configuration section. A catalog contains exact asset metadata, broker- or exchange-qualified instrument identities, aliases, decimal price and quantity rules, effective intervals, and explicit economics descriptors. Its operator-assigned version identifies the immutable snapshot used for a run.

When `catalog_path` is omitted, the server compiles supported `qs-symbols` FX, metal, commodity, and index rows into a guarded compatibility snapshot. This preserves existing sizing, P&L, and economic-guard metadata. Registry-backed cryptocurrency and unknown categories remain excluded before market-data access.

`default_listing_venue` is an optional alias-resolution hint. Compatibility snapshots use `repository-default` when it is omitted. Explicit catalogs do not receive that implicit default, so one unique alias resolves directly and aliases shared by several venues fail as ambiguous unless the operator supplies the intended broker or exchange listing namespace. A platform such as CTrader is not a listing venue.

Each service replay result can include a typed instrument manifest containing the catalog version, resolved instrument and specification revision, effective specification, and actual Parquet partition and symbol coordinates. Catalog-backed Entry metadata also records exact requested and adjusted quantity, adjustment direction, and post-rounding notional when notional rules are configured. Existing `exchange` request and storage fields remain data coordinates and are not reinterpreted as broker, exchange listing, platform, or execution identity.

## Replay semantics

The production service uses deterministic FutureQuote replay.

- Tick replay uses stored bid and ask values.
- Bar replay converts each bar close into a zero-spread synthetic quote.
- Actions become eligible according to signal timestamp and configured latency.
- Market entries use the appropriate future quote side.

Close-only bars cannot reconstruct an intrabar price path. Use tick data when exact ordering of stop, target, and management events matters.

## Service execution

Configure the data root, optional instrument catalog, symbol compatibility registry, management profiles, retained jobs, and result artifacts through [`config.example.toml`](../crates/backtest-server/config.example.toml).

The client streams retained-job progress by default. Polling and finite synchronous execution remain available as explicit alternatives. Results may be returned inline or stored as verified artifacts when they exceed the inline limit.

## Library execution

`qs-backtest` also exposes in-process APIs:

- implement the legacy `Strategy` trait for action-producing replay;
- describe future historical strategies with validated `StrategyDescriptor`, `StrategyRequirements`, fixed-duration `Timeframe`, per-series warmup, and bounded decision-record contracts;
- derive bounded causal closed bars from complete primary-tick timestamp batches with explicit bid, ask, or midpoint aggregation;
- inspect retained history and exact per-series or aggregate warmup readiness through read-only series views;
- provide strict timestamped `RawSignal` values for deterministic FutureQuote replay;
- supply a `DataFeed` implementation;
- consume structured reports and artifacts without starting a service.

The historical strategy domain, standalone causal multi-timeframe series, and complete-boundary causal analysis layer are available. The analysis layer provides bounded immutable observations, causal annotation scheduling, and confirmed pivots without invoking a strategy or altering execution prices; stateful `HistoricalStrategy` callbacks, execution feedback, and dynamic FutureQuote signal integration are not implemented yet. This library path is separate from the production service contract.

## Operational boundaries

- Signal symbols and timestamps must overlap imported data.
- Explicit instrument specifications control catalog-aware quantity rules, contract multipliers, and supported economics; the symbol registry supplies the guarded compatibility form when no catalog is configured.
- The current replay implementation supports the existing quote-linear FX/CFD economics with standard-lot quantities. Declaring another model in a catalog does not make it executable.
- Catalog-backed Entry sizing normalizes prices to the declared display scale, validates the price grid, floors quantity with exact decimal grid arithmetic, validates post-rounding notional bounds, and records the adjustment. Other established engine-facing values remain compatibility-oriented `f64`; this is not a general decimal migration.
