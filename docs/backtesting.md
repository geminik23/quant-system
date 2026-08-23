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
- implement a stateful `HistoricalStrategy` that receives one complete timestamp boundary, read-only series, observations, engine state, and committed execution facts, then returns an optional bounded decision draft with ordered strict signals;
- bind a compiled `qs-strategy::ConfiguredStrategy` through `BacktestConfiguredStrategyAdapter`, complete source and named-input bindings, exact tick-count volume projection, total trade-slot facts, and ordered command-correlated feedback;
- run direct or configured historical strategies from a materialized `DataFeed` or complete `FallibleBatchFeed` timestamp stream through the existing FutureQuote scheduler and accounting path;
- emit bounded ordered non-economic journal drafts during ordinary callbacks, including warmup, without changing executable scheduling or decision retention;
- retain hindsight and journal-only annotations outside decision context and compare two completed strategy results through existing position-level metrics;
- provide strict timestamped `RawSignal` values for deterministic FutureQuote replay;
- supply a `DataFeed` implementation;
- consume structured reports and artifacts without starting a service.

The historical strategy path invokes its strategy boundary once per complete timestamp after FutureQuote settlement, series updates, and causal analysis. Generated fill-bearing signals cannot consume the quote that produced their decision, generated latency comes from `StrategyRequirements`, warmup advances causal material state but rejects configured transitions and economic signals, and only committed effects plus newly terminal dispositions are delivered as ordered feedback. The configured adapter preserves opaque command IDs through scheduling and consumes any remaining committed feedback at a final non-market boundary. Decision and journal retention do not suppress execution. Journal timestamps and sequence are runtime-owned, hypothetical records remain non-economic, and hindsight or journal-only annotations are returned only as research output. This library path is separate from the production service contract. Callers may implement `HistoricalStrategy` directly or use the configured adapter.

## Configured strategy core

`qs-strategy` now provides the reusable synchronous configured strategy core as a library. It compiles recursively strict configuration without a schema-version field against bounded logical source IDs and an explicit immutable material library, derives ordered per-source lookback and named-input requirements, evaluates bounded typed expressions and causal materials, and atomically commits deterministic finite-state transitions. Adapter-supplied inputs include authoritative time, readiness, ordered completed-bar updates for independently changing logical sources, total vacant/pending/open trade-slot facts, named values, and command-correlated committed feedback. Ordered output contains deterministic correlation IDs and strict `RawSignal` payloads plus generic decisions and notes.

The core advances causal materials while adapter readiness is false but leaves configured state and output unchanged. Feedback consumed while readiness is false updates custom pulse materials once and remains visible to built-in feedback conditions on the first ready input. Successful command correlations accept committed facts and terminal dispositions in either order and are released only when the action-specific lifecycle is complete. Compilation rejects invalid references, named-input schema conflicts, types, cycles, impossible material triggers, bounds, transition priorities, and unreachable states. Runtime failures commit no partial configured state or output.

This capability is library-only. Applications own configuration file loading and persistence. `qs-backtest` provides the historical configured adapter, which binds every logical source to a historical series specification, validates retained history and warmup against source-specific lookbacks, converts completed-bar volume to an exactly representable tick count, invokes caller-owned typed named-input projectors, projects every declared trade slot as vacant, pending, or open, and maps configured decisions, signals, and notes into existing historical output. Configured commands retain their opaque IDs through FutureQuote scheduling, effect and disposition facts preserve commit order, and a final feedback boundary resolves terminal facts without another market evaluation. Configured runs reject any supplied `ManagementProfile` before feed polling and reuse current unprofiled Entry resolution, sizing, currency conversion, accounting, MTM, and reports.

Neutral conformance covers no-op, EMA crossover, EMA/ATR lifecycle, pending cancellation, custom materials, profile rejection, final feedback, direct-signal economic parity, aligned-EOD materialized/streaming full-result parity, and strict research serde. Server or RPC execution, live runtime orchestration, persisted configured state, and configured-strategy composition with `ManagementProfile` remain unavailable.

## Operational boundaries

- Signal symbols and timestamps must overlap imported data.
- Explicit instrument specifications control catalog-aware quantity rules, contract multipliers, and supported economics; the symbol registry supplies the guarded compatibility form when no catalog is configured.
- The current replay implementation supports the existing quote-linear FX/CFD economics with standard-lot quantities. Declaring another model in a catalog does not make it executable.
- Catalog-backed Entry sizing normalizes prices to the declared display scale, validates the price grid, floors quantity with exact decimal grid arithmetic, validates post-rounding notional bounds, and records the adjustment. Other established engine-facing values remain compatibility-oriented `f64`; this is not a general decimal migration.
