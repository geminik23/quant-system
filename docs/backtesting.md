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

`--account-currency` is required when an Entry is present. Monetary risk sizing also requires a protective stop. `--market-entry-sizing-basis` selects `fill-price` or `signal-entry-price` for FutureQuote Market Entry quantity calculation; the default is `fill-price`, and `signal-entry-price` falls back to the fill when `Entry.price` is absent. `ScaleIn.size` remains a concrete final quantity and is not interpreted as an Entry risk multiplier.

An Entry may carry an optional exact, case-sensitive `entry_class`. `tg_backtest --entry-profile-map routes.toml` maps classes to named or inline management profiles while `--profile` remains the default for unlabeled entries. Unknown classes, duplicate routes, malformed labels, and missing named profiles fail before market data is consumed; they never fall back silently. Accepted retained jobs freeze the resolved definitions, so later profile add, remove, or reload operations do not change the run.

See the [RawSignal reference](reference/raw-signal.md) for action shapes.

## Instrument identity and economics

The server can load an optional strict instrument catalog through the `[instruments]` configuration section. A catalog contains exact asset metadata, broker- or exchange-qualified instrument identities, aliases, decimal price and quantity rules, effective intervals, and explicit economics descriptors. Its operator-assigned version identifies the immutable snapshot used for a run.

When `catalog_path` is omitted, the server compiles supported `qs-symbols` FX, metal, commodity, and index rows into a guarded compatibility snapshot. This preserves existing sizing, P&L, and economic-guard metadata. Registry-backed cryptocurrency and unknown categories remain excluded before market-data access.

`default_listing_venue` is an optional alias-resolution hint. Compatibility snapshots use `repository-default` when it is omitted. Explicit catalogs do not receive that implicit default, so one unique alias resolves directly and aliases shared by several venues fail as ambiguous unless the operator supplies the intended broker or exchange listing namespace. A platform such as CTrader is not a listing venue.

Each service replay result can include a typed instrument manifest containing the catalog version, resolved instrument and specification revision, effective specification, and actual Parquet partition and symbol coordinates. Catalog-backed Entry metadata also records exact requested and adjusted quantity, adjustment direction, and post-rounding notional when notional rules are configured. Existing `exchange` request and storage fields remain data coordinates and are not reinterpreted as broker, exchange listing, platform, or execution identity.

## Replay semantics

The production service uses deterministic FutureQuote replay.

- Tick replay uses stored bid and ask values.
- Bar replay prices each bar at its close and applies the bar's recorded spread symmetrically around it, which treats the close as the midpoint. A bar stored on a bid or ask basis therefore executes against a quote centred half a spread away from the real midpoint; resample on the midpoint basis for execution. A bar with no recorded spread uses the per-symbol fallback spread when the run configures one, and otherwise keeps the historical zero-spread approximation. Execution metadata counts every bar quote that executed with a zero spread, so the approximation is never silent.
- Actions become eligible according to signal timestamp and configured latency.
- Market entries use the appropriate future quote side and retain that actual fill for position state, P&L, MTM, and actual risk artifacts.
- Market Entry sizing defaults to the actual fill price. The optional signal-entry basis uses an explicit original `Entry.price` only for quantity calculation and falls back to the fill price when it is absent.
- Profiles, relative stops, targets, and rules still resolve from the actual fill price. Signal-entry sizing can therefore produce actual fill-to-stop risk above or below the requested risk, and the execution metadata records both price roles and the resulting quantity.
- Management profiles carry an `entry_geometry` policy. The `strict` default rejects an Entry during profile resolution when a signal stoploss or selected signal target sits on the wrong side of the execution price. `permissive` retains those signal levels at the resolver boundary, but the current engine still validates ordinary `Action::Open` geometry strictly; crossed signal levels can therefore still be rejected before a position opens. Profile-generated levels, profile rule levels, numeric validation, and unprofiled entries are always strict.
- The shipped profile file states each current geometry policy explicitly. Operators should inspect the deployed file rather than infer a policy from a profile name.
- Management actions that reference an already-closed position resolve as skipped no-ops with reason `position_closed` instead of failed actions.
- Limit and Stop Entries remain sized at placement from their required requested price; their quantity stays frozen through later fill, balance, FX, or quote changes.

Close-only bars cannot reconstruct an intrabar price path. Use tick data when exact ordering of stop, target, and management events matters.

## Trading costs

Commission and overnight swap are optional and off by default. A run without a cost specification produces exactly the artifacts it produced before costs existed.

Each symbol may declare:

- commission as a fixed amount per lot per side in the account currency, or as a notional rate per side with separate buy and sell rates for venues that charge asymmetrically;
- swap as price points per lot per night, or as an account-currency amount per lot per night, together with the rollover instant, the weekday that charges three nights, and the weekdays that charge none.

Commission is charged on every entry, scale-in, and closing fill. Swap is charged once per rollover instant crossed while a position is open, evaluated before any fill or signal at the same timestamp, so a position opened and closed between two rollovers pays none and a weekend gap still charges the instants it skipped.

Point-denominated swap and notional-rate commission are computed in the instrument's native profit-and-loss currency and converted through the run's existing conversion routes; per-lot amounts are already in the account currency and are validated against it. A swap charge that has no causal conversion quote is skipped and counted in execution metadata rather than silently applied.

Reported results are net of costs on one basis throughout. A close event carries the exit commission subtracted from its own profit and loss. A trade row carries that exit commission too, and the row that fully closes a position additionally settles that position's entry commission and swap, because those belong to the position rather than to any single close. Summing the trade log therefore reproduces the run's net profit and loss, and every figure derived from it — subset statistics, the equity curve, maximum drawdown, and the risk-adjusted ratios — sits on the same fully net basis as the completed positions and the account balance. Each completed position also reports commission and swap totals alongside a gross figure, realized R is computed from the net result, and provider evaluation gains a cost section showing the gross and net outcome, the total charged, and the cost share of the gross outcome.

A position still open when the run ends has no final row to settle against. Its charges are inside the run totals and the closing balance but not inside the trade log, which is the only place the two views differ.

Costs are configured through `BacktestConfig.costs` in the library, through `config.costs` in a service request, or through `tg_backtest --costs-file`. The wire and file forms use the same strict shape, reject unknown fields, and are validated at the request boundary before any data loading. The client reads its cost file before connecting, so an invalid file fails immediately.

```toml
[EURUSD]
commission = { type = "PerLotPerSide", amount = 3.5, currency = "USD" }
swap = { amount = { unit = "Points", long = -6.1, short = 1.9 }, rollover = "22:00:00", triple_weekday = "Wed" }

[BTCUSD]
commission = { type = "NotionalRatePerSide", buy_rate = 0.005, sell_rate = 0.005 }
```

Omitting `skipped_weekdays` keeps the weekend default of `Sat` and `Sun`; an explicitly empty list is read the same way, so a schedule that charges every weekday of the week cannot currently be expressed here. Tags also accept the lower-case spelling that a run writes into its own metadata, so a specification read back out of a stored run can be submitted again without rewriting it.

## Management profile routing and generated levels

Class routes are explicit run input and do not reuse `group` or `trade_id`. Group remains a reporting and bulk-management address, while trade ID remains per-trade management identity. Entries without a class use the run default profile or the existing unprofiled path when no default exists.

A profile can retain signal targets or generate targets from the final protective-stop distance. The generated form is strict and uses existing target weights, remainder handling, integer lot-step allocation, and zero-unit rejection:

```toml
[[profile]]
name = "risk_multiple_example"
use_targets = []
close_ratios = [0.5, 0.5]
let_remainder_run = false
entry_geometry = "strict"

[profile.stoploss_mode]
type = "FromSignalDistance"
multiplier = 1.5

[profile.target_source]
type = "StopDistanceMultiples"
multiples = [1.3, 1.6]
```

For a Buy at 100 with signal stop 80, the stop multiplier produces a requested stop at 70. The stop is aligned outward to the declared instrument price grid, and the final grid-adjusted stop distance becomes the R basis. If the final distance remains 30, the targets are 139 and 148 before any required outward target-grid adjustment. Sell calculations are symmetric. The multiplier is applied to price distance, so no pip conversion is required. Existing `FixedDistance` and trailing distances are raw price distances, not universal pip counts.

Market Entries resolve these levels from the actual fill. The optional signal-entry sizing basis changes quantity calculation only. Limit and Stop Entries resolve and freeze their levels and quantity at placement; a later fill gap does not regenerate them. Generated targets cannot be combined with signal-target selection or profile-level `TakeProfit` rules.

`BreakevenAfterTargets` counts executed take profits rather than a target identifier. A configured `TrailingStop` is active from the first rule evaluation; combining it with breakeven does not delay trailing until the first target. Initial generated targets are not regenerated after scale-in, break-even, or later stop/target management.

Execution metadata records frozen route definitions and applied Entry profile resolution, including class, selected profile, level reference, original and resolved stop, generated targets, grid adjustments, target allocation, and action ID.

A route file uses exact labels:

```toml
[[route]]
entry_class = "baseline"
profile = "signal_targets"

[[route]]
entry_class = "expanded"
profile = "risk_multiple_example"
```

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
- charge per-symbol commission and overnight swap during replay and read the resulting net figures, charge list, and per-position totals from the result;
- apply a per-symbol fallback spread to bars that carry none, and read how many bar quotes still executed with a zero spread;
- supply a `DataFeed` implementation;
- consume structured reports and artifacts without starting a service.

The historical strategy path invokes its strategy boundary once per complete timestamp after FutureQuote settlement, series updates, and causal analysis. Generated fill-bearing signals cannot consume the quote that produced their decision, generated latency comes from `StrategyRequirements`, warmup advances causal material state but rejects configured transitions and economic signals, and only committed effects plus newly terminal dispositions are delivered as ordered feedback. The configured adapter preserves opaque command IDs through scheduling and consumes any remaining committed feedback at a final non-market boundary. Decision and journal retention do not suppress execution. Journal timestamps and sequence are runtime-owned, hypothetical records remain non-economic, and hindsight or journal-only annotations are returned only as research output. This library path is separate from the production service contract. Callers may implement `HistoricalStrategy` directly or use the configured adapter.

## Configured strategy core

`qs-strategy` now provides the reusable synchronous configured strategy core as a library. It compiles recursively strict configuration without a schema-version field against bounded logical source IDs and an explicit immutable material library, derives ordered per-source lookback and named-input requirements, evaluates bounded typed expressions and causal materials, and atomically commits deterministic finite-state transitions. Adapter-supplied inputs include authoritative time, readiness, ordered completed-bar updates for independently changing logical sources, total vacant/pending/open trade-slot facts, named values, and command-correlated committed feedback. Ordered output contains deterministic correlation IDs and strict `RawSignal` payloads plus generic decisions and notes.

The core advances causal materials while adapter readiness is false but leaves configured state and output unchanged. Feedback consumed while readiness is false updates custom pulse materials once and remains visible to built-in feedback conditions on the first ready input. Successful command correlations accept committed facts and terminal dispositions in either order and are released only when the action-specific lifecycle is complete. Compilation rejects invalid references, named-input schema conflicts, types, cycles, impossible material triggers, bounds, transition priorities, and unreachable states. Runtime failures commit no partial configured state or output.

This capability is library-only. Applications own configuration file loading and persistence. `qs-backtest` provides the historical configured adapter, which binds every logical source to a historical series specification, validates retained history and warmup against source-specific lookbacks, converts completed-bar volume to an exactly representable tick count, invokes caller-owned typed named-input projectors, projects every declared trade slot as vacant, pending, or open, and maps configured decisions, signals, and notes into existing historical output. Configured commands retain their opaque IDs through FutureQuote scheduling, effect and disposition facts preserve commit order, and a final feedback boundary resolves terminal facts without another market evaluation. Configured runs select a `ManagementProfile` for each Entry exactly as raw-signal replay does: an Entry with an entry class uses its routed profile, and an unclassified Entry uses the run default profile or none. Before feed polling, a run rejects an entry class with no route and a selected profile that replaces the signal stop or attaches a stop-moving rule on a trade slot whose stop the strategy also moves. Resolution, sizing, currency conversion, accounting, MTM, and reports are the existing ones.

A configured source is fed either by ticks or by stored bars, never both. A stored bar is stamped at its bucket open, as the resampler writes it, and must declare its timeframe and a positive tick count; the market loader supplies both from storage. It feeds only the series with the same symbol and timeframe, must start on that series' aligned bucket boundary, and becomes the series' completed bar only when a later bucket arrives, which is the same visibility rule tick-built bars follow. A bar with no matching series, a misaligned or duplicate bar, inconsistent OHLC, or ticks mixed into a bar-fed series is rejected rather than ignored. Execution over bars keeps the existing bar-feed convention: quotes are the bar close with its recorded spread, so fills and stop settlement cannot reproduce intrabar order.

An open trade slot reports the entry fill time, the campaign's favorable and adverse excursion in account currency, and a positive initial risk on the same basis completed positions use for R. Excursion is read as of the start of the boundary's batch, before that batch's own quotes are marked, so the value never includes a price the strategy's bars do not yet include; it is missing until the position has been marked once. `bars_since_open` counts completed bars of one source after entry, and `weekday` and `seconds_of_day` follow the input time. `FixedUtcSessionProjector` is a neutral named-input projector that reports whether the boundary lies inside fixed UTC windows; venue or daylight-saving sessions remain caller-owned projectors.

Neutral conformance covers no-op, EMA crossover, EMA/ATR lifecycle, pending cancellation, custom materials, routed and run-default profile parity with direct signals, profile preflight rejection, stored-bar and tick parity of completed bars and decisions, open-position time, excursion, and initial-risk rules at exact boundaries, calendar and session inputs, final feedback, direct-signal economic parity, aligned-EOD materialized/streaming full-result parity, and strict research serde. Server or RPC execution, live runtime orchestration, and persisted configured state remain unavailable.

## Operational boundaries

- Signal symbols and timestamps must overlap imported data.
- Explicit instrument specifications control catalog-aware quantity rules, contract multipliers, and supported economics; the symbol registry supplies the guarded compatibility form when no catalog is configured.
- The current replay implementation supports the existing quote-linear FX/CFD economics with standard-lot quantities. Declaring another model in a catalog does not make it executable.
- Catalog-backed Entry sizing normalizes prices to the declared display scale, validates the price grid, floors quantity with exact decimal grid arithmetic, validates post-rounding notional bounds, and records the adjustment. For signal-entry Market sizing, stop-distance risk uses the selected signal reference while final notional uses the actual execution price. Other established engine-facing values remain compatibility-oriented `f64`; this is not a general decimal migration.
