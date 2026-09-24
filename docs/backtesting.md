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
- Bar replay reads each bar's open, high, low, and close as midpoints and applies the bar's recorded spread symmetrically around each of them. A bar with a non-finite price, or whose lowest quote would not be positive, counts as an invalid quote and is skipped, and a range that does not contain the open and close is widened to contain them. A bar stored on a bid or ask basis therefore executes against quotes centred half a spread away from the real midpoint; resample on the midpoint basis for execution. A bar with no recorded spread uses the per-symbol fallback spread when the run configures one, and otherwise executes at a zero spread. Execution metadata counts every bar that executed with a zero spread, so the approximation is never silent.
- A bar is replayed in three steps, all stamped at the bar's bucket open. First, orders waiting for the bar fill at its open: a market Entry, a close, or any other fill-bearing action scheduled before the bar executes at the open quote, and a stop or pending order the open already gapped through fills at the open. Second, the bar's range settles every stop, target, breakeven trigger, and pending Limit or Stop Entry whose level lies inside it, at the level itself rather than at the high or low. Third, the close marks open positions for equity, drawdown, excursion, and end-of-data liquidation; it fills nothing.
- Inside the range the order of the high and the low is unknown, so each side is walked from its adverse extreme first: long exposure meets the low before the high, and short exposure meets the high before the low. A long position whose stop and target both lie inside the bar therefore closes at its stop, targets alone fill nearest to the open first, and a pending Limit Entry filled on the way down can still be stopped on the same way down. A stop that a trailing or breakeven rule raises on the favorable leg cannot fill in the same bar, and a pending Stop Entry filled on the favorable leg meets only the rest of that leg. Excursion and equity use the open and close marks, so a position that survived the bar does not record its intrabar extreme.
- When a strategy-driven run reads one symbol at several bar lengths, as a portfolio of strategies on different timeframes can, execution uses only the shortest declared bars of that symbol, while longer bars still feed the strategy series that declare them. A raw-signal bar replay executes every bar it receives.
- Actions become eligible according to signal timestamp and configured latency.
- Market entries use the appropriate future quote side and retain that actual fill for position state, P&L, MTM, and actual risk artifacts.
- Market Entry sizing defaults to the actual fill price. The optional signal-entry basis uses an explicit original `Entry.price` only for quantity calculation and falls back to the fill price when it is absent.
- Profiles, relative stops, targets, and rules still resolve from the actual fill price. Signal-entry sizing can therefore produce actual fill-to-stop risk above or below the requested risk, and the execution metadata records both price roles and the resulting quantity.
- Management profiles carry an `entry_geometry` policy. The `strict` default rejects an Entry during profile resolution when a signal stoploss or selected signal target sits on the wrong side of the execution price. `permissive` retains those signal levels at the resolver boundary, but the current engine still validates ordinary `Action::Open` geometry strictly; crossed signal levels can therefore still be rejected before a position opens. Profile-generated levels, profile rule levels, numeric validation, and unprofiled entries are always strict.
- The shipped profile file states each current geometry policy explicitly. Operators should inspect the deployed file rather than infer a policy from a profile name.
- Management actions that reference an already-closed position resolve as skipped no-ops with reason `position_closed` instead of failed actions.
- Limit and Stop Entries remain sized at placement from their required requested price; their quantity stays frozen through later fill, balance, FX, or quote changes.

Bars cannot reconstruct the real intrabar price path. The adverse-extreme-first rule is the pessimistic choice for each position, which makes a bar run a fast and conservative approximation. Use tick data when the exact ordering of stop, target, and management events matters, and confirm a bar-searched candidate over ticks.

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

A configured source is fed either by ticks or by stored bars, never both. A stored bar is stamped at its bucket open, as the resampler writes it, and must declare its timeframe and a positive tick count; the market loader supplies both from storage. It feeds only the series with the same symbol and timeframe, must start on that series' aligned bucket boundary, and becomes the series' completed bar only when a later bucket arrives, which is the same visibility rule tick-built bars follow. A bar with no matching series, a misaligned or duplicate bar, inconsistent OHLC, or ticks mixed into a bar-fed series is rejected rather than ignored. On a bar boundary a configured strategy decides before the new bars trade, having seen only the completed previous bar, so its market orders fill at the open of the bar that follows the one that produced the signal; execution then settles the bar's range as described under replay semantics. A direct Rust strategy is handed the boundary's own bars, including their close, so it decides after those bars settle and its orders fill at the next bar's open. A tick-driven strategy still decides after its boundary's ticks settle and fills on the next tick.

An open trade slot reports the entry fill time, the campaign's favorable and adverse excursion in account currency, and a positive initial risk on the same basis completed positions use for R. Excursion is read as of the start of the boundary's batch, before that batch's own quotes are marked, so the value never includes a price the strategy's bars do not yet include; it is missing until the position has been marked once. `bars_since_open` counts completed bars of one source after entry, and `weekday` and `seconds_of_day` follow the input time. `FixedUtcSessionProjector` is a neutral named-input projector that reports whether the boundary lies inside fixed UTC windows; venue or daylight-saving sessions remain caller-owned projectors.

Neutral conformance covers no-op, EMA crossover, EMA/ATR lifecycle, pending cancellation, custom materials, routed and run-default profile parity with direct signals, profile preflight rejection, stored-bar and tick parity of completed bars and decisions, open-position time, excursion, and initial-risk rules at exact boundaries, calendar and session inputs, final feedback, direct-signal economic parity, aligned-EOD materialized/streaming full-result parity, and strict research serde. Live runtime orchestration and persisted configured state remain unavailable.

## Portfolios of configured strategies

`BacktestRunner::run_portfolio_future` replays several configured strategy instances against one account. Each instance keeps its own series, analysis, decisions, notes, and entry-profile routes, and trades one symbol; fills, balance, costs, marks, drawdown, and the report are shared. Instances may trade different symbols, the same symbol with different parameters, or the same symbol at different bar lengths. Every instance sees feedback only for its own commands, every position carries an `instance` tag naming the instance that opened it, so provider evaluation can break results down by instance, and the result returns each instance's decisions beside the shared replay. Every instance needs its own instance identifier, which labels its positions, reviews, and output and scopes its trade and command identifiers. The shared feed is all ticks or all stored bars, each instance reads market data from its own derived warmup start even when the feed begins earlier for another instance, an instance whose symbol the account cannot size or settle is rejected before the feed is read, and one failing instance fails the whole run. One instance without a supervisor produces the same economic result as the single-instance runner.

An optional `PortfolioSupervisor` from `qs-risk` reviews every Entry and scale-in an instance generates before it is scheduled and approves or rejects it; it never blocks a close, a partial close, a stop move, or a cancellation. A rejected request reaches its instance as a rejected command with the policy and figures in its reason. The policies are:

- `max_open_positions` and `max_open_per_symbol`, which count open and pending positions and approved entries that have not filled yet;
- `group_risk_cap`, which bounds the combined risk of a declared correlation group, where each position's risk is the account-currency amount lost if its stop fills; it needs a monetary sizing policy, because a fixed-lot entry's risk is unknown until it fills, and it rejects any request or carried position whose risk cannot be measured;
- `daily_loss_halt`, which rejects new exposure from the moment the day's realized loss, net of costs, reaches an amount, a percent of the balance at the reset instant, or a number of realized R, until the next daily reset instant in UTC;
- `kill_switch`, which rejects new exposure for the rest of the run once marked equity falls a given percent below its peak, and with `halt_and_close_all` also closes every position at the next quote.

When a halt begins it cancels pending orders and rejects every approved Entry or scale-in that has not reached the market yet, such as one waiting for its symbol's next quote or for its decision latency, so a halt leaves no new exposure behind. Several kill switches may form tiers, for example halting at one drawdown and closing everything at a deeper one. The result records every review, every halt action, and every halt interval; an interval without an end was still in force when the run ended.

## Configured strategies through the service

## Configured strategies through the service

The backtest service accepts configured strategies at runtime, so a new strategy runs without rebuilding or restarting the server. Six methods sit beside the raw-signal methods and share their retained-job workflow, status, watch, cancellation, results, and artifacts:

- `run_configured_strategy` and `submit_configured_strategy` run one bound strategy document over one symbol;
- `run_portfolio` and `submit_portfolio` run several configured instances against one account, optionally under portfolio policies;
- `submit_search` runs a parameter search over a strategy template and a space document;
- `get_search_result` returns a completed search's summary and the artifact holding its complete output.

A configured request carries the document as a JSON value together with the geometry of each logical source (timeframe, price basis, and alignment), the data scope, and the same `profile`, `profile_def`, and `entry_profile_routes` fields a raw-signal request uses. The service decodes the document strictly, compiles it with the built-in material library, binds its sources, derives warmup from the compiled requirements, checks profile routing and sizing, and only then admits a job; an unknown field, an unknown material, an unresolved parameter, or an unrouted entry class is reported with its location before any data is read. Loading starts at the source-aligned warmup start, the run goes through the same stream, conversion, instrument, and FutureQuote path as a raw-signal run, and the result carries the decoded document, the source geometry, the compiled requirements, the data mode, the configured decisions, and the notes. A request with `data_type = "bar"` runs over stored bars of the declared timeframe under the bar replay rules described above.

A portfolio request lists its instances, each with a symbol, a strategy document and source geometry, a required `instance_id`, and its own profile fields, over one shared data scope, sizing, and account. Its `policies` and `groups` travel as JSON values that the service decodes strictly into portfolio policies, as it decodes documents. Every instance and policy is validated before admission, including identity uniqueness, the combined retained history, and a group cap's need for monetary sizing. The service loads the symbols of all instances into one feed from the earliest derived warmup start, lets each instance read from its own, and returns the shared result with each instance's document, requirements, and decisions and the supervisor's reviews and halts. A bar portfolio request loads one bar timeframe, which every instance source declares.

A search request carries a template and a space document as JSON values, one or more symbols, fixed or rolling windows, sizing, and profiles referenced by registered name only. The whole search is validated without loading data, including every parameter point, the run count against the server limit, and the stored-data range the batch reads. A search replays primary events without conversion quotes, so every searched symbol must settle in the account currency. Only one search holds its data in memory at a time; others wait. The worker count a request asks for is capped by the server, cancellation takes effect after the current run, and progress is reported after each run.

`strategy_backtest --run run.toml --out result.json` submits any of these requests from a TOML run file and waits for it. A run file names `strategy`, a list of `[[instances]]`, or `template` and `space`, by path relative to itself; documents stay in their TOML authoring form and are converted to JSON when the request is built.

```toml
strategy = "strategy.toml"
exchange = "demo"
symbol = "EURUSD"
data_type = "tick"
from = "2026-01-05T02:00:00"
to = "2026-01-05T15:00:00"
account_currency = "USD"

[config]
initial_balance = 10000.0
sizing = { type = "FixedLot", lots = 0.1 }

[[series]]
source = "primary"
timeframe_seconds = 60
price_basis = "mid"
```

A search run file replaces `strategy` and `[[series]]` with `template`, `space`, optional `symbols` and `workers`, and a `[windows]` table such as `type = "fixed"` with `in_sample` and `out_of_sample` entries; its series geometry comes from the space document. A portfolio run file keeps the shared scope and `[config]` and lists its instances and policies:

```toml
[[instances]]
strategy = "strategy.toml"
symbol = "EURUSD"
instance_id = "eur"
[[instances.series]]
source = "primary"
timeframe_seconds = 60
price_basis = "mid"

[[instances]]
strategy = "strategy.toml"
symbol = "GBPUSD"
instance_id = "gbp"
[[instances.series]]
source = "primary"
timeframe_seconds = 60
price_basis = "mid"

[[policies]]
type = "max_open_positions"
limit = 1

[[policies]]
type = "daily_loss_halt"
max_loss = { account_percent = 2.0 }
reset_at_utc = "22:00:00"
```

The `[strategies]` section of the server configuration bounds document size, retained history per run, search workers, search runs, and portfolio instances.

## Operational boundaries

- Signal symbols and timestamps must overlap imported data.
- Explicit instrument specifications control catalog-aware quantity rules, contract multipliers, and supported economics; the symbol registry supplies the guarded compatibility form when no catalog is configured.
- The current replay implementation supports the existing quote-linear FX/CFD economics with standard-lot quantities. Declaring another model in a catalog does not make it executable.
- Catalog-backed Entry sizing normalizes prices to the declared display scale, validates the price grid, floors quantity with exact decimal grid arithmetic, validates post-rounding notional bounds, and records the adjustment. For signal-entry Market sizing, stop-distance risk uses the selected signal reference while final notional uses the actual execution price. Other established engine-facing values remain compatibility-oriented `f64`; this is not a general decimal migration.
