# Parameter search

`qs-research` binds a parameterized strategy document to a declared search space, replays every surviving point over paired evaluation windows, and exposes both a comparison table and pooled provider evaluation. It never chooses a winner or adds a score, rank, rating, or "best" column.

## Documents

The normal workflow uses two TOML files:

- a strategy document declares typed parameters and references them from expressions or material arguments;
- a space document supplies values or integer ranges, filters combinations with parameter-only constraints, and declares the historical series geometry backing each logical source.

The runnable example uses [`ema_strategy.toml`](../crates/research/examples/ema_strategy.toml) and [`ema_space.toml`](../crates/research/examples/ema_space.toml). The strategy declares three entry conditions behind a `select`; the space visits each condition alongside EMA periods and an ATR stop multiple.

A parameter is fixed for one run and is substituted before compilation. It is different from an `input`, which an adapter supplies at every evaluation, and from a variable, which strategy state may change during a run. Binding produces a complete ordinary `StrategyConfig`; unresolved parameters never reach the runtime.

A research batch runs without management profiles, so every Entry uses its own signal stop and targets. If a document's Entry declares an `entry_class`, every run fails before replay with an unrouted-class error and is recorded as a failed row, because the batch supplies no route for that class.

A strategy and space are loaded explicitly:

```rust
let family = DeclaredSpace::load("strategy.toml", "space.toml")?;
let batch = run_batch(&plan, &family, &events)?;
```

There are no includes, overlays, environment substitutions, directory discovery, or hot reload. A caller-defined Rust `StrategyFamily` remains available for spaces that range products and constraints cannot express, but it is the escape hatch rather than the default authoring workflow.

The checked-in strategy file uses the complete tagged document model and is intentionally explicit, but deeply nested expressions are verbose in TOML. A shorter typed structural authoring form is not implemented yet; when added, it must lower into this same model rather than introduce a string expression language or a second compiler path.

## Material parameters and indicators

Each material factory declares a typed, bounded parameter schema. The compiler rejects unknown arguments, missing required arguments, incorrect types, and values outside the declared bounds before replay starts. Built-in and caller-registered factories use the same path, so a custom period-based material does not require a new core enum variant.

The built-in numerical primitives are:

- `ema` and `atr`;
- `sma` and population `stddev`;
- `rolling_min` and `rolling_max`;
- `lag`;
- Wilder-smoothed `rsi`;
- `cross_above` and `cross_below` over derived expressions or a value and a literal level.

More elaborate indicators are expression compositions rather than separate special cases. For example, MACD uses two EMAs and subtraction, Bollinger bands use SMA plus standard deviation, stochastic values use rolling extrema and SMA, and rate of change uses lag and division.

Binding removes materials not referenced by the selected expression branches before the strategy is compiled. This prevents an unused branch from increasing the run's required warmup.

## Search spaces

A space binds every declared strategy parameter exactly once. Integer and number parameters may use an inclusive positive-step range or explicit values; choice parameters use explicit values or `all = true`.

```toml
[parameters.ema_fast]
range = { from = 5, to = 20, step = 5 }

[parameters.ema_slow]
values = [30, 50]

[parameters.atr_stop]
values = [1.0, 1.5, 2.0]

[parameters.entry]
all = true

[[constraints]]
op = "lt"
left = { op = "param", id = "ema_fast" }
right = { op = "param", id = "ema_slow" }
```

Constraints reuse the typed expression model but may contain only parameters and literals. Runtime inputs, materials, position facts, feedback, and bar fields are rejected in a space constraint.

Enumeration follows strategy parameter declaration order and is independent of map iteration. `points_total` reports the number of combinations remaining after constraints.

## Geometry and warmup

Each logical source has declared geometry: source ID, symbol, timeframe, price basis, and alignment offset. Geometry fields may reference parameters, allowing a space to compare timeframes without regenerating Rust code.

Warmup and retained history are derived from the bound strategy's compiled `CompletedBarRequirement`. A family does not restate indicator lookback. Chained rolling materials compose their lookbacks, and branch pruning occurs before requirements are derived.

A search runs in process through `run_batch`, or on the backtest service through `submit_search`, which loads the data the batch needs once, runs it with the same batch code, and returns the table, pooled evaluation, and bound documents as one artifact. `run_batch_controlled` adds cancellation before each run and progress after each run, and `validate_batch` and `batch_data_range` check a batch and compute the stored-data range it reads without loading any data. A search runs over ticks or over stored bars. `load_symbol_ticks` and `load_symbol_bars` each read one symbol's range into memory once, and every row and position records the input as its `data_mode`, `ticks` or `bars`; a symbol whose events mix the two is rejected. A stored bar feeds only a source declared with the same timeframe, and it becomes visible to the strategy only after its bucket closes, so a bar search sees the same completed bars as a tick search over the ticks those bars were resampled from. Execution differs: a bar run fills waiting orders at each bar's open and settles stops, targets, and pending orders against the bar's range with each side meeting its adverse extreme first, which is conservative but cannot reproduce the real order of events inside a bar, so a candidate found over bars should be confirmed over ticks.

## Windows

A run is evaluated over paired windows:

```rust
WindowPlan::Fixed { in_sample, out_of_sample }
WindowPlan::RollingWalkForward { start, end, train, test, step }
```

Windows are half-open: events at `to` belong to the following window. Fixed in-sample and out-of-sample labels must be distinct. A rolling split is emitted only when its complete test span fits inside the declared range.

The feed includes the derived warmup preceding `from`. Positions still open when the window ends are closed by the run and counted in `forced_closes`. Every row reports `entries_before_window` so an early-entry violation remains visible.

## Batch results

`run_batch` returns `ResearchBatch`:

```rust
let table = batch.table();
let report = batch.evaluate(EvaluationOptions {
    breakdowns: vec![BreakdownDimension::Tag("entry".to_owned())],
    ..EvaluationOptions::default()
});
let document = batch.bound_document(0);
```

The table and pooled evaluation are projections of the same retained runs. Each normalized position ID is prefixed with a deterministic run ordinal before pooling, so IDs remain unique across symbols, windows, parameter points, and worker schedules.

Rows are ordered by family, symbol, parameter labels, and window, never by a metric. Individual compilation, binding, or replay failures become failed rows while other runs continue. Invalid plans, documents, spaces, labels, or input ordering fail before the batch starts.

Run tags carry parameter values, symbol, window, and data mode onto every completed position. Pooled evaluation can therefore produce one bucket per parameter value across the complete search rather than the single bucket available inside one run.

## Portfolio plans

`ResearchPlan::with_portfolio` replays every plan symbol together, one instance per symbol against one account, instead of one run per symbol. Each run then covers one parameter point over one window, the row's symbol names every symbol joined with `+`, each position carries an `instance` tag naming the symbol that traded it, and each instance reads from its own derived warmup start. A `PortfolioPlan` with policies builds a fresh supervisor for every run, and its rows report `rejected_entries` and `halt_minutes`, a halt still in force counting to the window's end; the CSV adds those two columns only when a run was supervised. All symbols must be ticks or all stored bars, `instance` joins the reserved tag names, and a group risk cap requires a monetary sizing policy.

## Example

```bash
cargo run -p qs-research --example ema_grid
```

The example writes deterministic synthetic ticks into a temporary Parquet store, loads them through `qs-market-loader`, reads the two TOML documents, runs the declared space with commission charged, writes `target/research/ema_grid.csv`, and prints pooled breakdown counts by entry condition and window. The synthetic series is an execution fixture and has no economic meaning.
