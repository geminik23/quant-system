# Parameter search

`qs-research` binds a parameterized strategy document to a declared search space, replays every surviving point over paired evaluation windows, and exposes both a comparison table and pooled provider evaluation. It never chooses a winner or adds a score, rank, rating, or "best" column.

## Documents

The normal workflow uses two TOML files:

- a strategy document declares typed parameters and references them from expressions or material arguments;
- a space document supplies values or integer ranges, filters combinations with parameter-only constraints, and declares the historical series geometry backing each logical source.

The runnable example uses [`ema_strategy.toml`](../crates/research/examples/ema_strategy.toml) and [`ema_space.toml`](../crates/research/examples/ema_space.toml). The strategy declares three entry conditions behind a `select`; the space visits each condition alongside EMA periods and an ATR stop multiple.

A parameter is fixed for one run and is substituted before compilation. It is different from an `input`, which an adapter supplies at every evaluation, and from a variable, which strategy state may change during a run. Binding produces a complete ordinary `StrategyConfig`; unresolved parameters never reach the runtime.

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

Only tick-driven search is currently available. A configured strategy derives its analysis bars from ticks while replay proceeds; stored bars do not yet drive configured replay.

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

## Example

```bash
cargo run -p qs-research --example ema_grid
```

The example writes deterministic synthetic ticks into a temporary Parquet store, loads them through `qs-market-loader`, reads the two TOML documents, runs the declared space with commission charged, writes `target/research/ema_grid.csv`, and prints pooled breakdown counts by entry condition and window. The synthetic series is an execution fixture and has no economic meaning.
