# Parameter search

`qs-research` binds a parameterized strategy document to a declared search space, replays every surviving point over paired evaluation windows, and exposes both a comparison table and pooled provider evaluation. It never chooses a winner or adds a score, rank, rating, or "best" column.

## Documents

The normal workflow uses two TOML files:

- a strategy document declares typed parameters and references them from expressions or material arguments;
- a space document supplies values or integer ranges, filters combinations with parameter-only constraints, and declares the historical series geometry backing each logical source.

The runnable example uses [`ema_strategy.toml`](../crates/research/examples/ema_strategy.toml) and [`ema_space.toml`](../crates/research/examples/ema_space.toml). The strategy declares three entry conditions behind a `select`; the space visits each condition alongside EMA periods and an ATR stop multiple.

A parameter is fixed for one run and is substituted before compilation. It is different from an `input`, which an adapter supplies at every evaluation, and from a variable, which strategy state may change during a run. Binding produces a complete ordinary `StrategyConfig`; unresolved parameters never reach the runtime.

A research batch may run unprofiled or with an immutable `PreparedEntryProfiles` snapshot supplied through `ResearchPlan::with_entry_profiles`. Unclassified Entries use the run default profile when present; classified Entries require an exact route. Missing routes and stop-ownership conflicts fail explicitly rather than falling back.

A strategy and space are loaded explicitly:

```rust
let family = DeclaredSpace::load("strategy.toml", "space.toml")?;
let batch = run_batch(&plan, &family, &events)?;
```

There are no includes, overlays, environment substitutions, directory discovery, or hot reload. A caller-defined Rust `StrategyFamily` remains available for spaces that range products and constraints cannot express, but it is the escape hatch rather than the default authoring workflow.

The checked-in strategy file uses the complete tagged document model and is intentionally explicit. `StructuralSearchSpec` provides bounded typed generation for predicates, `not`, `and`, `or`, sequence, and capture/retest candidates; every generated candidate lowers into this same canonical document model and compiler rather than introducing a string expression language or a second execution path.

## Material parameters and indicators

Each material factory declares a typed, bounded parameter schema. The compiler rejects unknown arguments, missing required arguments, incorrect types, and values outside the declared bounds before replay starts. Built-in and caller-registered factories use the same path, so a custom period-based material does not require a new core enum variant.

The built-in numerical primitives are:

- `ema` and `atr`;
- `sma` and population `stddev` (legacy valid-sample windows), plus `strict_sma` and `strict_ema` for observed-bar windows and SMA-seeded EMA;
- `rolling_min` and `rolling_max`;
- `lag`;
- Wilder-smoothed `rsi`;
- `cross_above` and `cross_below` over derived expressions or a value and a literal level.

The new `strict_sma` and `strict_ema` require a `source` argument naming the completed-bar sampling clock and a `period` from 1 to 1024. They currently accept exactly one direct bar field on that source or one typed numeric named input; arbitrary mixed-source expressions are rejected. For a named input, the caller-owned projector must bind it to the declared bar source: the compiler enforces the declared clock and checks its `updated` flag on that clock, but cannot independently verify the projector's source provenance. `strict_sma` counts an observed Missing input as a window position, while `strict_ema` seeds from the first complete period and restarts its own seed after an observed Missing. A named input marked `updated = false` on a source-bar update is treated as a missing observation rather than silently reusing its retained value. No bar update means neither evaluator advances. Existing `sma` and `ema` keep their earlier valid-sample and first-valid seed behavior. A configured transition may wrap Boolean expressions in `strict` to preserve invalidity through comparisons, `not`, `all`, and `any`: it fires only when the result is valid true. Legacy expressions keep their previous comparison and Boolean behavior.

Strict presence diagnostics evaluate their operand under the enclosing strict rules: `strict(is_present(...))` does not turn a missing comparison into a present `false`. Explicit `is_missing(strict(...))` and `is_present(strict(...))` return required Booleans and can be compared normally; this is intentional observation of validity, not implicit coercion into a trading predicate.

`MaterialLibrary::numeric_descriptor` and `ConfiguredStrategy::numeric_descriptors` expose effective source clock, actual scalar or OHLC inputs, semantic unit/range, bounded parameters, calculation, seed/Missing policy, first-output count, composed lookback, and evaluator state bound. The compiler checks descriptors against factory inputs, trigger, output type, effective lookback, and state declaration. Public scalar types distinguish Price, Ratio, Percent, observation-normalized units, log return, and log-return variance, so a Number, Ratio, Percent, and Price threshold cannot be interchanged silently. This does not prove arbitrary custom-factory arithmetic or caller projector provenance; descriptor-free custom factories retain their previous contract.

Both strict averages use fixed-size observed storage, including staged clones. Their means accumulate finite binary64 samples exactly and round after division; strict EMA updates likewise round the rational expression `(2 * sample + (period - 1) * previous) / (period + 1)` once. This preserves representable cancellation residuals and flat subnormal inputs, but is not guaranteed bit-identical to separately rounded floating-point products. Existing legacy averages are unchanged. Required warmup is a first-output minimum, not a convergence guarantee or independence from initialization history.

Compatibility note: the new `Expr::Strict` variant requires downstream exhaustive Rust matches on `Expr` to be updated. Existing serialized expression forms retain their meanings, but older readers do not understand the new variant, and callers cannot register custom factories under the newly reserved `strict_sma` or `strict_ema` keys.

The named catalog now covers bar shape and relative structures; log/simple changes; selected prices; strict SMA/EMA/RMA/WMA/ATR; MA distance, slope, acceleration, gap and alignment; Wilder RSI, Stochastic, MACD, CCI and DMI/ADX; Donchian/range, Bollinger, Keltner, squeeze and median outputs; return volatility, EWMA, semivariance, percentile, ER, regression, extrema age, Aroon, CHOP and autocorrelation; HMA, KAMA, SuperTrend and Heikin-Ashi; and previous/current ATR-normalized variants. Every registered output is exercised through canonical configured execution, and the stored-bar adapter records and compares the same typed output value on equivalent input history with a fixture-specific floating-point tolerance. Family oracles cover formulas, initialization, recovery, extrema ties, and recursive behavior independently from that integration comparison. Legacy comparisons and `not` still keep their prior Missing behavior; use `strict` for validity-sensitive conditions.

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

Enumeration follows strategy parameter declaration order and is independent of map iteration. Axis cardinality and the pre-constraint Cartesian product are checked before expansion; constraints cannot justify an unbounded intermediate product. `DeclaredSpaceLimits` selects axis, pre-constraint, and admitted-point limits, while `ResearchAdmissionLimits` bounds rolling-window pairs and scheduled runs. Existing convenience constructors use documented bounded compatibility defaults, and the service applies its stricter configured run limit during both admission and worker reconstruction. `points_total` reports the number of combinations remaining after constraints.

## Geometry and warmup

Each logical source has declared geometry: source ID, symbol, timeframe, price basis, and alignment offset. Geometry fields may reference parameters, allowing a space to compare timeframes without regenerating Rust code.

Warmup and retained history are derived from the bound strategy's compiled `CompletedBarRequirement`. A family does not restate indicator lookback. Chained rolling materials compose their lookbacks, and branch pruning occurs before requirements are derived.

A search runs in process through `run_batch`, or on the backtest service through `submit_search`, which loads the data the batch needs once, runs it with the same batch code, and returns the table, pooled evaluation, bound documents, effective recipes, generation disposition, and a bounded resume checkpoint as one artifact. A strict structural document may accompany a one-point base space and is decoded against the server's trusted material catalog before job admission. `run_batch_controlled` and the direct/variant controlled entry points check cancellation between runs and report progress without publishing a success batch after cancellation. `validate_batch` and `batch_data_range` check a batch and compute the stored-data range it reads without loading any data. A search runs over ticks or over stored bars. Legacy loaders preserve their established materialized compatibility path. Enhanced ordered-tick and price-bar inputs provide fingerprinted, slice-bounded Parquet cursors that validate monotonic order across reads and check cancellation without materializing the full dataset. Their materialized compatibility loaders retain explicit row and resident-byte limits. Every row and position records the input as its `data_mode`, `ticks` or `bars`; a symbol whose primary events mix the two is rejected. The additive price-only storage path uses `PriceBar.tick_count: Option<u64>` and never maps unknown to zero, one, or trade volume. `PriceBar.available_at` is retained separately from the nominal bucket-open timestamp, orders delayed bars by actual availability, and never lets a delayed bar execute retrospectively through its old range. Service searches select these paths with `data_type = "ordered_tick"` or `data_type = "price_bar"`; price-bar requests supply one verified `SeriesDescriptor` per symbol, and service-owned row/feed/resident limits bound the cursor and the shared in-memory research view. Price-only configured candidates receive Missing volume and run normally; a document that directly requires count rejects unknown-count bars before replay. Strict integer-multiple parent aggregation propagates unknown count, checks complete ordered child coverage, and never flushes an incomplete tail. Legacy `Bar { tick_vol: i64 }` remains supported. An on-time stored bar feeds only a source declared with the same timeframe and becomes visible when its bucket closes. Execution differs from ticks: a bar run fills waiting orders at each bar's open and settles stops, targets, and pending orders against the bar's range with each side meeting its adverse extreme first, which cannot reproduce the real intrabar order.

## Windows

A run is evaluated over paired windows:

```rust
WindowPlan::Fixed { in_sample, out_of_sample }
WindowPlan::RollingWalkForward { start, end, train, test, step }
```

Research windows, including service searches, are half-open: events at `to` belong to the following window. Stored-bar `from` and `to` must align to every candidate's shortest execution timeframe and offset; malformed bounds fail before replay or retained-job admission and are never rounded or split. This differs from the legacy configured/portfolio service endpoint, whose inclusive storage cursor may select the full bar beginning at an aligned `to`; identical timestamp strings therefore do not select equivalent data across those endpoint contracts. Fixed in-sample and out-of-sample labels must be distinct. A rolling split is counted with checked timestamp arithmetic and emitted only when its complete test span fits inside the declared range.

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
let experiment = batch.experiment_recipe();
let candidates = batch.candidate_recipes();
let runs = batch.run_recipes();
```

The table and pooled evaluation are projections of the same retained runs. Each normalized position ID is prefixed with a deterministic run ordinal before pooling, so IDs remain unique across symbols, windows, parameter points, and worker schedules. A batch stores one experiment recipe, one recipe per candidate, and one lightweight recipe per run. `run_batch_with_experiment` accepts an optional validated experiment ID plus caller revision and dataset reference without changing `ResearchPlan`; the compatibility APIs intentionally use no persistent experiment ID. Candidate recipes retain typed parameters, the exact bound document or registered factory, effective series bindings by symbol, per-instance bindings for heterogeneous portfolios, and typed calendar/quote projector configurations including dataset identity and bounded query policy. The experiment recipe snapshots replay, FutureQuote, evaluation, retention, profile, decision-latency, ordered-symbol, portfolio-policy, sizing, and cost settings through their owning typed values. Run recipes refer to a candidate ordinal and record half-open bounds, data mode, successful replay input coverage, actual single-instance first-ready time, and forced closes. Recipes reference caller-owned data and code; they do not prove a mutable dataset or revision stayed available. Selected-candidate support includes bounded trace records with omission counts, complete typed cache equality keys, deterministic checkpoint encoding and dependency validation, protected search/validation/final access with explicit release, post-test-tuning disclosure, and symbol/period/regime concentration with explicit uncertainty status. A checkpoint separates the candidate-generation frontier from completed run ordinals and retains committed rows, positions, run recipes, candidate recipes, the frozen selection, and split-access records. `ResearchBatch::merge_checkpoint` recombines those outcomes with newly executed runs and rejects duplicate run or position identity.

Rows are ordered by family, symbol, parameter labels, and window, never by a metric. Individual compilation, binding, or replay failures become failed rows while other runs continue. Invalid plans, documents, spaces, labels, or input ordering fail before the batch starts.

Run tags carry parameter values, symbol, window, and data mode onto every completed position. Pooled evaluation can therefore produce one bucket per parameter value across the complete search rather than the single bucket available inside one run.

## Structural search and direct factories

`StructuralSearchSpec` enumerates a finite declared atom/operator universe in stable breadth-first order and recomposes prior levels through the requested depth. Node, candidate, and retained-frontier byte limits are checked before recursive child clones and expansion allocation; budget exhaustion is reported as incomplete, never exhaustive success. Commutative ordering is canonical, sequence remains directional, exact close-price aliases share one canonical material, and matching same-material threshold implications are reduced only when strict/equality and floating-point behavior are preserved. Generated documents use ordinary replay economics and carry deterministic candidate ordinals; there is no automatic score or winner.

Configured strategy documents, typed parameter spaces, and bounded structural documents are the normal authoring and remote execution path; changing supported rules, parameters, calendars, or session selections does not require registering a strategy name in server Rust. `DirectResearchFactory` is the optional caller-compiled extension path for `HistoricalStrategy` implementations. It creates fresh state for every candidate/window and records an exact factory name/revision. The server's shipped `noop_direct@r1` entry verifies this trusted mechanism but does not trade and is not a production strategy catalog; arbitrary remote code and plugin upload are prohibited. Configured/direct mixed portfolios retain their distinct pre-/post-settlement callback timing.

## Time, setup, calendar, and enhanced quote facts

Source facts are supplied through `SourceBarFactProjector`: source ordinal, scheduled open/close, actual `available_at`, and explicit gap-before facts remain distinct. Confirmed swings retain anchor and confirmation times through `ConfirmedSwingFactProjector`. Temporal materials include strict cross, delta/rise/fall, hold/count/share/streak, bars-since, sequence, and the `single_keep_first`, `replace_latest`, and bounded queue setup policies. Setup capture, tolerance, normalization and level are frozen; creation-bar retest is impossible; expiry and opposite-close invalidation use the documented strict boundaries.

The compatibility `IanaTradingCalendar` keeps its original one-window behavior. New document-driven calendar inputs use `ConfiguredTradingCalendar`: an omitted session schedule expands to exactly one `full_day` session spanning adjacent resolved trading-day boundaries, while explicit custom mode uses only its bounded named sessions. Full day is 24 hours in UTC and the actual 23/25-hour local day across DST. Trading-day/week aggregates are independent from analytical sessions, so overlapping named sessions never double-count or redefine daily values. Previous-session facts target the most recent completed occurrence of the selected session ID; previous-day/week facts require the immediately preceding eligible period and never fall back to older observed data. Exact previous/final values require an explicit market schedule and complete aligned, actually revealed children; an unspecified schedule cannot prove completeness. Sessions are analytical inputs only and do not themselves permit or prohibit trading.

A declared space can bind a strategy input to the default full-day session without a hardcoded strategy factory:

```toml
[historical_inputs.calendars.main]
id = "main"
timezone = "UTC"
market = { mode = "continuous" }
# sessions omitted: exactly one effective full_day session

[[historical_inputs.inputs]]
name = "previous_session_high"
source = "primary"
calendar = "main"

[historical_inputs.inputs.input]
calendar_id = "main"
kind = "previous_session_high"
child_seconds = 60
alignment_offset_seconds = 0
maximum_history = 2
```

Use `sessions = { mode = "custom", items = [...] }` to replace the default with named sessions. In custom mode, every session-dependent input names its session explicitly; an empty custom list, unknown ID, duplicate input name, invalid local time, incompatible bar boundary, or oversized calendar state rejects before replay.

Calendar configuration is carried through configured single/portfolio requests and through a declared search space's `historical_inputs`. The service requires finite `from`/`to` bounds when configured historical inputs are selected, loads their bounded history, and gates strategy transitions until the evaluation start while still warming indicators and calendar state. Effective expanded defaults are retained in candidate recipes; selected configured reruns reconstruct supported calendar projectors before choosing the load range. Unknown projector kinds or projectors whose external data authority cannot be restored reject explicitly.

Enhanced quote storage persists source ordinals and preserves equal timestamps. Provider sequence is used as identity only with a source identity; repeated equal payloads count as duplicates and conflicting payloads reject. `QuoteStatisticsProjector` performs a row-bounded exact query over one immutable ordered-tick slice and exposes accepted/rejected/coverage counts, spread statistics, activity/interarrival/path/extrema/TWAP, captured-level crossings and distinct breakout durations, plus half-open time-at-price bins as typed named inputs. These are quote statistics, not trade volume, order flow, or VWAP. The legacy CSV importer reports observed fractional precision and simultaneous rows but has no provider sequence or persisted ordinal and writes through timestamp-deduplicated legacy paths, so those partitions are never relabeled as complete exact quote paths.

## Portfolio plans

`ResearchPlan::with_portfolio` replays every plan symbol together, one instance per symbol against one account, instead of one run per symbol. Each run then covers one parameter point over one window, the row's symbol names every symbol joined with `+`, each position carries an `instance` tag naming the symbol that traded it, and each instance reads from its own derived warmup start. A `PortfolioPlan` with policies builds a fresh supervisor for every run, and its rows report `rejected_entries` and `halt_minutes`, a halt still in force counting to the window's end; the CSV adds those two columns only when a run was supervised. All symbols must be ticks or all stored bars, `instance` joins the reserved tag names, and a group risk cap requires a monetary sizing policy.

## Service resume, portfolio candidates, and protected reruns

A service search may provide `portfolio_candidates` to evaluate distinct configured documents and geometries or mixed configured/server-registered direct instances against one account per candidate. `direct_factory` selects an immutable trusted catalog entry by exact name/revision and bounded typed parameters while requiring null configured documents; unknown revisions and ambiguous combinations reject before loading. Artifacts retain `series_by_instance`, globally unique candidate/run/position identities, and the effective portfolio snapshot. A downloaded artifact checkpoint may be supplied as `resume_checkpoint`; completed run ordinals are skipped, committed outcomes are retained, and dependency or recipe changes reject before replay. Cancellation after complete configured or direct runs publishes only a separately labeled checkpoint artifact, never a completed search artifact; incomplete runs restart from fresh state. `selected_rerun` names one downloaded experiment/candidate/run recipe and an evaluation role. Final access requires a frozen selection, a declared future horizon with sufficient embargo, and either `release_final = true` or a checkpoint that already records the release. The returned checkpoint records release, access, rerun, and post-test tuning. Selected reruns also return bounded linked trace output and explicit concentration/uncertainty evidence. These controls audit actions made through this application; they cannot prove that external data or code stayed unchanged or that a user did not inspect data elsewhere.

## Examples

```bash
cargo run -p qs-research --example ema_grid
cargo run -p qs-research --example unified_workflows -- all
```

`ema_grid` writes deterministic synthetic ticks into a temporary Parquet store, loads them through `qs-market-loader`, reads the two TOML documents, runs the declared space with commission charged, writes `target/research/ema_grid.csv`, and prints pooled breakdown counts by entry condition and window.

`unified_workflows` is a compact executable acceptance fixture. Its modes are `bar-inputs`, `direct`, `variants`, `mixed`, `temporal`, `cache`, and `resume-final`; `all` runs every mode. Its cache mode admits an immutable market view, proves miss/hit reuse, and consumes the cached midpoint as a real configured Entry condition with nonempty economic output. The remaining modes cover counted and unknown-count bars, optional direct factories, variants, mixed timing, generated capture, cancellation resume, and protected final reruns. All data is synthetic and carries no profitability claim.
