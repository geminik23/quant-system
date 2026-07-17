# quant-system

A modular Rust workspace for algorithmic trading: historical data, deterministic backtesting, structured signal ingestion, real-time market data, and shared-memory RPC services.

## Crates

| Crate | Description |
|---|---|
| [`qs-core`](crates/core/) | Pure synchronous trade engine with composable stoploss, trailing-stop, take-profit, breakeven, and time-exit rules. FutureQuote uses fill-bearing effects, absolute size accounting, remaining-inventory cost basis, stop provenance, and in-place rollback transactions. |
| [`qs-backtest`](crates/backtest/) | Strategy and raw-signal replay over ticks or close-only synthetic bar quotes. Production replay uses deterministic `FutureQuoteV1`, targeted rollback, exact online account metrics, bounded MTM output, account-currency accounting, evaluation, cancellation, and progress. Generic in-memory strategy APIs remain available for library simulations. |
| [`qs-market-data`](crates/market-data/) | Real-time bid/ask distribution over shared-memory IPC, with per-client subscriptions, one-shot alerts, and reconnection. |
| [`qs-data-preprocess`](crates/data-preprocess/) | Historical tick/OHLCV import and query CLI. Parquet/Polars is the default backend and provides bounded ascending cursors with cooperative cancellation; DuckDB remains optional behind `duckdb-backend`. |
| [`qs-symbols`](crates/symbols/) | TOML symbol registry for canonical names, aliases, pip/digit and lot metadata, and explicit P&L/base/quote currency metadata. |
| [`qs-backtest-server`](crates/backtest-server/) | Multi-client shared-memory RPC server. Strict recursive schema-2 sync, async, and multi-profile methods expose streaming `FutureQuoteV1`, typed evaluation, bounded jobs, progress, cancellation, and inline/artifact result delivery. |
| [`qs-signal-parser`](crates/signal-parser/) | Reusable structured-message parser framework with offline/online runners, per-message outcomes, deterministic source ordering, source identity, and reply-root resolution. |

## Execution and accounting

The server and `tg_backtest` use `BacktestRunner::new_future` and streaming `FutureQuoteV1` exclusively. `BacktestRunner::new` remains a generic library constructor for in-memory strategy simulations and is not exposed through RPC.

- both modes reject non-finite, non-positive, crossed, and per-symbol time-regressing quotes;
- FutureQuote consumes timestamp batches, records every conversion-role tick before processing any primary-role event at that timestamp, and lets one event carry both roles;
- only primary-role events drive signal execution, pending/rule settlement, and end-of-data time; conversion-only ticks update FX state and valuation without extending EOD;
- FutureQuote uses the first eligible matching-symbol primary quote and gives existing pending/rules exact-timestamp priority;
- one core `ExecutionFill` is carried by every fill-bearing `FutureEffect`; the executor validates it without repricing;
- in-place core mutations carry rollback checkpoints; accounting snapshots only affected position state and append lengths, and portfolio updates commit after successful accounting, so accumulated histories are not cloned per settlement;
- pending orders emit `Placed` and exactly one terminal `Filled`, `Cancelled`, or `UnfilledAtEnd` event;
- absolute entered/closed/open size and active remaining-inventory average cost are conserved;
- target/stop/rule mutations validate state and geometry and remain synchronized with the optional alert register.

A validated run currency plan freezes primary and conversion symbol roles, P&L currencies, routes, and strict-before warmup quotes. Historical FX ticks from the same exchange provide identity, direct, inverse, or deterministic two-leg routes. Signed conversion uses executable bid/ask sides, and all FutureQuote monetary accounting and report aggregates are in account currency; close, risk, mark, and exposure artifacts retain native amounts and conversion audits. During monotonic replay, old conversion history is pruned while retaining the latest causal predecessor needed by the next signal and current batch.

`ModifyTarget` is atomic and ratio-preserving, supports repeated updates, and is available through core, raw signals, and RPC.

## FutureQuote replay and output bounds

V2 server planning filters signals first, derives active primary symbols only from retained Entry signals, and records explicitly requested symbols with no retained Entry as idle metadata. Data loading starts at the earliest retained effective signal. Management-only input on the empty engine produces an idle result without loading market data.

Active primary and required conversion series are opened as bounded ascending Parquet cursors. A deterministic lookahead k-way merge emits complete batches ordered by `(timestamp, series_rank, row_sequence)`; replay still records all conversion-role ticks before primary work at a shared timestamp. After a complete batch, streaming replay stops only when scheduled signals, queued actions, open positions, and pending orders are all empty. Multi-profile V2 runs reopen the immutable stream description for each profile rather than cloning a materialized feed.

Exact online equity, drawdown, and campaign MAE/MFE are maintained independently from returned MTM points. `MtmOutputPolicy` supports `none`, deterministic `bounded`, and `full`; the default is bounded to 4096 points, with valid bounds from 8 through 16384. `none` returns no curve and `full` retains every observation, while both preserve the same exact online metrics. Output summaries report observed, retained, and omitted point counts.

Request and artifact schema values remain `2`. V2 result delivery supports `auto`, `inline`, and `artifact`: `auto` uses inline delivery up to the configured 12 MiB default and otherwise stores complete JSON as an artifact; `inline` rejects oversized JSON; `artifact` always stores it. Artifact references include byte length, SHA-256, and chunk size. The client retrieves ordered base64 chunks, verifies offsets, length, and checksum, writes through a temporary file before rename when an output is requested, and requests cleanup after a successful download.

## In-place Entry and sizing contract

Wire `Entry` objects require a finite positive `risk` and do not accept an Entry `size`. Internally it is retained as `risk_multiplier` until sizing. Core `Action::Open.size` has not changed: it is the final lot quantity passed to the engine.

Exactly one sizing policy resolves each Entry:

- `FixedLot { lots }` scales configured lots and may open without a stop;
- `FixedRiskAmount { amount }` risks a fixed account-currency amount and requires a protective stop;
- `BalanceRiskPercent { percent }` risks a percentage of realized balance immediately before entry and requires a protective stop.

The Entry multiplier is applied exactly once to the policy basis before lot constraints. Authoritative integer lot steps are then floored to the symbol step, checked against the minimum, capped at the optional maximum, and converted back to final lots. Target weights are allocated only after those steps are final. Without a profile, FutureQuote retains every target with equal `1/N` weights; two targets therefore receive `0.5/0.5`, and any selected target that receives zero lot steps is rejected.

For a FutureQuote market Entry, the actual execution price is established before final profile resolution and sizing. Pending Entries are profiled and sized when placed, and that final size remains frozen until fill or terminal cancellation.

Run `tg_backtest` with exactly one account sizing option:

```bash
cargo run -p qs-backtest-server --bin tg_backtest -- \
  --input <signals.jsonl> \
  --shm-name backtest \
  --symbols XAUUSD,GBPJPY \
  --exchange icmarkets \
  --data-type tick \
  --from 2026-03-08 \
  --to 2026-03-11 \
  --profile conservative \
  --balance 10000 \
  --account-currency USD \
  --risk-percent 1.0 \
  --output <result.json>
```

Use `--base-lot 0.02` to set the account's fixed lot basis, `--risk-per-trade 100` to risk a fixed account-currency amount, or `--risk-percent 1.0` to risk one percent of realized balance. These options are mutually exclusive. Each Entry's parsed `risk` then scales the selected basis, so `risk: 0.5` uses half of it.

## Signal outcomes and evaluation

Structured parsing returns deterministic per-source outcomes in original input order while timestamp-sorting successful signals for replay. Identity-aware parsing can resolve reply chains to the ultimate source root and returns structured failures for malformed or unresolved actions.

Typed source-coverage counts can be included in evaluation. Integrated V2 evaluation normalizes symbol filters, rejects unsupported tag filters and breakdowns because completed positions have no tags, and can include deterministically capped normalized position rows selected by the same filter. Empty signal input is an error for normal reports but can produce a valid zero-trade coverage report.

## Services and reproducibility

Async backtests cooperatively cancel at planning boundaries and during partition discovery, bounded Parquet reads, stream refill, timestamp batches, signals, queued actions, and primary-EOD liquidation. Status reports structured loading/conversion/replay progress with event, signal, and symbol counts. Jobs are bounded, terminal jobs and artifacts are periodically cleaned up, active jobs are cancelled on shutdown, and tracked blocking workers are awaited. Multi and multi-V2 handlers run in `spawn_blocking`; empty multi-profile requests fail explicitly.

FutureQuote server results record deterministic metadata for requested, active, and idle symbols, requested and effective loading bounds, timeframe, execution latency, account currency, the immutable run currency plan, profile and sizing identity/options, and active-symbol lot metadata. Bar runs declare `close_only_zero_spread` and no intrabar simulation.

## Validation

Use the workspace test and strict lint commands for release validation:

```text
cargo test --workspace --all-features --all-targets
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

The replay optimization is implemented; its final broad validation and completion summary are tracked separately.

## Current boundaries

Only these boundaries remain:

1. Several complex FutureQuote RPC artifacts still cross the wire as JSON `Value` objects rather than fully typed wire models.
2. Exact intrabar simulation is not implemented; bars are close-only zero-spread quotes.
3. Policy-sized `ScaleIn` is not implemented. Scale-in still carries an explicit final size, and FutureQuote rejects scale-in after a campaign has begun closing.
4. During cancellation, one low-level bounded Polars Parquet read may complete atomically before cancellation is observed.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
