# Current capabilities and boundaries

This reference describes the current source checkout's implemented capabilities, ownership limits, and operational caveats. It is not a promise of future delivery or a claim that every historical dataset or broker contract has been certified.

The published `0.4.1` packages do not include the subsequent instrument-admission and service cost-key corrections described by current source. See [Backtesting](backtesting.md) for executable replay contracts, [Architecture](architecture.md) for ownership, and [Roadmap](roadmap.md) for non-binding intended direction.

## Historical replay, data and economics

- Bars are replayed with the spread recorded while the bar formed, or with a configured per-symbol fallback; a bar that supplies neither executes at a zero spread and the run counts how often that happened. Waiting orders fill at a bar's open, stops, targets, and pending orders inside its range fill at their level with each side meeting its adverse extreme first, and the close only marks. The real intrabar order is unknown, so use ticks when the ordering of stop, target, and management events matters.
- FutureQuote Market Entry sizing can use the actual fill price or an explicit signal Entry price, with fill-price default and fallback. The option changes quantity calculation only; profile resolution, actual fills, P&L, MTM, and actual risk remain execution-price based, while pending order quantity remains fixed at placement.
- Direct RawSignal replay can map optional exact Entry classes to immutable per-run management-profile snapshots. Profiles can scale the directional signal-stop distance and generate targets from multiples of the final grid-adjusted stop distance; pending levels and quantity remain frozen at placement.
- Current source normalizes service cost keys to replay symbols and rejects duplicate canonical settings. The published `0.4.1` service has the earlier key mismatch; see [Trading costs](backtesting.md#trading-costs) when using cost-enabled replay.
- Direct signal replay skips and reports unavailable selected instruments by default; `--on-unavailable error` requests strict failure. Existing aliases connect canonical names to stored datasets without renaming them. Configurable lot-linear BTCUSD/ETHUSD simulation specifications are available through the server's `linear_instruments` settings; they are operator assumptions, not certified broker contracts. Bare registry crypto rows still have no executable compatibility economics. General spot/inventory, inverse/perpetual, funding, margin, and liquidation models remain unsupported.
- Historical import accepts the documented MetaTrader-style tab-delimited tick and bar formats, not arbitrary CSV layouts.

Further references: [Replay semantics](backtesting.md#replay-semantics), [Trading costs](backtesting.md#trading-costs), [Instrument identity and economics](backtesting.md#instrument-identity-and-economics), and the [import/storage guide](../crates/data-preprocess/GUIDE.md).

## Configured strategies, calendars and research

- `qs-strategy` provides a reusable synchronous configured strategy core with typed parameter templates, factory-declared material schemas, branch pruning, bounded logical bar sources, derived source-specific input requirements, a named causal numeric catalog with semantic units and strict validity, source-clocked temporal/setup materials, typed bounded expressions, deterministic material and finite-state evaluation, total vacant/pending/open trade-slot facts including open-position time, excursion, and initial risk, calendar materials, generic decisions and notes, and validated command-correlated strict `RawSignal` values, where an Entry may carry an optional entry class for adapter-owned profile routing. It remains library-only and owns no historical feeds, services, live runtime, persistence, or management-profile composition.
- `qs-backtest` provides validated historical strategy contracts and a configured-strategy adapter over the existing FutureQuote scheduler. The adapter performs complete logical-source binding from ticks or stored bars, named-input projection, total trade-slot projection, ordered command provenance and feedback, and profile selection shared with raw-signal replay. Document-driven historical calendar inputs default to one `full_day` analytical session for an explicitly selected timezone and support explicitly configured named sessions, independent day/week aggregates, actual reveal times, bounded history and strict coverage. Calendar sessions do not automatically restrict Entries, close positions, or change swap, sizing, or risk-reset behavior.
- Configured historical execution is available through materialized and streaming in-process library APIs. Current conformance verifies neutral no-op, EMA crossover, EMA/ATR lifecycle, declared parameter binding, parameterized custom materials, compositional rolling indicators, pending cancellation, direct-signal economic parity, aligned-EOD materialized/streaming parity, final feedback handling, and strict research-output deserialization.
- `qs-backtest` replays several configured strategy instances against one account, and `qs-risk` supplies a synchronous portfolio supervisor that approves or rejects new exposure under position-count limits, correlation-group risk caps, a daily loss halt, and a drawdown kill switch, without ever blocking risk reduction.
- The backtest service accepts strict `RawSignal` runs, configured strategy documents, portfolios, and parameter or bounded structural searches as retained jobs. Supported strategy rules, parameters, calendars, and sessions are supplied as documents rather than registered strategy IDs. The optional trusted direct-Rust path remains server-compiled code selected by exact name/revision; its shipped no-op entry is conformance evidence, not a production strategy catalog, and arbitrary code/plugin upload remains prohibited. Compatible checkpoints retain complete runs, and protected selected reruns authorize final access before the service opens the selected market view.

Further references: [Configured strategies through the service](backtesting.md#configured-strategies-through-the-service), [Portfolios](backtesting.md#portfolios-of-configured-strategies), and [Parameter search](research.md).

## Execution preparation versus live trading

- `qs-execution` is a runtime-neutral library boundary that prepares concrete Market, Limit, Stop, full-close, original-entered-size ratio-close, pending-cancel, and stop-modification requests from strict `RawSignal` intent and caller-supplied current facts. It provides explicit automatic or entry-approval gating, separates accepted submission from committed reports, and projects validated fills, cancellation, modification, rejection, and failure observations into existing configured command feedback. Its local scripted example requires no credentials or network. It owns no broker adapter, connection, scheduler, persistence, P&L, or recovery, and preparation-time quantity is not silently resized when a later fill price differs.
- Actual live order submission, a live strategy scheduler, restart-safe strategy state, account reconciliation, and broker order adapters are not included. The broker-neutral `qs-execution` contract does not claim those operational capabilities.

Further reference: [Execution ownership](architecture.md#trading-domain).

## Source ingestion

- Source-neutral ingestion is available as embeddable library APIs for JSONL, Telegram, and authenticated webhook sources. A webhook `202 Accepted` response confirms admission only; it does not confirm normalization, committed-batch publication, or trading activity. Hosted application processing is not restart-safe, and the committed-batch trading bridge is not implemented.

Further reference: [Signal ingestion](signal-ingestion.md).

## Service, market-data and security boundaries

- Shipped backtest clients use provider-neutral retained-job, artifact, synchronous-execution, and discovery capabilities through the typed xrpc facade; RPC method names and provider error mapping remain inside the API provider module.
- Market-data snapshots and streams use service quote-observation timestamps rather than unavailable CTrader source timestamps. Reconnect invalidates prior-session quote cache entries, source-state events carry transition timestamps, and the combined event stream exposes detected receiver lag or subscription rejection without claiming replay or exactly-once delivery.
- Internal service TCP endpoints have no built-in authentication or TLS and are restricted to loopback by default.

Further references: [Service contracts](architecture.md#service-contracts-and-providers), [Security](architecture.md#security-boundary), and [Market data](market-data.md).
