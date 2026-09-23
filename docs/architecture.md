# Architecture

The workspace separates synchronous trading logic, historical replay, storage, logical service contracts, transport providers, source parsing, and venue-specific integrations.

## Layer map

```text
                           operator configuration
                                    |
                 +------------------+------------------+
                 |                                     |
                 v                                     v
        Backtest application                  Market-data application
        qs-backtest-server                    qs-market-data
                 |                                     |
                 v                                     v
        qs-backtest-api                       qs-market-data-api
                 +------------------+------------------+
                                    |
                                    v
                              qs-service
                       endpoint and failure policy
                                    |
                                    v
                           qs-service-xrpc
                    Channel / SHM / Unix / TCP

Domain and data:
qs-symbols -----> qs-instruments -----> quant-system-core
 compatibility      exact identity,           |
 facade             specs, catalogs           +----> qs-strategy
                                              |      configured core only
                                              v
                                      qs-backtest -> qs-backtest-server
                                           ^                  ^
                                           |                  |
                              qs-data-preprocess -> qs-market-loader -> qs-research
                                    storage           stored data as      parameter
                                                      replay input        family search

qs-signal-parser -> strict RawSignal compatibility boundary
```

## Ownership boundaries

### Trading domain

`qs-instruments` owns source-neutral `AssetId`, broker- or exchange-qualified `InstrumentId`, exact decimal and grid rules, effective-dated instrument specifications, economics descriptors, immutable catalog snapshots, and stored-series or platform bindings. It has no storage, async runtime, parser, strategy, service, or venue SDK dependency.

An instrument listing venue, trading platform, execution venue, and market-data source are distinct identities. For example, an IC Markets listing may be exposed through CTrader and executed on a particular broker account or server while historical quotes come from a separate Parquet source. CTrader is therefore a `TradingPlatformId`, not the `ListingVenueId` in the instrument identity.

`quant-system-core` owns the synchronous engine, strict `RawSignal`, position management, pure profile resolution, sizing, currency conversion, and commission and swap arithmetic. Its catalog-aware sizing path uses explicit quantity rules and contract multipliers. It performs no networking, storage, configuration IO, parsing, state lookup, or broker calls.

`qs-strategy` owns the reusable synchronous configured strategy core. It validates typed parameter templates against explicit immutable material-factory schemas, substitutes one complete binding, prunes unused material branches, and compiles the resulting strict unversioned configuration against bounded logical source IDs. It derives compositional source lookback and named-input requirements, evaluates ordered source updates, total trade-slot facts, bounded typed expressions, causal indicator primitives, and deterministic finite-state transitions, and emits generic decisions and notes plus ordered commands carrying strict `RawSignal` values with deterministic correlation IDs. An Entry may declare an optional entry class, and the compiled requirements list every slot and class an Entry can emit together with the slots whose stop the strategy moves itself, so an adapter can check profile routing before a run. An open trade slot also carries its entry fill time, favorable and adverse campaign excursion, and initial risk, which an adapter must supply, and calendar materials derive the weekday and seconds of day from the input time alone. Command feedback rejects unknown, mismatched, duplicate, or replayed events, retains successful correlations until the action-specific committed fact and terminal disposition are both observed, and supports an adapter-owned final feedback boundary without evaluating another market transition. The crate is library-only and does not own logical-source binding, historical history or feeds, FutureQuote adaptation, service or RPC execution, live runtime orchestration, persistence, or management-profile composition.

`qs-symbols` remains the compatibility facade for current canonical symbols, aliases, precision, lot, and currency metadata. Supported FX, metal, commodity, and index rows can be translated through the guarded compatibility economics result into an immutable instrument snapshot. Registry-backed cryptocurrency and unknown rows are not promoted into executable instruments.

### Historical replay

`qs-backtest` owns historical scheduling, deterministic FutureQuote execution, accounting including when commission and swap are charged, metrics, reports, profile-file loading, validated historical strategy contracts, bounded causal fixed-duration closed-bar series, complete-boundary causal observations and annotations, and stateful callback replay with read-only context, causal generated-signal scheduling, warmup enforcement, committed execution feedback, bounded non-economic journal output, research-only annotation results, and explicit completed-result comparison. It also owns the historical adapter for `qs-strategy`: complete logical-source-to-series binding fed by ticks or by stored bars of the declared timeframe, exact tick-count volume and named-input projection, total immutable trade-slot projection, opaque configured command-ID preservation, ordered effect/disposition feedback, final committed-feedback processing, generic decision and note mapping, management-profile routing preflight, and reuse of the existing Entry profile selection and FutureQuote economics. The existing action-producing `Strategy` mode and strict predefined-signal replay remain available and use their established behavior.

`qs-backtest-server` composes storage, an explicit instrument catalog or guarded symbol compatibility snapshot, profiles, retained jobs, artifacts, and the logical backtest API into an operator-facing service and CLI. Its shipped CLI and example use provider-neutral retained-job, artifact, synchronous-execution, and discovery capabilities through the typed xrpc facade rather than owning raw RPC clients or method names. It also runs configured strategy documents and parameter searches received as JSON values: it decodes and compiles each document strictly with the built-in material library before admitting a job, loads a configured run's single symbol from the warmup start its compiled strategy derives, and runs a search through `qs-research` behind a gate that admits one search at a time, returning the search table, pooled evaluation, and bound documents as one artifact. The server resolves and pins active and conversion instruments before replay, rejects specification changes across a requested range, and records the physical Parquet coordinates as stored-series bindings.

### Data storage

`qs-data-preprocess` owns supported tick/bar import, storage, and tick-to-bar resampling. Its bucket arithmetic and quote-acceptance rule are shared with the replay engine's in-memory bar series, so a stored bar equals the bar a tick-driven replay derives from the same input. Parquet is the default backend. Backtest replay reads bounded chronological cursors instead of requiring complete datasets in memory.

`qs-research` owns declared parameter-space search over replay. It loads an explicit TOML strategy template and TOML space document, enumerates typed values in declaration order, applies parameter-only constraints, binds parameterized series geometry, and derives warmup from the compiled strategy requirements. A batch retains every run's normalized outcomes and bound document, exposing a deterministic comparison table and pooled provider evaluation with run-scoped position identity. Caller-written Rust families remain an escape hatch. The crate produces no score, rank, or rating and owns no execution, accounting, or storage of its own. A batch runs over ticks or over stored bars and records which one each row used as its data mode.

`qs-market-loader` owns the bridge from stored market data to replay input. It reopens Parquet tick and bar cursors as ordered market events, applies stored bar spreads in price units, merges series deterministically, and validates that the coordinates it will read match the instrument manifest pinned for the run. It exists as its own crate so that a service and an in-process parameter search open identical streams through identical code, and so that the replay crate keeps reading no market-data storage of its own.

### Service contracts and providers

`qs-service` owns only provider-neutral endpoint and transport-failure vocabulary. Logical APIs own typed DTOs, events, errors, and client ports. The backtest API separates retained-job and artifact consumption, finite synchronous execution, configured strategy runs and parameter searches, discovery, and profile administration into provider-neutral capabilities implemented by one optional xrpc facade. Strategy, template, and space documents cross the API as JSON values, so the API crate carries no strategy-domain dependency and the service remains the one strict decoder.

`qs-service-xrpc` owns the current runtime for in-process channels, shared memory, Unix sockets, and TCP. Provider-specific clients, handshakes, slots, codecs, and lifecycle handles do not enter domain APIs.

A future secure remote provider should implement the typed service boundary rather than adding transport concepts to trading-domain crates.

### Sources and venues

`qs-signal-parser` owns bounded source facts, stateless source-neutral routing and normalization, strict structured-signal decoding, durable source application, committed normalization lifecycle, checkpoints, and committed-batch outbox state. Telegram, strict JSONL, and authenticated webhook adapters translate provider input into source events. The runner composes the shipped local JSONL pipeline, SQLite state, committed-batch JSONL publication, causal replay, and optional provider bindings as library APIs. The append-only JSONL output is at-least-once. Deployment packages, restart-safe hosted application processing, non-local sinks, and committed-batch trading projection are not available. Parsing remains optional because direct strict `RawSignal` input is still supported.

`qs-market-data` owns the CTrader FIX quote connection and market-data service. It registers source disconnect callbacks, invalidates prior-session quotes during reconnect, retains one service observation timestamp per cached quote, timestamps source-state transitions, and exposes detected receiver lag or subscription rejection through combined-stream data-quality events. It does not own live order execution.

External source transports, internal service transports, market-data sources, and execution venues are different adapters and failure domains.

## Historical replay flow

```text
supported tick/bar export
  -> partitioned Parquet
  -> replay request validation
  -> explicit instrument resolution and economic-capability preflight
  -> catalog, specification, and physical-series binding manifest
  -> bounded chronological cursors
  -> deterministic timestamp batches
  -> FutureQuote execution with explicit multiplier and quantity rules
  -> accounting, metrics, and lifecycle artifacts
  -> inline result or verified artifact
```

Unsupported compatibility economics fail before data access. An explicit catalog may declare additional model identifiers, but replay accepts only code-supported FX/CFD model and quantity-unit combinations. Output bounds control returned data volume but must not change economic results.

## Signal flow

```text
Telegram batch row --------> TelegramBatchSourceAdapter ---+
                                                           |
Telegram relay delivery ---> TelegramRelaySourceAdapter ---+-> source event
                                                           |
SourceEvent JSONL ----------> strict JSONL codec -----------+
                                                           |
signed webhook request ----> authenticated provider edge
  -> admission reference
  -> in-process runner
  -> durable preflight and reservation
  -> stateless route
  -> selected-pipeline snapshot when required
  -> optional decoder/parser and shared validation
  -> durable compare-and-commit
  -> committed normalization batch, lifecycle facts, checkpoint, and outbox state

manual/API RawSignal, direct historical strategy output, or configured adapter output
  -> Entry profile selection by entry class or run default, resolution, and sizing
  -> deterministic replay
```

All source adapters terminate before runner ownership. The webhook edge authenticates and binds requests to one source before admission. A hosted `202 Accepted` response returns before source application completes. The runner owns source application and committed-batch publication; adapters do not own normalization state or sink publication. Source deletes commit lifecycle withdrawal only and do not create trading actions.

The existing `OfflineRunner`, optional `OnlineServer`, handler callbacks, and standalone JSONL contracts remain unchanged compatibility facades and are not hosted through durable state.

A committed normalization batch is an authoritative ingestion result, but it does not enter replay automatically. Source edits, deletes, supersession, and withdrawal remain audit and lifecycle facts. A source event is not a trade, and a parsed signal is not an engine action until the replay boundary validates and resolves an explicitly supplied `RawSignal`. Strategy decisions, sizing, execution scheduling, and accounting remain explicit downstream responsibilities.

## Market-data flow

```text
CTrader FIX -> disconnect callbacks and reconnect -> timestamped source state
                                                       |
                                                       v
                                  observed quote cache -> prices and alerts
                                                       |
                                                       v
                              combined price/state/data-quality consumers
```

Local transport health, CTrader source state, and quote freshness are separate. Price timestamps are service callback-observation times, state timestamps are service transition times, reconnect invalidates prior-session cache entries, and detected stream gaps are visible without claiming replay or upstream source timestamps.

## Security boundary

Shared memory and Unix sockets are intended for trusted same-host deployments. TCP has no built-in authentication or TLS and defaults to loopback-only access. Cross-host deployment requires an explicitly trusted network boundary or a future secure provider.
