# Architecture

The workspace separates synchronous trading logic, historical replay, storage, logical service contracts, transport providers, source parsing, and venue-specific integrations.

## Layer map

```text
                           operator configuration
                                    |
                 +------------------+------------------+
                 |                                     |
                 v                                     v
        qs-backtest-server                       qs-market-data
                 |                                     |
                 v                                     v
        qs-backtest-api                       qs-market-data-api
                 ^                                     |
                 |                                     |
        qs-backtest-client                             |
        connection/catalog utilities                   |
                 |                                     |
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
                                           ^
                                           |
                              qs-data-preprocess

qs-signal-parser -> strict RawSignal compatibility boundary
```

## Ownership boundaries

### Trading domain

`qs-instruments` owns source-neutral `AssetId`, broker- or exchange-qualified `InstrumentId`, exact decimal and grid rules, effective-dated instrument specifications, economics descriptors, immutable catalog snapshots, and stored-series or platform bindings. It has no storage, async runtime, parser, strategy, service, or venue SDK dependency.

An instrument listing venue, trading platform, execution venue, and market-data source are distinct identities. For example, an IC Markets listing may be exposed through CTrader and executed on a particular broker account or server while historical quotes come from a separate Parquet source. CTrader is therefore a `TradingPlatformId`, not the `ListingVenueId` in the instrument identity.

`quant-system-core` owns the synchronous engine, strict `RawSignal`, position management, pure profile resolution, sizing, and currency conversion. Its catalog-aware sizing path uses explicit quantity rules and contract multipliers. It performs no networking, storage, configuration IO, parsing, state lookup, or broker calls.

`qs-strategy` owns the reusable synchronous configured strategy core. It compiles recursively strict unversioned configuration against bounded logical source IDs and an explicit immutable material library, derives source-specific lookback and named-input requirements, evaluates ordered source updates, total trade-slot facts, bounded typed expressions, and causal materials, commits deterministic finite-state transitions atomically, and emits generic decisions and notes plus ordered commands carrying strict `RawSignal` values with deterministic correlation IDs. Command feedback rejects unknown, mismatched, duplicate, or replayed events, retains successful correlations until the action-specific committed fact and terminal disposition are both observed, and supports an adapter-owned final feedback boundary without evaluating another market transition. The crate is library-only and does not own logical-source binding, historical history or feeds, FutureQuote adaptation, service or RPC execution, live runtime orchestration, persistence, or management-profile composition.

`qs-symbols` remains the compatibility facade for current canonical symbols, aliases, precision, lot, and currency metadata. Supported FX, metal, commodity, and index rows can be translated through the guarded compatibility economics result into an immutable instrument snapshot. Registry-backed cryptocurrency and unknown rows are not promoted into executable instruments.

### Historical replay

`qs-backtest` owns historical scheduling, deterministic FutureQuote execution, accounting, metrics, reports, profile-file loading, validated historical strategy contracts, bounded causal fixed-duration closed-bar series, complete-boundary causal observations and annotations, and stateful callback replay with read-only context, causal generated-signal scheduling, warmup enforcement, committed execution feedback, bounded non-economic journal output, research-only annotation results, and explicit completed-result comparison. It also owns the historical adapter for `qs-strategy`: complete logical-source-to-series binding, exact tick-count volume and named-input projection, total immutable trade-slot projection, opaque configured command-ID preservation, ordered effect/disposition feedback, final committed-feedback processing, generic decision and note mapping, management-profile rejection, and reuse of existing unprofiled Entry and FutureQuote economics. The existing action-producing `Strategy` mode and strict predefined-signal replay remain available and use their established behavior.

`qs-backtest-server` composes storage, an explicit instrument catalog or guarded symbol compatibility snapshot, profiles, retained jobs, artifacts, and the logical backtest API into an operator-facing service and CLI. Its shipped CLI and example use provider-neutral retained-job, artifact, synchronous-execution, and discovery capabilities through the typed xrpc facade rather than owning raw RPC clients or method names. The server resolves and pins active and conversion instruments before replay, rejects specification changes across a requested range, and records the physical Parquet coordinates as stored-series bindings.

### Data storage

`qs-data-preprocess` owns supported tick/bar import and storage. Parquet is the default backend. Backtest replay reads bounded chronological cursors instead of requiring complete datasets in memory.

### Service contracts and providers

`qs-service` owns only provider-neutral endpoint and transport-failure vocabulary. Logical APIs own typed DTOs, events, errors, and client ports. The backtest API separates retained-job and artifact consumption, finite synchronous execution, discovery, and profile administration into provider-neutral capabilities implemented by one optional xrpc facade.

`qs-backtest-client` currently owns provider-neutral desktop endpoint validation and a managed connection/catalog probe that performs ping, profile discovery, symbol availability discovery, and explicit close. Its default normal dependency graph remains xrpc-free; the optional xrpc connector is isolated under its provider module. Retained submit/watch/reconnect, input preparation, output delivery, and analysis workflows are not implemented by this crate yet.

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
  -> profile or configured unprofiled Entry resolution and sizing
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
