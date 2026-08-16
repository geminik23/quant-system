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
 facade             specs, catalogs           v
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

`qs-symbols` remains the compatibility facade for current canonical symbols, aliases, precision, lot, and currency metadata. Supported FX, metal, commodity, and index rows can be translated through the guarded compatibility economics result into an immutable instrument snapshot. Registry-backed cryptocurrency and unknown rows are not promoted into executable instruments.

### Historical replay

`qs-backtest` owns historical scheduling, deterministic FutureQuote execution, accounting, metrics, reports, profile-file loading, validated historical strategy contracts, bounded causal fixed-duration closed-bar series, complete-boundary causal observations and annotations, and a stateful callback contract with read-only context and borrowed execution facts. The existing action-producing `Strategy` mode and strict predefined-signal replay remain available; dynamic strategy execution through FutureQuote is not implemented yet.

`qs-backtest-server` composes storage, an explicit instrument catalog or guarded symbol compatibility snapshot, profiles, retained jobs, artifacts, and the logical backtest API into an operator-facing service and CLI. It resolves and pins active and conversion instruments before replay, rejects specification changes across a requested range, and records the physical Parquet coordinates as stored-series bindings.

### Data storage

`qs-data-preprocess` owns supported tick/bar import and storage. Parquet is the default backend. Backtest replay reads bounded chronological cursors instead of requiring complete datasets in memory.

### Service contracts and providers

`qs-service` owns only provider-neutral endpoint and transport-failure vocabulary. Logical APIs own typed DTOs, events, errors, and client ports.

`qs-service-xrpc` owns the current runtime for in-process channels, shared memory, Unix sockets, and TCP. Provider-specific clients, handshakes, slots, codecs, and lifecycle handles do not enter domain APIs.

A future secure remote provider should implement the typed service boundary rather than adding transport concepts to trading-domain crates.

### Sources and venues

`qs-signal-parser` owns bounded source facts, stateless source-neutral routing and normalization, strict structured-signal decoding, durable source application, committed normalization lifecycle, checkpoints, and committed-batch outbox state. Telegram, strict JSONL, and authenticated webhook adapters translate provider input into source events. The runner composes the shipped local JSONL pipeline, SQLite state, committed-batch JSONL publication, causal replay, and optional provider bindings as library APIs. The append-only JSONL output is at-least-once. Deployment packages, restart-safe hosted application processing, non-local sinks, and committed-batch trading projection are not available. Parsing remains optional because direct strict `RawSignal` input is still supported.

`qs-market-data` owns the CTrader FIX quote connection and market-data service. It does not own live order execution.

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

manual/API RawSignal or historical strategy output
  -> profile and sizing
  -> deterministic replay
```

All source adapters terminate before runner ownership. The webhook edge authenticates and binds requests to one source before admission. A hosted `202 Accepted` response returns before source application completes. The runner owns source application and committed-batch publication; adapters do not own normalization state or sink publication. Source deletes commit lifecycle withdrawal only and do not create trading actions.

The existing `OfflineRunner`, optional `OnlineServer`, handler callbacks, and standalone JSONL contracts remain unchanged compatibility facades and are not hosted through durable state.

A committed normalization batch is an authoritative ingestion result, but it does not enter replay automatically. Source edits, deletes, supersession, and withdrawal remain audit and lifecycle facts. A source event is not a trade, and a parsed signal is not an engine action until the replay boundary validates and resolves an explicitly supplied `RawSignal`. Strategy decisions, sizing, execution scheduling, and accounting remain explicit downstream responsibilities.

## Market-data flow

```text
CTrader FIX -> reconnect and source state -> price cache and alerts
                                              |
                                              v
                                     MarketDataClient consumers
```

Transport health and upstream data freshness are observed separately.

## Security boundary

Shared memory and Unix sockets are intended for trusted same-host deployments. TCP has no built-in authentication or TLS and defaults to loopback-only access. Cross-host deployment requires an explicitly trusted network boundary or a future secure provider.
