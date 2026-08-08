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
qs-symbols -> quant-system-core -> qs-backtest -> qs-backtest-server
                    ^                  ^
                    |                  |
             qs-signal-parser   qs-data-preprocess
```

## Ownership boundaries

### Trading domain

`quant-system-core` owns the synchronous engine, normalized signals, position management, pure profile resolution, sizing, and currency conversion. It performs no networking, storage, configuration IO, parsing, or broker calls.

`qs-symbols` owns the current canonical symbol, alias, precision, lot, and currency metadata used by the compatibility economics path.

### Historical replay

`qs-backtest` owns historical scheduling, deterministic FutureQuote execution, accounting, metrics, reports, and profile-file loading. It supports in-process strategy and predefined-signal modes.

`qs-backtest-server` composes storage, symbols, profiles, retained jobs, artifacts, and the logical backtest API into an operator-facing service and CLI.

### Data storage

`qs-data-preprocess` owns supported tick/bar import and storage. Parquet is the default backend. Backtest replay reads bounded chronological cursors instead of requiring complete datasets in memory.

### Service contracts and providers

`qs-service` owns only provider-neutral endpoint and transport-failure vocabulary. Logical APIs own typed DTOs, events, errors, and client ports.

`qs-service-xrpc` owns the current runtime for in-process channels, shared memory, Unix sockets, and TCP. Provider-specific clients, handshakes, slots, codecs, and lifecycle handles do not enter domain APIs.

A future secure remote provider should implement the typed service boundary rather than adding transport concepts to trading-domain crates.

### Sources and venues

`qs-signal-parser` owns bounded source facts, stateless source-neutral routing and normalization, strict structured-signal decoding, backend-neutral durable source application, committed normalization lifecycle, checkpoints, and transactional committed-batch outbox state. `signal_parser::adapters::telegram` owns the public provider-specific boundary for separate batch and relay adaptation, exact opaque Telegram identity, strict bounded evidence, stable delivery identity, and an existing-`ChannelParser` compatibility producer that uses shared validation and durable context snapshots. The crate includes an in-memory conformance store and a single-process SQLite backend. Neutral runner hosting, a full deployment manifest, source providers, publication workers, external sink calls, and committed-batch trading projection remain separate future responsibilities. Parsing remains optional because direct strict `RawSignal` input is still supported.

`qs-market-data` owns the CTrader FIX quote connection and market-data service. It does not own live order execution.

External source transports, internal service transports, market-data sources, and execution venues are different adapters and failure domains.

## Historical replay flow

```text
supported tick/bar export
  -> partitioned Parquet
  -> replay request validation
  -> symbol and economic-capability preflight
  -> bounded chronological cursors
  -> deterministic timestamp batches
  -> FutureQuote execution
  -> accounting, metrics, and lifecycle artifacts
  -> inline result or verified artifact
```

Unsupported economics fail before data access. Output bounds control returned data volume but must not change economic results.

## Signal flow

```text
Telegram batch row --------> TelegramBatchSourceAdapter ---+
                                                           |
Telegram relay delivery ---> TelegramRelaySourceAdapter ---+-> source event
  -> durable preflight and reservation
  -> stateless route
  -> selected-pipeline snapshot when required
  -> optional decoder/parser and shared validation
  -> fenced compare-and-commit
  -> committed normalization batch, lifecycle facts, checkpoint, and outbox state

manual/API RawSignal
  -> profile and sizing
  -> deterministic replay
```

The Telegram adapters terminate before neutral runner ownership. They preserve exact chat, message, thread, and reply identities in opaque versioned keys, attach strict bounded evidence, and provide stable offline-position or relay-delivery identities for durable duplicate handling. The legacy producer reconstructs bounded `ChannelParser` history and parent context from the selected state snapshot; source deletes bypass it and commit lifecycle withdrawal only.

The existing `OfflineRunner`, optional `OnlineServer`, handler callbacks, and standalone JSONL contracts remain unchanged compatibility facades and are not hosted through durable state.

A committed normalization batch is an authoritative ingestion result, but it does not enter replay automatically. Source edits, deletes, supersession, and withdrawal remain audit and lifecycle facts unless an explicit downstream bridge produces an eligible trading action. A source event is not a trade, and a parsed signal is not a broker command. Strategy decisions, portfolio supervision, execution planning, venue translation, and execution reports remain explicit downstream responsibilities.

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
