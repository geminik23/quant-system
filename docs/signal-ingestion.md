# Signal ingestion

The source-neutral compatibility boundary is strict `RawSignal` JSONL. The backtest path does not require Telegram or the parser crate when another producer already emits that format.

## Direct normalized input

Use direct JSONL when source decoding and normalization happen elsewhere. Each line must be one supported action with no unknown top-level fields. See the [RawSignal reference](reference/raw-signal.md).

## Stateless normalization library

`signal_parser::normalization` provides bounded source-neutral routing, typed structured-decoder and text-parser pipelines, immutable history/parent context values, semantic reports, and mandatory shared core validation before candidate construction. The built-in strict `quant-system/raw-signals@1` JSON decoder covers every current `RawSignal` action without changing the standalone direct JSONL format.

Routing and selected-pipeline evaluation are separate operations. No-route and cross-pipeline ambiguity complete without requesting pipeline context, while a selected route returns a `PreparedEvaluation` carrying exact context requirements. The evaluator remains inline and stateless. Durable application is provided by the separate `signal_parser::state` boundary; neither layer performs source transport IO, worker scheduling, external sink calls, or venue action.

## Durable source application

`signal_parser::state` durably applies source events, records lifecycle facts and committed batches, and provides checkpoints for replay. `MemorySourceStateStore` is intended for conformance and tests. `SqliteSourceStateStore` supports serialized writers on one process and SQLite-supported local filesystems; it is not a distributed or network-filesystem coordination system.

A completed evaluation may commit an auditable batch even when it has no normalized envelopes. A source delete withdraws active outputs as a lifecycle fact. It does not synthesize a close, cancellation, exit, or broker command. Committed-batch publication is separate from source application and is at-least-once.

## Reference JSONL codecs

`signal_parser::adapters::structured_json` provides explicit `source-event-jsonl@1` input and `committed-normalization-jsonl@1` output codecs. These codecs are separate from standalone direct `RawSignal` JSONL and never infer a record kind after decode failure.

Source-event JSONL uses the exact recursively strict version 1 `SourceEvent` wire. Its artifact identity is SHA-256 over exact input bytes, and each non-empty record receives an offline delivery identity using that artifact identity plus its 1-based physical line number. Blank lines retain physical numbering without producing events. `OfflineIngestionRunner` applies an explicit strict-stop or tolerant-continue policy to bounded record errors.

Committed output uses strict `quant-system/committed-normalization-batch@1` records. Each record represents one authoritative committed batch. Physical JSONL line order is not authoritative under at-least-once publication; consumers use committed batch identity for duplicate detection and commit index for logical order.

## Authenticated webhook provider edge

`signal_parser::adapters::webhook` provides a bounded provider edge for `POST /v1/source-events`. It verifies the version 1 HMAC-SHA256 profile over exact raw body bytes, binds configured key IDs to one source, decodes strict `SourceEvent` only after authentication, and rejects a payload source mismatch before replay reservation.

The webhook edge provides replay protection and requires a configured HMAC secret of at least 32 bytes. The optional HTTP binding returns `202 Accepted` with an admission reference, not a committed batch reference. Consumers must resolve processing results separately. Provider replay protection is durable, but hosted application processing is not restart-safe.

## Neutral structured JSONL composition

`signal_parser::runner` provides library composition for strict manifests, local JSONL ingestion, provider bindings, committed-batch publication, and causal replay. Applications own their executable, deployment configuration, source binding, secrets, and sink policy. Committed-batch JSONL publication is at-least-once, so consumers must deduplicate by committed batch identity.

## Telegram adapter library

`signal_parser::adapters::telegram` adapts exported Telegram rows and relay deliveries into source events before routing and durable application. Batch input is treated as upserts; relay input maps new, edit, and delete deliveries to create, update, and delete source operations. Adapter output preserves the provider delivery identity so retries can be reconciled without treating a source edit or delete as a trading action.

The existing `ChannelParser` registry remains available as a compatibility producer. It is separate from direct strict `RawSignal` input and source-neutral ingestion composition.

## Compatibility facades

The existing `OfflineRunner`, optional `OnlineServer`, handler callback surfaces, and standalone input and output JSONL contracts remain unchanged compatibility facades. They are not internally hosted through `signal_parser::state`, so using those facades alone does not provide durable reservations, restart-safe lifecycle state, checkpoints, or publication outbox processing.

## Offline Telegram parser

`qs-signal-parser` includes an offline CLI for configured Telegram channel parsers:

```bash
cargo run -p qs-signal-parser --bin parse_signals -- \
  --input messages.jsonl \
  --parsers-config crates/signal-parser/parsers.example.toml \
  --output signals.jsonl
```

Use `--input -` for stdin and omit `--output` to write JSONL to stdout.

A minimal Telegram input row is:

```json
{"chat_id":2331249584,"msg_id":1,"ts":"2026-01-15T10:00:00","message":"EURUSD BUY NOW SL 1.0800 TP 1.0900","reply_to":null}
```

The configured `channel_ids` select the parser. The included `template` parser recognizes its own bounded message grammar; it is not a general natural-language trading parser.

## Online feature

The optional `online` feature retains the existing Telegram-oriented `OnlineServer` compatibility API. Webhook hosting is enabled by the optional HTTP provider feature. Both remain library APIs that applications embed in their own process lifecycle.

## Current limits

- Source-neutral ingestion is library-first. Applications own their executable, configuration, source bindings, and publication policy. Existing Telegram CLI and online server remain compatibility facades.
- Non-local sinks, additional provider transports, and the committed-batch trading bridge remain future work.
- Offline and online parser paths may report failures differently.
- A source edit or delete is not a trading action unless a separately reviewed downstream policy produces an eligible action.
