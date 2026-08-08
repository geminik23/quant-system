# Signal ingestion

The source-neutral compatibility boundary is strict `RawSignal` JSONL. The backtest path does not require Telegram or the parser crate when another producer already emits that format.

## Direct normalized input

Use direct JSONL when source decoding and normalization happen elsewhere. Each line must be one supported action with no unknown top-level fields. See the [RawSignal reference](reference/raw-signal.md).

## Stateless normalization library

`signal_parser::normalization` provides bounded source-neutral routing, typed structured-decoder and text-parser pipelines, immutable history/parent context values, semantic reports, and mandatory shared core validation before candidate construction. The built-in strict `quant-system/raw-signals@1` JSON decoder covers every current `RawSignal` action without changing the standalone direct JSONL format.

Routing and selected-pipeline evaluation are separate operations. No-route and cross-pipeline ambiguity complete without requesting pipeline context, while a selected route returns a `PreparedEvaluation` carrying exact context requirements. The evaluator remains inline and stateless. Durable application is provided by the separate `signal_parser::state` boundary; neither layer performs source transport IO, worker scheduling, external sink calls, or venue action.

## Durable source application

`signal_parser::state` provides backend-neutral contracts for durable intake, monotonic and unversioned duplicate policy, fenced reservations, cutoff-safe selected-pipeline snapshots, evaluation-attempt recording, compare-and-commit, committed batches, immutable lifecycle facts, checkpoints, and committed-batch publication outbox leasing.

`MemorySourceStateStore` is the semantic conformance implementation. `SqliteSourceStateStore` persists the same bounded logical state through one transactionally replaced schema-versioned snapshot. The SQLite backend supports one process, serialized writers, and SQLite-supported local filesystems. It does not claim distributed consensus, coordinated multi-process writers, or network-filesystem safety. Unknown schema versions and malformed persisted state fail closed.

Completed semantic evaluations may commit auditable batches even when they contain no normalized envelopes. Operational failures are recorded separately and do not advance the application checkpoint. The initial delete policy withdraws active normalized outputs as lifecycle facts without running a normalization pipeline or synthesizing a close, cancellation, exit, or broker command.

Application and publication progress are separate. The state layer atomically creates enabled committed-batch outbox records with the application commit, while a future runner performs external sink calls. Lease expiry may redeliver the same stable delivery identity, so external delivery is at least once rather than exactly once.

Recorded receipts, committed batches, source state, and checkpoints are available for separately hosted causal replay and committed redelivery. The state module does not select or retain executable parser graphs.

## Telegram adapter library

`signal_parser::adapters::telegram` is the public provider-specific library boundary before source-neutral routing and durable application. `TelegramBatchSourceAdapter` adapts exported `RawTgMessage` rows as unversioned upserts, while `TelegramRelaySourceAdapter` maps relay new, edit, and delete deliveries to create, update, and delete source operations without flattening their different timestamp and payload rules.

Telegram identity remains exact and opaque in source-event keys after adaptation. Event keys use `tgmsg:v1:{chat_id}:{message_id}`, thread identities use `tgchat:v1:{chat_id}`, and replies reference an event key in the same configured source and Telegram chat. These keys preserve signed 64-bit Telegram identifiers as exact decimal text rather than converting them through a floating-point representation.

Each adaptation produces an accepted, ignored, or rejected outcome with versioned `TelegramSourceEvidenceV1`. Evidence decoding rejects unknown fields, missing required fields, unsupported schema versions, and payloads over 65,536 bytes; timestamp text is limited to 1,024 bytes and ingress delivery identity to 512 bytes. Evidence records the adapter path, opaque identity inputs, source operation, timestamp rule, original timestamp or exact relay epoch bits where applicable, and ingress delivery identity.

Accepted batch rows use stable offline delivery identity derived from artifact identity and row ordinal. Accepted relay outcomes derive stable delivery identity from the supplied ingress delivery ID and output ordinal, and a relay delete expands at most 256 ordered deduplicated message IDs with one stable ordinal per resulting event. The adapters expose deterministic configuration identities so batch and relay policy changes remain distinguishable during durable preflight.

`bind_legacy_telegram_producer` wraps the existing `ChannelParser` registry as a pre-normalized compatibility producer. It reconstructs bounded Telegram history and optional parent context from the selected durable snapshot, preserves parser output order, and sends candidates through the existing shared normalization and core-signal validation before durable compare-and-commit. Deletes remain lifecycle-only durable commits rather than parser calls or synthesized trading actions.

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

The optional `online` feature exposes the existing Telegram-oriented `OnlineServer` library API. The new adapter library does not rewire this compatibility server or its callbacks through durable state. There is no turnkey neutral online ingestion service, source supervisor, or publication worker; applications must still provide process composition.

## Current limits

- The public CLI and optional online server remain Telegram-oriented compatibility facades; source-neutral normalization, durable state, and path-specific Telegram adaptation are library APIs with no neutral hosted runner.
- A full deployment manifest, neutral offline and online runners, source providers, worker scheduling, publication workers, external sink calls, and the committed-batch trading bridge remain application concerns or future work.
- Offline and online parser paths may report failures differently.
- A source edit or delete is not a trading action unless a separately reviewed downstream policy produces an eligible action.
