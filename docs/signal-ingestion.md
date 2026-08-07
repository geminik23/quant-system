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

The optional `online` feature exposes the existing Telegram-oriented `OnlineServer` library API. There is no turnkey generic online ingestion service, source supervisor, or publication worker; applications must still provide source adaptation and process composition.

## Current limits

- The public CLI and optional online server remain Telegram-oriented; source-neutral normalization and durable state are library APIs with no generic hosted runner.
- Source transports, generic adapters, deployment-manifest compilation, worker scheduling, external sink calls, and the committed-batch trading bridge remain application or future runner concerns.
- Offline and online parser paths may report failures differently.
- A source edit or delete is not a trading action unless an upstream policy explicitly normalizes it.
