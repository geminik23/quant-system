# Signal ingestion

The source-neutral compatibility boundary is strict `RawSignal` JSONL. The backtest path does not require Telegram or the parser crate when another producer already emits that format.

## Direct normalized input

Use direct JSONL when source decoding and normalization happen elsewhere. Each line must be one supported action with no unknown top-level fields. See the [RawSignal reference](reference/raw-signal.md).

## Stateless normalization library

`signal_parser::normalization` provides bounded source-neutral routing, typed structured-decoder and text-parser pipelines, immutable history/parent context values, semantic reports, and mandatory shared core validation before candidate construction. The built-in strict `quant-system/raw-signals@1` JSON decoder covers every current `RawSignal` action without changing the standalone direct JSONL format.

Routing and selected-pipeline evaluation are separate operations. No-route and cross-pipeline ambiguity complete without requesting pipeline context, while a selected route returns a `PreparedEvaluation` carrying exact context requirements. The current implementation runs inline stateless components only and performs no durable source application, IO, worker scheduling, publication, or venue action.

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

The optional `online` feature exposes an `OnlineServer` library API. There is no turnkey online parser binary or durable source supervisor; applications must provide their own handler and process composition.

## Current limits

- The public CLI remains Telegram-oriented; the source-neutral normalization API is currently library-only and has no generic hosted runner.
- Source transport connections, durable state, idempotency storage, lifecycle commit, and publication remain application concerns.
- Offline and online parser paths may report failures differently.
- A source edit or delete is not a trading action unless an upstream policy explicitly normalizes it.
