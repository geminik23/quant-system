# Ingestion compatibility fixtures

These fixtures record the current Telegram-oriented parser behavior before source-neutral ingestion contracts are introduced.

- Inputs are synthetic and contain no provider data.
- Exact-byte JSONL expectations include record order and the final newline.
- Exact-byte expectations compare generated artifacts directly with the committed golden files.
- Semantic expectations describe behavior that is not represented as a persisted artifact.
- The fixture manifest is test-only and is not the future runtime ingestion manifest.

The structured fixture proves that processing order and outcome order differ: messages are processed by Telegram identity for history, outcomes retain input order, successful signals sort by signal timestamp, and an unregistered source is classified before its timestamp is parsed.
