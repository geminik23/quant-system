# Market data

`qs-market-data` connects to CTrader FIX for live forex/CFD bid and ask prices, then exposes typed snapshots, subscriptions, source state, and one-shot alerts through a local service endpoint.

It is a quote-distribution service, not a live order-execution service.

## Prerequisites

- valid CTrader FIX sender ID, server, username, and password;
- network access to the configured CTrader endpoint;
- a supported local endpoint for consumers;
- the optional `tui-client` feature for the terminal UI.

Do not commit real credentials. Copy and protect a local configuration derived from [`template_config_market_data.toml`](../crates/market-data/template_config_market_data.toml).

The current configuration schema parses `ssl`, but workspace code does not use that field to select connector behavior. Do not treat the value as proof that the upstream FIX connection is encrypted; verify the provider and `ctrader-fix` transport requirements for the selected endpoint.

## Run the service

```bash
cargo run -p qs-market-data --bin ctrader_market_data -- \
  --config /path/to/config_market_data.toml
```

Run a minimal client against the default shared-memory endpoint:

```bash
cargo run -p qs-market-data --example client -- \
  --endpoint shm://market-data \
  --symbols eurusd,xauusd
```

Or run the optional TUI:

```bash
cargo run -p qs-market-data --features tui-client --bin market_data_client -- \
  --endpoint shm://market-data \
  --symbols eurusd,xauusd
```

## Endpoint choices

- `shm://NAME` is the default same-host deployment.
- `unix:///absolute/path.sock` provides same-host IPC without shared-memory mappings.
- `tcp://127.0.0.1:PORT` provides loopback TCP.

TCP has no built-in authentication or TLS. Non-loopback binding is rejected unless the operator explicitly enables the insecure trusted-network option. Do not expose the service directly to the public Internet.

## Health model

Local transport connectivity, CTrader source state, and quote freshness are separate facts. A client can connect to the local service while the FIX source is reconnecting. Entering `CONNECTING` or `DISCONNECTED` clears cached quotes, so snapshot requests return `found = false` until the replacement session produces a new observation.

`GetPriceResponse.ts_ms`, `PriceSnapshot.ts_ms`, and streamed `PriceTick.ts_ms` record when this service observed the CTrader callback. They are not exchange event time or FIX `SendingTime`. `GetStateResponse.ts_ms` and `STATE` events record when this service committed the source-state transition.

The combined event stream sends the current source state first and then emits `PRICE`, `STATE`, and `DATA_QUALITY` events. A data-quality event reports service-observed loss such as broadcast receiver lag or a rejected spot subscription, including a dropped count when known. It does not provide replay, exactly-once delivery, an upstream sequence, or market-hours-aware staleness. The direct price stream remains price-only and best-effort; consumers that need gap visibility should use the combined event stream.

## Supported operations

The provider-neutral `MarketDataClient` supports one or multiple snapshots, source-state queries, local subscription filters, one-shot above/below alerts, owned-alert queries, combined price/state/quality events, and alert streams. The current service also registers provider-edge ping, symbol-discovery, and direct price-stream RPCs used by the bundled operational clients; those operations are not methods on the provider-neutral trait.

See the full [`qs-market-data` guide](../crates/market-data/GUIDE.md) for configuration and the typed client contract.
