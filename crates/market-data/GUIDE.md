# market-data

Real-time market-data service that connects to the CTrader FIX API for live forex/CFD bid and ask prices, then exposes snapshots, subscriptions, source state, and price alerts through a logical service endpoint.

## Features

- Live bid/ask streaming from CTrader FIX
- Per-client price subscriptions with symbol filtering
- One-shot ABOVE/BELOW price alerts with ownership cleanup
- CTrader disconnect detection and automatic reconnection with prior-session quote invalidation
- Shared-memory default plus Unix-socket and loopback-TCP endpoints
- One owned client lifecycle with deterministic close and task join

## Quick start

A live run requires valid CTrader FIX credentials and network access to the configured server. Copy the template to a protected local file, replace its placeholders, and do not commit credentials.

```bash
cargo run -p qs-market-data --bin ctrader_market_data -- --config /path/to/config_market_data.toml
cargo test -p qs-market-data
```

Run the optional TUI or minimal example against the default endpoint:

```bash
cargo run -p qs-market-data --features tui-client --bin market_data_client -- --endpoint shm://market-data --symbols eurusd,xauusd
cargo run -p qs-market-data --example client -- --endpoint shm://market-data --symbols eurusd,xauusd
```

## Configuration

```toml
[ctrader]
sendercompid = "demo.ctrader.12345"
server = "demo.ctrader.com"
username = "12345"
password = "your_password"
ssl = false

[market_data]
endpoint = "shm://market-data"
shm_buffer_size = 4194304
max_connections = 256
allow_insecure_non_loopback = false

[logging]
level = "info"
```

The current configuration schema parses `ssl`, but workspace code does not use that field to select connector behavior. Do not treat the value as proof that the upstream FIX connection is encrypted; verify the provider and `ctrader-fix` transport requirements for the selected endpoint.

Supported endpoints are `shm://NAME`, `unix:///absolute/path.sock`, and `tcp://IP:PORT`. SHM remains the recommended same-host default. Unix sockets provide direct same-host IPC without SHM mappings. TCP is unauthenticated and restricted to loopback unless `allow_insecure_non_loopback = true` explicitly acknowledges a trusted-network boundary; do not expose it directly to the public Internet.

Legacy `shm_name = "market-data"` configuration and the `--shm-name` client argument remain accepted during migration, but new deployments should use `endpoint`.

## Client contract

The provider-neutral `MarketDataClient` trait lives in `qs-market-data-api`. Application consumers work with typed snapshots, commands, and event streams; they do not construct transport handshakes, shared-memory slot names, or RPC receive-task handles.

### Unary operations

| Operation | Request | Response |
|---|---|---|
| Ping | `()` | `CommandAck` |
| Source state | `()` | `GetStateResponse` |
| Symbols | `()` | `GetSymbolListResponse` |
| One snapshot | `GetPriceRequest` | `GetPriceResponse` |
| Multiple snapshots | `GetPricesRequest` | `GetPricesResponse` |
| Subscribe | `SubscribePricesRequest` | `CommandAck` |
| Unsubscribe | `UnsubscribePricesRequest` | `CommandAck` |
| Clear subscription | `()` | `CommandAck` |
| Set alert | `SetAlertRequest` | `CommandAck` |
| Remove alert | `RemoveAlertRequest` | `CommandAck` |
| Owned alerts | `()` | `GetAlertsResponse` |

### Streams

| Stream | Item | Meaning |
|---|---|---|
| Price stream | `PriceTick` | Subscription-filtered bid/ask updates |
| Alert stream | `AlertResult` | Triggers for alerts owned by the client |
| Combined event stream | `StreamEvent` | Current source-state snapshot followed by price, state-transition, and data-quality events |

The provider-neutral trait covers snapshots, source state, local subscription filters, alerts, combined events, and alert events. Ping, symbol discovery, and the direct price stream are currently provider-edge operations used by the bundled clients rather than methods on `MarketDataClient`.

Transport connectivity, CTrader source state, and quote freshness are separate facts. Entering `CONNECTING` or `DISCONNECTED` clears cached quotes, so `found = false` remains authoritative until a replacement-session quote arrives. Price timestamps are service observation times shared by cache snapshots and streams; they are not exchange or FIX source timestamps. State timestamps are transition times rather than query times.

The combined stream emits `DATA_QUALITY` for detected receiver lag and rejected spot subscriptions. A known lag includes the dropped item count and delivery continues when possible. This visibility does not imply replay, exactly-once delivery, source sequence tracking, or market-calendar-aware stale classification. The direct price stream remains price-only and best-effort.

## Provider compatibility

The current internal provider uses xrpc-rs 0.3.1. Its 0.3.0 and 0.3.1 SHM layouts are compatible with one another, while the 0.3 SHM generation is incompatible with 0.2. Stop old peers before reusing the same SHM endpoint or use non-overlapping endpoint names for a side-by-side cutover. The common service runtime owns exact endpoint cleanup, connection limits, accept loops, and close/join behavior.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
