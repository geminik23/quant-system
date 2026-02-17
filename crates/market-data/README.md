# market-data

Real-time market data service that connects to **CTrader FIX API** for live forex/CFD prices and exposes them to local clients via **xrpc-rs shared memory IPC**.

## Features

- Live bid/ask price streaming from CTrader FIX protocol
- Multi-client support via shared memory acceptor pattern (one shm segment per client)
- Per-client price subscriptions with symbol filtering
- One-shot price alerts with ABOVE/BELOW thresholds
- Automatic CTrader reconnection on disconnect
- Alert ownership tracking and cleanup on client disconnect

## Quick Start

```bash
# Build the server
cargo build -p market-data

# Run the server
cargo run -p market-data --bin ctrader_market_data -- --config path/to/config.toml

# Run tests
cargo test -p market-data

# Run the TUI client (streaming + commands)
cargo run -p market-data --features tui-client --bin market_data_client -- --shm-name market-data --symbols eurusd,xauusd

# Run the minimal example client
cargo run -p market-data --example client -- --shm-name market-data --symbols eurusd,xauusd
```

### Install via cargo

```bash
# Install the CTrader server binary
cargo install market-data --bin ctrader_market_data

# Install the TUI client
cargo install market-data --bin market_data_client --features tui-client
```

## Configuration

See `template_config_market_data.toml` for a full template.

```toml
[ctrader]
sendercompid = "demo.ctrader.12345"
server = "demo.ctrader.com"
username = "12345"
password = "your_password"
ssl = false

[market_data]
shm_name = "market-data"         # base name for shm endpoints
shm_buffer_size = 4194304        # 4MB per client slot

[logging]
level = "info"
```

## Client Connection Flow

1. Connect to `shm://{shm_name}-accept`
2. Call `connect` RPC → server assigns a `client_id` and dedicated `shm://{shm_name}-client-{id}`
3. Disconnect from acceptor, reconnect to the dedicated slot
4. All subsequent RPCs go through the dedicated slot
5. On disconnect, server cleans up all alerts owned by that client

## RPC Methods

### Unary

| Method | Request | Response | Description |
|--------|---------|----------|-------------|
| `connect` | `ConnectRequest` | `ConnectResponse` | Acceptor handshake, get dedicated slot |
| `ping` | `()` | `CommandAck` | Health check |
| `get_state` | `()` | `GetStateResponse` | Connection state |
| `get_symbols` | `()` | `GetSymbolListResponse` | All available symbols |
| `get_price` | `GetPriceRequest` | `GetPriceResponse` | Latest bid/ask for one symbol |
| `get_prices` | `GetPricesRequest` | `GetPricesResponse` | Latest bid/ask for multiple symbols |
| `subscribe` | `SubscribePricesRequest` | `CommandAck` | Subscribe to price stream |
| `unsubscribe` | `UnsubscribePricesRequest` | `CommandAck` | Remove symbols from subscription |
| `clear_subscription` | `()` | `CommandAck` | Clear all subscriptions |
| `set_alert` | `SetAlertRequest` | `CommandAck` | Set price alert (ABOVE/BELOW) |
| `remove_alert` | `RemoveAlertRequest` | `CommandAck` | Remove an owned alert |
| `get_alerts` | `()` | `GetAlertsResponse` | Query active alerts owned by this client |

### Server Streaming

| Method | Request | Stream Item | Description |
|--------|---------|-------------|-------------|
| `stream_prices` | `()` | `PriceTick` | Continuous price ticks (filtered by subscription) |
| `stream_alerts` | `()` | `AlertResult` | Alert trigger notifications (filtered by ownership) |
| `stream_events` | `()` | `StreamEvent` | Price ticks + connection state changes (recommended) |

## Wire Format

All RPC types derive `serde::{Serialize, Deserialize}`. The xrpc-rs transport uses **Bincode** encoding by default (not JSON). The `serde_json` dependency is only used in tests and error types.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.