# market-data

Real-time market data service connecting to CTrader FIX API, exposed to local clients via xrpc-rs shared memory IPC.

## Features

- Live bid/ask price streaming from CTrader FIX
- Multi-client support via shared memory acceptor pattern
- Per-client price subscriptions with symbol filtering
- One-shot price alerts (ABOVE/BELOW threshold)
- Automatic reconnection on CTrader disconnect
- Alert cleanup on client disconnect (no orphans)

## Quick Start

```bash
# Build
cargo build -p market-data

# Run server
cargo run -p market-data --bin market_data -- --config config.toml

# Run example client
cargo run -p market-data --example client -- --shm-name market-data --symbols eurusd,xauusd

# Run tests
cargo test -p market-data
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
shm_name = "market-data"
shm_buffer_size = 4194304  # 4MB

[logging]
level = "info"
```

## Client Connection

1. Connect to `shm://{shm_name}-accept`
2. Call `connect` RPC → receive dedicated `slot_name`
3. Reconnect to `shm://{slot_name}` for all subsequent RPCs

## RPC Methods

**Unary**: `ping`, `get_state`, `get_symbols`, `get_price`, `get_prices`, `subscribe`, `unsubscribe`, `clear_subscription`, `set_alert`, `remove_alert`

**Server Streaming**: `stream_prices` (filtered by subscription), `stream_alerts` (filtered by ownership)

See `examples/client.rs` for a complete usage example.
