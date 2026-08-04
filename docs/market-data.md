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

Local transport connectivity and upstream CTrader freshness are separate states. A client can connect to the local service while the FIX source is reconnecting or stale. Consumers must observe source-state events and handle reconnect or resubscribe as potentially data-loss-visible behavior.

## Supported operations

The provider-neutral `MarketDataClient` supports:

- ping and source-state queries;
- symbol discovery;
- one or multiple snapshots;
- filtered price subscriptions;
- one-shot above/below alerts;
- price, alert, and combined source-state streams.

See the full [`qs-market-data` guide](../crates/market-data/GUIDE.md) for configuration and the typed client contract.
