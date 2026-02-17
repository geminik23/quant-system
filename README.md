# quant-system

A modular workspace for algorithmic trading — real-time market data, strategy execution, and analysis.

## Crates

| Crate | Description |
|-------|-------------|
| [`qs-market-data`](crates/market-data/) | Real-time price streaming from CTrader FIX API, exposed to local clients via shared memory IPC (`xrpc-rs`). Supports per-client subscriptions, price alerts, and automatic reconnection. |

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
