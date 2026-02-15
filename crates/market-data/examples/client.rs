//! Example xrpc market data client.
//!
//! Connects to the market data server via shared memory, subscribes to
//! price ticks, sets a price alert, and streams prices.
//!
//! Usage:
//!   cargo run --example client
//!   cargo run --example client -- --shm-name market-data --symbols eurusd,xauusd

use std::sync::Arc;

use clap::Parser;
use market_data::rpc_types::*;
use xrpc::{MessageChannelAdapter, RpcClient, SharedMemoryFrameTransport};

#[derive(Parser, Debug)]
#[command(about = "Market data xrpc client example")]
struct Args {
    /// Shared memory acceptor name (must match server config)
    #[arg(long, default_value = "market-data")]
    shm_name: String,

    /// Symbols to subscribe to (comma-separated, empty = all)
    #[arg(long, default_value = "")]
    symbols: String,

    /// Set a test alert: symbol:kind:price (e.g. eurusd:ABOVE:1.1000)
    #[arg(long)]
    alert: Option<String>,
}

/// Connect to the acceptor, get a dedicated slot, return an RpcClient on that slot.
async fn connect(
    shm_name: &str,
    client_name: &str,
) -> Result<
    (
        RpcClient<MessageChannelAdapter<SharedMemoryFrameTransport>>,
        ConnectResponse,
    ),
    Box<dyn std::error::Error>,
> {
    let accept_name = format!("{}-accept", shm_name);

    // Step 1: Connect to acceptor
    let acceptor_transport = SharedMemoryFrameTransport::connect_client(&accept_name)?;
    let acceptor_channel = MessageChannelAdapter::new(acceptor_transport);
    let acceptor_client = RpcClient::new(acceptor_channel);
    let _handle = acceptor_client.start();

    let resp: ConnectResponse = acceptor_client
        .call(
            "connect",
            &ConnectRequest {
                client_name: client_name.into(),
            },
        )
        .await?;

    println!(
        "[connect] assigned client_id={}, slot={}",
        resp.client_id, resp.slot_name
    );

    // Give server a moment to create the slot
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    acceptor_client.close().await?;

    // Step 2: Connect to dedicated slot
    let transport = SharedMemoryFrameTransport::connect_client(&resp.slot_name)?;
    let channel = MessageChannelAdapter::new(transport);
    let client = RpcClient::new(channel);
    let _handle = client.start();

    Ok((client, resp))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Connect
    let (client, _info) = connect(&args.shm_name, "example-client").await?;
    let client = Arc::new(client);

    // Ping
    let ack: CommandAck = client.call("ping", &()).await?;
    println!("[ping] kind={} ref={}", ack.kind, ack.reference);

    // Get connection state
    let state: GetStateResponse = client.call("get_state", &()).await?;
    println!("[state] {} at ts={}", state.state, state.ts_ms);

    // Get symbol list
    let symbols: GetSymbolListResponse = client.call("get_symbols", &()).await?;
    println!("[symbols] {} symbols available", symbols.symbols.len());
    if symbols.symbols.len() <= 20 {
        println!("  {:?}", symbols.symbols);
    } else {
        println!("  first 20: {:?}", &symbols.symbols[..20]);
    }

    // Get a single price
    if let Some(first) = symbols.symbols.first() {
        let price: GetPriceResponse = client
            .call(
                "get_price",
                &GetPriceRequest {
                    symbol: first.clone(),
                },
            )
            .await?;
        println!(
            "[get_price] {} bid={} ask={} found={}",
            price.symbol, price.bid, price.ask, price.found
        );
    }

    // Subscribe to prices
    let sub_symbols: Vec<String> = if args.symbols.is_empty() {
        vec![] // empty = all
    } else {
        args.symbols
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    };

    let ack: CommandAck = client
        .call(
            "subscribe",
            &SubscribePricesRequest {
                symbols: sub_symbols.clone(),
            },
        )
        .await?;
    println!("[subscribe] {} ref={}", ack.kind, ack.reference);

    // Set alert if requested
    if let Some(alert_str) = &args.alert {
        let parts: Vec<&str> = alert_str.split(':').collect();
        if parts.len() == 3 {
            let ack: CommandAck = client
                .call(
                    "set_alert",
                    &SetAlertRequest {
                        alert_id: String::new(),
                        symbol: parts[0].to_string(),
                        kind: parts[1].to_uppercase(),
                        price: parts[2].parse().expect("invalid alert price"),
                    },
                )
                .await?;
            println!("[set_alert] {} ref={}", ack.kind, ack.reference);
        } else {
            eprintln!("Invalid --alert format. Use: symbol:kind:price");
        }
    }

    // Stream prices (runs until Ctrl-C)
    println!("\n[stream_prices] streaming... (Ctrl-C to stop)\n");

    let mut stream = client
        .call_server_stream::<_, PriceTick>("stream_prices", &())
        .await?;

    let mut count = 0u64;
    while let Some(result) = stream.recv().await {
        match result {
            Ok(tick) => {
                count += 1;
                println!(
                    "  #{:>6} {} bid={:<12} ask={:<12} ts={}",
                    count, tick.symbol, tick.bid, tick.ask, tick.ts_ms
                );
            }
            Err(e) => {
                eprintln!("[stream error] {:?}", e);
                break;
            }
        }
    }

    println!("\n[done] received {} ticks", count);
    client.close().await?;
    Ok(())
}
