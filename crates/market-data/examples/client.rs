//! Basic CLI client for the market data service.
//!
//! Connects to the market-data service, subscribes to price ticks, sets a
//! price alert, and streams prices.
//!
//! Usage:
//!   cargo run -p qs-market-data --example client
//!   cargo run -p qs-market-data --example client -- --endpoint shm://market-data --symbols eurusd,xauusd

use std::sync::Arc;

use clap::Parser;
use market_data::rpc_types::*;
use qs_service::ServiceEndpoint;
use qs_service_xrpc::{BincodeCodec, DynRpcClient, XrpcClientSession, XrpcTransportConfig};

#[derive(Parser, Debug)]
#[command(about = "Market data service client example")]
struct Args {
    /// Legacy shared-memory service name (must match server configuration).
    #[arg(long, default_value = "market-data")]
    shm_name: String,

    /// Transport endpoint. When omitted, `--shm-name` is interpreted as `shm://NAME`.
    #[arg(long)]
    endpoint: Option<ServiceEndpoint>,

    /// Symbols to subscribe to (comma-separated, empty = all)
    #[arg(long, default_value = "")]
    symbols: String,

    /// Set a test alert: symbol:kind:price (e.g. eurusd:ABOVE:1.1000)
    #[arg(long)]
    alert: Option<String>,
}

type MarketClient = DynRpcClient<BincodeCodec>;

struct MarketClientSession {
    client: Arc<MarketClient>,
    session: XrpcClientSession<BincodeCodec>,
}

impl MarketClientSession {
    fn client(&self) -> Arc<MarketClient> {
        Arc::clone(&self.client)
    }

    async fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        self.session.close().await?;
        Ok(())
    }
}

/// Connect through the configured logical service endpoint.
async fn connect(
    endpoint: &ServiceEndpoint,
    client_name: &str,
) -> Result<MarketClientSession, Box<dyn std::error::Error>> {
    let session = qs_service_xrpc::connect(
        endpoint,
        client_name,
        &XrpcTransportConfig::default(),
        BincodeCodec,
    )
    .await?;
    println!(
        "[connect] endpoint={endpoint} client_id={:?}",
        session.logical_client_id()
    );
    let client = session.raw_client();
    Ok(MarketClientSession { client, session })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Connect
    let endpoint = match args.endpoint.clone() {
        Some(endpoint) => endpoint,
        None => format!("shm://{}", args.shm_name).parse()?,
    };
    let session = connect(&endpoint, "example-client").await?;
    let client = session.client();

    let operation_result: Result<(), Box<dyn std::error::Error>> = async {
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
        Ok(())
    }
    .await;

    let shutdown_result = session.close().await;
    match (operation_result, shutdown_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(shutdown_error)) => Err(shutdown_error),
        (Err(operation_error), Ok(())) => Err(operation_error),
        (Err(operation_error), Err(shutdown_error)) => {
            eprintln!("[shutdown] client shutdown also failed: {shutdown_error}");
            Err(operation_error)
        }
    }
}
