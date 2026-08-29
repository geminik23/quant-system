#![cfg(feature = "xrpc")]

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use qs_backtest_api::{
    ListProfilesResponse, ListSymbolsRequest, ListSymbolsResponse, PingResponse, ProfileInfo,
    SymbolAvailability,
};
use qs_backtest_client::provider::xrpc::XrpcBacktestConnector;
use qs_backtest_client::{BacktestCatalogConnector, parse_desktop_endpoint, probe_service_catalog};
use qs_service::ServiceEndpoint;
use qs_service_xrpc::{JsonCodec, XrpcTransportConfig, serve_host};
use tokio::sync::watch;
use xrpc::{RpcError, RpcServer};

#[test]
fn xrpc_connector_keeps_a_redacted_typed_endpoint() {
    let endpoint = parse_desktop_endpoint("tcp://127.0.0.1:41001").unwrap();
    let connector = XrpcBacktestConnector::new(endpoint, "desktop-test");
    assert_eq!(connector.endpoint_display(), "tcp://127.0.0.1:41001");
}

#[tokio::test]
async fn xrpc_connector_probes_a_loopback_tcp_service() {
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let endpoint: ServiceEndpoint = format!("tcp://{address}").parse().unwrap();
    let registrar = Arc::new(
        |server: &RpcServer<JsonCodec>, _context: &qs_service_xrpc::ConnectionContext| {
            server.register_typed("ping", |_: ()| async move {
                Ok::<_, RpcError>(PingResponse {
                    status: "ok".into(),
                    uptime_secs: 123,
                    data_dir: "redacted".into(),
                })
            });
            server.register_typed("list_profiles", |_: ()| async move {
                Ok::<_, RpcError>(ListProfilesResponse {
                    profiles: vec![ProfileInfo {
                        name: "default".into(),
                        use_targets: vec![],
                        close_ratios: vec![],
                        stoploss_mode: "None".into(),
                        rules_count: 0,
                        let_remainder_run: true,
                    }],
                })
            });
            server.register_typed("list_symbols", |request: ListSymbolsRequest| async move {
                assert!(request.exchange.is_none());
                assert!(request.data_type.is_none());
                Ok::<_, RpcError>(ListSymbolsResponse {
                    symbols: vec![SymbolAvailability {
                        exchange: "fixture".into(),
                        symbol: "EURUSD".into(),
                        data_type: "tick".into(),
                        timeframe: None,
                        row_count: 42,
                        earliest: "2026-01-01T00:00:00".into(),
                        latest: "2026-01-01T00:01:00".into(),
                    }],
                })
            });
        },
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let host_endpoint = endpoint.clone();
    let host = tokio::spawn(async move {
        serve_host(
            host_endpoint,
            XrpcTransportConfig::default(),
            JsonCodec,
            registrar,
            shutdown_rx,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let connector = XrpcBacktestConnector::new(endpoint, "desktop-integration-test");
    let snapshot = probe_service_catalog(&connector).await.unwrap();
    assert_eq!(snapshot.ping.status, "ok");
    assert_eq!(snapshot.profiles.profiles.len(), 1);
    assert_eq!(snapshot.symbols.symbols.len(), 1);

    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), host)
        .await
        .expect("host stops within the bound")
        .expect("host task joins")
        .expect("host exits cleanly");
}
