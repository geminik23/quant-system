use qs_backtest_api::{
    BacktestClientError, ListProfilesResponse, ListSymbolsResponse, PingResponse,
};
use qs_backtest_client::scripted::{ScriptedCall, ScriptedCatalogConnector};
use qs_backtest_client::{
    CatalogProbeStage, DesktopEndpointError, parse_desktop_endpoint, probe_service_catalog,
};

fn connector() -> ScriptedCatalogConnector {
    ScriptedCatalogConnector::success(
        "tcp://127.0.0.1:41001",
        PingResponse {
            status: "ok".into(),
            uptime_secs: 42,
            data_dir: "redacted".into(),
        },
        ListProfilesResponse { profiles: vec![] },
        ListSymbolsResponse { symbols: vec![] },
    )
}

#[tokio::test]
async fn catalog_probe_records_typed_call_order_and_closes() {
    let connector = connector();
    let snapshot = probe_service_catalog(&connector).await.unwrap();

    assert_eq!(snapshot.endpoint_display, "tcp://127.0.0.1:41001");
    assert_eq!(snapshot.ping.status, "ok");
    assert!(snapshot.profiles.profiles.is_empty());
    assert!(snapshot.symbols.symbols.is_empty());
    assert_eq!(
        connector.calls(),
        vec![
            ScriptedCall::Connect,
            ScriptedCall::Ping,
            ScriptedCall::Profiles,
            ScriptedCall::Symbols,
            ScriptedCall::Close,
        ]
    );
}

#[tokio::test]
async fn failed_probe_preserves_stage_and_still_closes() {
    let mut connector = connector();
    connector
        .client_mut()
        .fail_ping(BacktestClientError::Service("not ready".into()));

    let error = probe_service_catalog(&connector).await.unwrap_err();
    assert_eq!(error.stage, CatalogProbeStage::Ping);
    assert_eq!(
        connector.calls(),
        vec![
            ScriptedCall::Connect,
            ScriptedCall::Ping,
            ScriptedCall::Close,
        ]
    );
}

#[tokio::test]
async fn profile_symbol_and_close_failures_keep_exact_stage_and_order() {
    let mut profiles = connector();
    profiles
        .client_mut()
        .fail_profiles(BacktestClientError::Service("profiles unavailable".into()));
    let error = probe_service_catalog(&profiles).await.unwrap_err();
    assert_eq!(error.stage, CatalogProbeStage::Profiles);
    assert_eq!(
        profiles.calls(),
        vec![
            ScriptedCall::Connect,
            ScriptedCall::Ping,
            ScriptedCall::Profiles,
            ScriptedCall::Close,
        ]
    );

    let mut symbols = connector();
    symbols
        .client_mut()
        .fail_symbols(BacktestClientError::Service("symbols unavailable".into()));
    let error = probe_service_catalog(&symbols).await.unwrap_err();
    assert_eq!(error.stage, CatalogProbeStage::Symbols);
    assert_eq!(
        symbols.calls(),
        vec![
            ScriptedCall::Connect,
            ScriptedCall::Ping,
            ScriptedCall::Profiles,
            ScriptedCall::Symbols,
            ScriptedCall::Close,
        ]
    );

    let mut close = connector();
    close.fail_close(BacktestClientError::Service("close failed".into()));
    let error = probe_service_catalog(&close).await.unwrap_err();
    assert_eq!(error.stage, CatalogProbeStage::Close);
    assert_eq!(
        close.calls(),
        vec![
            ScriptedCall::Connect,
            ScriptedCall::Ping,
            ScriptedCall::Profiles,
            ScriptedCall::Symbols,
            ScriptedCall::Close,
        ]
    );
}

#[test]
fn desktop_endpoint_accepts_only_loopback_tcp() {
    let endpoint = parse_desktop_endpoint("tcp://127.0.0.1:41001").unwrap();
    assert!(endpoint.is_tcp_loopback());

    assert_eq!(
        parse_desktop_endpoint("http://127.0.0.1:41001").unwrap_err(),
        DesktopEndpointError::TcpRequired
    );
    assert_eq!(
        parse_desktop_endpoint("tcp://192.168.1.10:41001").unwrap_err(),
        DesktopEndpointError::LoopbackRequired
    );
    assert_eq!(
        parse_desktop_endpoint("tcp://user@127.0.0.1:41001").unwrap_err(),
        DesktopEndpointError::InvalidSyntax
    );
    assert_eq!(
        parse_desktop_endpoint("tcp://127.0.0.1:41001?token=x").unwrap_err(),
        DesktopEndpointError::InvalidSyntax
    );
}
