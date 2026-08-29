use async_trait::async_trait;
use chrono::{DateTime, Utc};
use qs_backtest_api::{
    BacktestClient, BacktestClientError, BacktestDiscoveryClient, ListProfilesResponse,
    ListSymbolsRequest, ListSymbolsResponse, PingResponse,
};
use qs_service::{EndpointScheme, ServiceEndpoint};

use crate::{CatalogProbeError, CatalogProbeStage, DesktopEndpointError};

/// Successful service health and discovery snapshot.
#[derive(Debug, Clone)]
pub struct ServiceCatalogSnapshot {
    pub endpoint_display: String,
    pub ping: PingResponse,
    pub profiles: ListProfilesResponse,
    pub symbols: ListSymbolsResponse,
    pub loaded_at: DateTime<Utc>,
}

/// Connector used by catalog probes and later workflow composition.
#[async_trait]
pub trait BacktestCatalogConnector: Send + Sync + 'static {
    type Client: BacktestClient + BacktestDiscoveryClient + Send + Sync;

    fn endpoint_display(&self) -> String;

    async fn connect(&self) -> Result<Self::Client, BacktestClientError>;

    async fn close(&self, client: Self::Client) -> Result<(), BacktestClientError>;
}

/// Parse the normal Windows desktop endpoint surface.
///
/// HTTP, credentials, query strings, fragments, custom schemes, and
/// non-loopback TCP are rejected by construction.
pub fn parse_desktop_endpoint(value: &str) -> Result<ServiceEndpoint, DesktopEndpointError> {
    let endpoint: ServiceEndpoint = value
        .parse()
        .map_err(|_| DesktopEndpointError::InvalidSyntax)?;
    if !matches!(endpoint.scheme(), EndpointScheme::Tcp) {
        return Err(DesktopEndpointError::TcpRequired);
    }
    if !endpoint.is_tcp_loopback() {
        return Err(DesktopEndpointError::LoopbackRequired);
    }
    Ok(endpoint)
}

/// Connect, read the complete discovery snapshot, and close the session.
pub async fn probe_service_catalog<C>(
    connector: &C,
) -> Result<ServiceCatalogSnapshot, CatalogProbeError>
where
    C: BacktestCatalogConnector,
{
    let client = connector
        .connect()
        .await
        .map_err(|error| CatalogProbeError::new(CatalogProbeStage::Connect, error))?;

    let probe = async {
        let ping = client
            .ping()
            .await
            .map_err(|error| CatalogProbeError::new(CatalogProbeStage::Ping, error))?;
        let profiles = client
            .list_profiles()
            .await
            .map_err(|error| CatalogProbeError::new(CatalogProbeStage::Profiles, error))?;
        let symbols = client
            .list_symbols(ListSymbolsRequest {
                exchange: None,
                data_type: None,
            })
            .await
            .map_err(|error| CatalogProbeError::new(CatalogProbeStage::Symbols, error))?;
        Ok::<_, CatalogProbeError>((ping, profiles, symbols))
    }
    .await;

    match probe {
        Ok((ping, profiles, symbols)) => {
            connector
                .close(client)
                .await
                .map_err(|error| CatalogProbeError::new(CatalogProbeStage::Close, error))?;
            Ok(ServiceCatalogSnapshot {
                endpoint_display: connector.endpoint_display(),
                ping,
                profiles,
                symbols,
                loaded_at: Utc::now(),
            })
        }
        Err(error) => {
            let _ = connector.close(client).await;
            Err(error)
        }
    }
}
