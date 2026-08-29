use std::time::Duration;

use async_trait::async_trait;
use qs_backtest_api::BacktestClientError;
use qs_backtest_api::provider::xrpc::BacktestXrpcClient;
use qs_service::ServiceEndpoint;
use qs_service_xrpc::XrpcTransportConfig;

use crate::BacktestCatalogConnector;

/// Xrpc connector used by process composition roots.
pub struct XrpcBacktestConnector {
    endpoint: ServiceEndpoint,
    client_name: String,
    config: XrpcTransportConfig,
}

impl XrpcBacktestConnector {
    pub fn new(endpoint: ServiceEndpoint, client_name: impl Into<String>) -> Self {
        let config = XrpcTransportConfig {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Some(Duration::from_secs(10)),
            write_timeout: Some(Duration::from_secs(10)),
            maximum_retry_attempts: 1,
            ..XrpcTransportConfig::default()
        };
        Self {
            endpoint,
            client_name: client_name.into(),
            config,
        }
    }

    pub fn with_config(
        endpoint: ServiceEndpoint,
        client_name: impl Into<String>,
        config: XrpcTransportConfig,
    ) -> Self {
        Self {
            endpoint,
            client_name: client_name.into(),
            config,
        }
    }
}

#[async_trait]
impl BacktestCatalogConnector for XrpcBacktestConnector {
    type Client = BacktestXrpcClient;

    fn endpoint_display(&self) -> String {
        self.endpoint.redacted()
    }

    async fn connect(&self) -> Result<Self::Client, BacktestClientError> {
        BacktestXrpcClient::connect(&self.endpoint, &self.client_name, &self.config).await
    }

    async fn close(&self, client: Self::Client) -> Result<(), BacktestClientError> {
        client.close().await
    }
}
