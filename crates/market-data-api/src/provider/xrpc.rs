use async_trait::async_trait;
use qs_service::ServiceEndpoint;
use qs_service_xrpc::{
    BincodeCodec, XrpcClientSession, XrpcProviderError, XrpcTransportConfig, map_rpc_error,
};

use crate::{
    AlertEventStream, AlertResult, CommandAck, GetAlertsResponse, GetPriceRequest,
    GetPriceResponse, GetPricesRequest, GetPricesResponse, GetStateResponse, MarketDataClient,
    MarketDataClientError, MarketDataServiceProtocolError, PriceEventStream, RemoveAlertRequest,
    SetAlertRequest, StreamEvent, SubscribePricesRequest, UnsubscribePricesRequest,
};

/// Market-data xrpc connection facade. Provider types are not exposed by its API.
pub struct MarketDataXrpcClient {
    session: XrpcClientSession<BincodeCodec>,
}

impl MarketDataXrpcClient {
    pub async fn connect(
        endpoint: &ServiceEndpoint,
        client_name: &str,
        config: &XrpcTransportConfig,
    ) -> Result<Self, MarketDataClientError> {
        let session = qs_service_xrpc::connect(endpoint, client_name, config, BincodeCodec)
            .await
            .map_err(map_provider_error)?;
        Ok(Self { session })
    }

    pub fn endpoint(&self) -> &ServiceEndpoint {
        self.session.endpoint()
    }

    pub async fn close(self) -> Result<(), MarketDataClientError> {
        self.session.close().await.map_err(map_provider_error)
    }
}

fn map_provider_error(error: XrpcProviderError) -> MarketDataClientError {
    match error {
        XrpcProviderError::Transport(error) => error.into(),
        XrpcProviderError::Remote(message) => MarketDataClientError::Service(message),
        XrpcProviderError::Protocol(detail) | XrpcProviderError::ClientTask(detail) => {
            MarketDataServiceProtocolError { detail }.into()
        }
    }
}

#[async_trait]
impl MarketDataClient for MarketDataXrpcClient {
    async fn get_price(
        &self,
        request: GetPriceRequest,
    ) -> Result<GetPriceResponse, MarketDataClientError> {
        self.session
            .call("get_price", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn get_prices(
        &self,
        request: GetPricesRequest,
    ) -> Result<GetPricesResponse, MarketDataClientError> {
        self.session
            .call("get_prices", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn state(&self) -> Result<GetStateResponse, MarketDataClientError> {
        self.session
            .call("get_state", &())
            .await
            .map_err(map_provider_error)
    }

    async fn subscribe(
        &self,
        request: SubscribePricesRequest,
    ) -> Result<CommandAck, MarketDataClientError> {
        self.session
            .call("subscribe", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn unsubscribe(
        &self,
        request: UnsubscribePricesRequest,
    ) -> Result<CommandAck, MarketDataClientError> {
        self.session
            .call("unsubscribe", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn clear_subscription(&self) -> Result<CommandAck, MarketDataClientError> {
        self.session
            .call("clear_subscription", &())
            .await
            .map_err(map_provider_error)
    }

    async fn set_alert(
        &self,
        request: SetAlertRequest,
    ) -> Result<CommandAck, MarketDataClientError> {
        self.session
            .call("set_alert", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn remove_alert(
        &self,
        request: RemoveAlertRequest,
    ) -> Result<CommandAck, MarketDataClientError> {
        self.session
            .call("remove_alert", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn alerts(&self) -> Result<GetAlertsResponse, MarketDataClientError> {
        self.session
            .call("get_alerts", &())
            .await
            .map_err(map_provider_error)
    }

    async fn events(&self) -> Result<PriceEventStream, MarketDataClientError> {
        let endpoint = self.session.endpoint().clone();
        let receiver = self
            .session
            .call_server_stream::<_, StreamEvent>("stream_events", &())
            .await
            .map_err(map_provider_error)?;
        Ok(Box::pin(futures::stream::unfold(
            receiver,
            move |mut receiver| {
                let endpoint = endpoint.clone();
                async move {
                    receiver.recv().await.map(|item| {
                        let item = item.map_err(|error| {
                            map_provider_error(map_rpc_error(error, Some(endpoint)))
                        });
                        (item, receiver)
                    })
                }
            },
        )))
    }

    async fn alert_events(&self) -> Result<AlertEventStream, MarketDataClientError> {
        let endpoint = self.session.endpoint().clone();
        let receiver = self
            .session
            .call_server_stream::<_, AlertResult>("stream_alerts", &())
            .await
            .map_err(map_provider_error)?;
        Ok(Box::pin(futures::stream::unfold(
            receiver,
            move |mut receiver| {
                let endpoint = endpoint.clone();
                async move {
                    receiver.recv().await.map(|item| {
                        let item = item.map_err(|error| {
                            map_provider_error(map_rpc_error(error, Some(endpoint)))
                        });
                        (item, receiver)
                    })
                }
            },
        )))
    }
}
