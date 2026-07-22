use async_trait::async_trait;
use qs_service::ServiceEndpoint;
use qs_service_xrpc::{
    JsonCodec, XrpcClientSession, XrpcProviderError, XrpcTransportConfig, map_rpc_error,
};

use crate::{
    BacktestClient, BacktestClientError, BacktestEvent, BacktestEventStream,
    BacktestServiceProtocolError, BacktestStatusResponse, CancelBacktestRequest,
    CancelBacktestResponse, GetBacktestResultRequest, GetBacktestResultResponse,
    GetBacktestStatusRequest, PingResponse, SubmitBacktestRequest, SubmitBacktestResponse,
    WatchBacktestRequest,
};

/// Backtest connection façade. Its public contract is provider-neutral.
pub struct BacktestXrpcClient {
    session: XrpcClientSession<JsonCodec>,
}

impl BacktestXrpcClient {
    pub async fn connect(
        endpoint: &ServiceEndpoint,
        client_name: &str,
        config: &XrpcTransportConfig,
    ) -> Result<Self, BacktestClientError> {
        let session = qs_service_xrpc::connect(endpoint, client_name, config, JsonCodec)
            .await
            .map_err(map_provider_error)?;
        Ok(Self { session })
    }

    pub fn endpoint(&self) -> &ServiceEndpoint {
        self.session.endpoint()
    }

    pub async fn close(self) -> Result<(), BacktestClientError> {
        self.session.close().await.map_err(map_provider_error)
    }
}

fn map_provider_error(error: XrpcProviderError) -> BacktestClientError {
    match error {
        XrpcProviderError::Transport(error) => error.into(),
        XrpcProviderError::Remote(message) => BacktestClientError::Service(message),
        XrpcProviderError::Protocol(detail) | XrpcProviderError::ClientTask(detail) => {
            BacktestServiceProtocolError { detail }.into()
        }
    }
}

#[async_trait]
impl BacktestClient for BacktestXrpcClient {
    async fn ping(&self) -> Result<PingResponse, BacktestClientError> {
        self.session
            .call("ping", &())
            .await
            .map_err(map_provider_error)
    }

    async fn submit(
        &self,
        request: SubmitBacktestRequest,
    ) -> Result<SubmitBacktestResponse, BacktestClientError> {
        self.session
            .call("submit_backtest", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn status(&self, job_id: &str) -> Result<BacktestStatusResponse, BacktestClientError> {
        self.session
            .call(
                "get_backtest_status",
                &GetBacktestStatusRequest {
                    job_id: job_id.into(),
                },
            )
            .await
            .map_err(map_provider_error)
    }

    async fn watch(&self, job_id: &str) -> Result<BacktestEventStream, BacktestClientError> {
        let endpoint = self.session.endpoint().clone();
        let receiver = self
            .session
            .call_server_stream::<_, BacktestEvent>(
                "watch_backtest",
                &WatchBacktestRequest {
                    job_id: job_id.into(),
                },
            )
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

    async fn result(&self, job_id: &str) -> Result<GetBacktestResultResponse, BacktestClientError> {
        self.session
            .call(
                "get_backtest_result",
                &GetBacktestResultRequest {
                    job_id: job_id.into(),
                },
            )
            .await
            .map_err(map_provider_error)
    }

    async fn cancel(&self, job_id: &str) -> Result<CancelBacktestResponse, BacktestClientError> {
        self.session
            .call(
                "cancel_backtest",
                &CancelBacktestRequest {
                    job_id: job_id.into(),
                },
            )
            .await
            .map_err(map_provider_error)
    }
}
