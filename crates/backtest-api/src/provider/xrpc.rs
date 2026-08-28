use async_trait::async_trait;
use qs_service::ServiceEndpoint;
use qs_service_xrpc::{
    JsonCodec, XrpcClientSession, XrpcProviderError, XrpcTransportConfig, map_rpc_error,
};

use crate::{
    AddProfileRequest, AddProfileResponse, BacktestAdminClient, BacktestClient,
    BacktestClientError, BacktestDiscoveryClient, BacktestEvent, BacktestEventStream,
    BacktestServiceProtocolError, BacktestStatusResponse, BacktestSyncClient,
    CancelBacktestRequest, CancelBacktestResponse, DeleteResultArtifactRequest,
    DeleteResultArtifactResponse, GetBacktestResultRequest, GetBacktestResultResponse,
    GetBacktestStatusRequest, GetResultArtifactChunkRequest, GetResultArtifactChunkResponse,
    ListProfilesResponse, ListSymbolsRequest, ListSymbolsResponse, PingResponse,
    ReloadProfilesResponse, RemoveProfileRequest, RemoveProfileResponse, RunBacktestMultiRequest,
    RunBacktestMultiResponse, RunBacktestRequest, RunBacktestResponse, SubmitBacktestRequest,
    SubmitBacktestResponse, WatchBacktestRequest,
};

/// Backtest connection facade. Its public contract is provider-neutral.
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

    async fn get_result_artifact_chunk(
        &self,
        request: GetResultArtifactChunkRequest,
    ) -> Result<GetResultArtifactChunkResponse, BacktestClientError> {
        self.session
            .call("get_result_artifact_chunk", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn delete_result_artifact(
        &self,
        request: DeleteResultArtifactRequest,
    ) -> Result<DeleteResultArtifactResponse, BacktestClientError> {
        self.session
            .call("delete_result_artifact", &request)
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

#[async_trait]
impl BacktestSyncClient for BacktestXrpcClient {
    async fn run_backtest(
        &self,
        request: RunBacktestRequest,
    ) -> Result<RunBacktestResponse, BacktestClientError> {
        self.session
            .call("run_backtest", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn run_backtest_multi(
        &self,
        request: RunBacktestMultiRequest,
    ) -> Result<RunBacktestMultiResponse, BacktestClientError> {
        self.session
            .call("run_backtest_multi", &request)
            .await
            .map_err(map_provider_error)
    }
}

#[async_trait]
impl BacktestDiscoveryClient for BacktestXrpcClient {
    async fn list_profiles(&self) -> Result<ListProfilesResponse, BacktestClientError> {
        self.session
            .call("list_profiles", &())
            .await
            .map_err(map_provider_error)
    }

    async fn list_symbols(
        &self,
        request: ListSymbolsRequest,
    ) -> Result<ListSymbolsResponse, BacktestClientError> {
        self.session
            .call("list_symbols", &request)
            .await
            .map_err(map_provider_error)
    }
}

#[async_trait]
impl BacktestAdminClient for BacktestXrpcClient {
    async fn add_profile(
        &self,
        request: AddProfileRequest,
    ) -> Result<AddProfileResponse, BacktestClientError> {
        self.session
            .call("add_profile", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn remove_profile(
        &self,
        request: RemoveProfileRequest,
    ) -> Result<RemoveProfileResponse, BacktestClientError> {
        self.session
            .call("remove_profile", &request)
            .await
            .map_err(map_provider_error)
    }

    async fn reload_profiles(&self) -> Result<ReloadProfilesResponse, BacktestClientError> {
        self.session
            .call("reload_profiles", &())
            .await
            .map_err(map_provider_error)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use qs_service_xrpc::{ConnectionContext, channel_pair, serve_transport};
    use xrpc::{RpcError, RpcServer};

    use super::*;
    use crate::{
        BacktestConfigMsg, BacktestMultiRunSpec, BacktestRunSpec, FutureQuoteConfigMsg,
        ManagementProfileMsg, ProfileInfo, ProfileRef, ProviderEvaluationOptionsMsg,
        ResultDeliveryMsg, SymbolAvailability,
    };

    fn assert_all_capabilities<T>()
    where
        T: BacktestClient + BacktestSyncClient + BacktestDiscoveryClient + BacktestAdminClient,
    {
    }

    fn run_backtest_request() -> RunBacktestRequest {
        RunBacktestRequest {
            request: BacktestRunSpec {
                symbol: "EURUSD".into(),
                symbols: Vec::new(),
                all_symbols: false,
                exchange: "fixture".into(),
                data_type: "tick".into(),
                timeframe: None,
                from: None,
                to: None,
                raw_signals: Vec::new(),
                profile: None,
                profile_def: None,
                config: BacktestConfigMsg {
                    initial_balance: None,
                    close_on_finish: None,
                    fill_model: None,
                    sizing: None,
                },
            },
            future: FutureQuoteConfigMsg::default(),
            evaluation: ProviderEvaluationOptionsMsg::default(),
            result_delivery: ResultDeliveryMsg::Inline,
        }
    }

    fn run_backtest_multi_request() -> RunBacktestMultiRequest {
        RunBacktestMultiRequest {
            request: BacktestMultiRunSpec {
                symbol: "EURUSD".into(),
                symbols: Vec::new(),
                all_symbols: false,
                exchange: "fixture".into(),
                data_type: "tick".into(),
                timeframe: None,
                from: None,
                to: None,
                raw_signals: Vec::new(),
                profiles: vec![ProfileRef::Named("default".into())],
                config: BacktestConfigMsg {
                    initial_balance: None,
                    close_on_finish: None,
                    fill_model: None,
                    sizing: None,
                },
            },
            future: FutureQuoteConfigMsg::default(),
            evaluation: ProviderEvaluationOptionsMsg::default(),
            result_delivery: ResultDeliveryMsg::Inline,
        }
    }

    fn profile() -> ManagementProfileMsg {
        ManagementProfileMsg {
            name: "runtime".into(),
            target_selection: None,
            use_targets: Vec::new(),
            close_ratios: Vec::new(),
            stoploss_mode: None,
            rules: Vec::new(),
            group_override: None,
            let_remainder_run: true,
        }
    }

    fn registrar() -> Arc<impl qs_service_xrpc::XrpcServiceRegistrar<JsonCodec>> {
        Arc::new(
            |server: &RpcServer<JsonCodec>, _context: &ConnectionContext| {
                server.register_typed(
                    "get_result_artifact_chunk",
                    |request: GetResultArtifactChunkRequest| async move {
                        assert_eq!(request.artifact_id, "artifact-1");
                        assert_eq!(request.offset, 128);
                        Ok(GetResultArtifactChunkResponse {
                            success: true,
                            artifact_id: request.artifact_id,
                            offset: request.offset,
                            data_base64: "Y2h1bms=".into(),
                            eof: true,
                            error: None,
                        })
                    },
                );
                server.register_typed(
                    "delete_result_artifact",
                    |request: DeleteResultArtifactRequest| async move {
                        Ok(DeleteResultArtifactResponse {
                            success: true,
                            artifact_id: request.artifact_id,
                            error: None,
                        })
                    },
                );
                server.register_typed("run_backtest", |request: RunBacktestRequest| async move {
                    assert_eq!(request.request.symbol, "EURUSD");
                    Ok(RunBacktestResponse {
                        success: true,
                        error: None,
                        result: None,
                        elapsed_ms: 10,
                        artifact: None,
                        inline_complete: true,
                    })
                });
                server.register_typed(
                    "run_backtest_multi",
                    |request: RunBacktestMultiRequest| async move {
                        assert_eq!(request.request.profiles.len(), 1);
                        Ok(RunBacktestMultiResponse {
                            success: true,
                            error: None,
                            results: Vec::new(),
                            elapsed_ms: 20,
                            artifact: None,
                            inline_complete: true,
                        })
                    },
                );
                server.register_typed("list_profiles", |_: ()| async move {
                    Ok(ListProfilesResponse {
                        profiles: vec![ProfileInfo {
                            name: "default".into(),
                            use_targets: Vec::new(),
                            close_ratios: Vec::new(),
                            stoploss_mode: "None".into(),
                            rules_count: 0,
                            let_remainder_run: true,
                        }],
                    })
                });
                server.register_typed("list_symbols", |request: ListSymbolsRequest| async move {
                    assert_eq!(request.exchange.as_deref(), Some("fixture"));
                    assert_eq!(request.data_type.as_deref(), Some("tick"));
                    Ok(ListSymbolsResponse {
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
                server.register_typed("add_profile", |request: AddProfileRequest| async move {
                    assert_eq!(request.profile.name, "runtime");
                    assert!(request.overwrite);
                    Ok(AddProfileResponse {
                        success: true,
                        error: None,
                        profile_count: 2,
                    })
                });
                server.register_typed(
                    "remove_profile",
                    |request: RemoveProfileRequest| async move {
                        assert_eq!(request.name, "runtime");
                        Ok(RemoveProfileResponse {
                            success: true,
                            error: None,
                            profile_count: 1,
                        })
                    },
                );
                server.register_typed("reload_profiles", |_: ()| async move {
                    Err::<ReloadProfilesResponse, _>(RpcError::ServerError(
                        "reload rejected".into(),
                    ))
                });
            },
        )
    }

    #[tokio::test]
    async fn client_capabilities_use_canonical_rpc_contracts() {
        assert_all_capabilities::<BacktestXrpcClient>();

        let endpoint: ServiceEndpoint = "channel://backtest-api-contract".parse().unwrap();
        let (session, server_transport) =
            channel_pair(&endpoint, &XrpcTransportConfig::default(), JsonCodec).unwrap();
        let server = tokio::spawn(serve_transport(
            server_transport,
            JsonCodec,
            registrar(),
            ConnectionContext {
                client_id: 1,
                client_name: Some("backtest-api-contract".into()),
                endpoint,
            },
        ));
        let client = BacktestXrpcClient { session };

        let chunk = client
            .get_result_artifact_chunk(GetResultArtifactChunkRequest {
                artifact_id: "artifact-1".into(),
                offset: 128,
            })
            .await
            .unwrap();
        assert!(chunk.success);
        assert!(chunk.eof);

        let deleted = client
            .delete_result_artifact(DeleteResultArtifactRequest {
                artifact_id: "artifact-1".into(),
            })
            .await
            .unwrap();
        assert!(deleted.success);

        let run = client.run_backtest(run_backtest_request()).await.unwrap();
        assert_eq!(run.elapsed_ms, 10);
        let multi = client
            .run_backtest_multi(run_backtest_multi_request())
            .await
            .unwrap();
        assert_eq!(multi.elapsed_ms, 20);

        let profiles = client.list_profiles().await.unwrap();
        assert_eq!(profiles.profiles[0].name, "default");
        let symbols = client
            .list_symbols(ListSymbolsRequest {
                exchange: Some("fixture".into()),
                data_type: Some("tick".into()),
            })
            .await
            .unwrap();
        assert_eq!(symbols.symbols[0].symbol, "EURUSD");

        let added = client
            .add_profile(AddProfileRequest {
                profile: profile(),
                overwrite: true,
            })
            .await
            .unwrap();
        assert_eq!(added.profile_count, 2);
        let removed = client
            .remove_profile(RemoveProfileRequest {
                name: "runtime".into(),
            })
            .await
            .unwrap();
        assert_eq!(removed.profile_count, 1);

        let error = client.reload_profiles().await.unwrap_err();
        match error {
            BacktestClientError::Service(message) => {
                assert_eq!(message, "Server error: reload rejected")
            }
            other => panic!("expected service error, got {other:?}"),
        }

        let _ = client.close().await;
        let _ = server.await.unwrap();
    }
}
