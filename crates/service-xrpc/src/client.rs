use std::sync::Arc;

use qs_service::{EndpointScheme, ServiceEndpoint, TransportFailure};
use serde::{Serialize, de::DeserializeOwned};
use xrpc::{
    CallOptions, ChannelConfig, ChannelFrameTransport, Codec, FrameTransport,
    MessageChannelAdapter, RpcClient, RpcClientHandle, SharedMemoryConfig,
    SharedMemoryFrameTransport, StreamReceiver, TcpConfig, TcpFrameTransport,
};

#[cfg(unix)]
use xrpc::{UnixConfig, UnixFrameTransport};

use crate::config::XrpcTransportConfig;
use crate::error::{XrpcProviderError, map_rpc_error, map_transport_error};
use crate::shared_memory::{ConnectRequest, ConnectResponse};

pub type DynFrameTransport = Box<dyn FrameTransport>;
pub type DynMessageChannel<C> = MessageChannelAdapter<DynFrameTransport, C>;
pub type DynRpcClient<C> = RpcClient<DynMessageChannel<C>, C>;

pub struct XrpcClientSession<C>
where
    C: Codec + Clone + Default + 'static,
{
    client: Arc<DynRpcClient<C>>,
    handle: Option<RpcClientHandle>,
    endpoint: ServiceEndpoint,
    logical_client_id: Option<usize>,
}

/// Create a started client session and its connected server transport for in-process hosting/tests.
pub fn channel_pair<C>(
    endpoint: &ServiceEndpoint,
    config: &XrpcTransportConfig,
    codec: C,
) -> Result<(XrpcClientSession<C>, DynFrameTransport), XrpcProviderError>
where
    C: Codec + Clone + Default + 'static,
{
    if endpoint.scheme() != &EndpointScheme::Channel {
        return Err(TransportFailure::new(
            qs_service::TransportFailureKind::InvalidConfiguration,
            qs_service::RetryDisposition::Never,
            Some(endpoint.clone()),
            "channel_pair requires a channel:// endpoint",
        )
        .into());
    }
    config.validate().map_err(|detail| {
        TransportFailure::new(
            qs_service::TransportFailureKind::InvalidConfiguration,
            qs_service::RetryDisposition::Never,
            Some(endpoint.clone()),
            detail,
        )
    })?;
    let mut channel_config = ChannelConfig::default().with_buffer_size(config.buffer_bytes);
    if let Some(timeout) = config.read_timeout {
        channel_config = channel_config.with_read_timeout(timeout);
    }
    if let Some(timeout) = config.write_timeout {
        channel_config = channel_config.with_write_timeout(timeout);
    }
    let (client, server) = ChannelFrameTransport::create_pair(endpoint.address(), channel_config)
        .map_err(|error| map_transport_error(error, Some(endpoint.clone())))?;
    let session = XrpcClientSession::start(Box::new(client), codec, endpoint.clone(), None)?;
    Ok((session, Box::new(server)))
}

impl<C> XrpcClientSession<C>
where
    C: Codec + Clone + Default + 'static,
{
    fn start(
        transport: DynFrameTransport,
        codec: C,
        endpoint: ServiceEndpoint,
        logical_client_id: Option<usize>,
    ) -> Result<Self, XrpcProviderError> {
        let channel = MessageChannelAdapter::<_, C>::with_codec(transport);
        let client = Arc::new(RpcClient::with_codec(channel, codec));
        let handle = client
            .try_start()
            .map_err(|error| map_rpc_error(error, Some(endpoint.clone())))?;
        Ok(Self {
            client,
            handle: Some(handle),
            endpoint,
            logical_client_id,
        })
    }

    pub fn endpoint(&self) -> &ServiceEndpoint {
        &self.endpoint
    }

    pub fn logical_client_id(&self) -> Option<usize> {
        self.logical_client_id
    }

    pub fn raw_client(&self) -> Arc<DynRpcClient<C>> {
        Arc::clone(&self.client)
    }

    pub async fn call<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
    ) -> Result<Resp, XrpcProviderError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        self.client
            .call(method, request)
            .await
            .map_err(|error| map_rpc_error(error, Some(self.endpoint.clone())))
    }

    pub async fn call_with_options<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
        options: CallOptions,
    ) -> Result<Resp, XrpcProviderError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        self.client
            .call_with_options(method, request, options)
            .await
            .map_err(|error| map_rpc_error(error, Some(self.endpoint.clone())))
    }

    pub async fn call_server_stream<Req, Resp>(
        &self,
        method: &str,
        request: &Req,
    ) -> Result<StreamReceiver<Resp, C>, XrpcProviderError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        self.client
            .call_server_stream(method, request)
            .await
            .map_err(|error| map_rpc_error(error, Some(self.endpoint.clone())))
    }

    pub async fn close(mut self) -> Result<(), XrpcProviderError> {
        let close_result = self
            .client
            .close()
            .await
            .map_err(|error| map_rpc_error(error, Some(self.endpoint.clone())));
        let join_result = match self.handle.take() {
            Some(handle) => handle
                .join()
                .await
                .map_err(|error| XrpcProviderError::ClientTask(error.to_string())),
            None => Ok(()),
        };
        match (close_result, join_result) {
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

pub async fn connect<C>(
    endpoint: &ServiceEndpoint,
    client_name: &str,
    config: &XrpcTransportConfig,
    codec: C,
) -> Result<XrpcClientSession<C>, XrpcProviderError>
where
    C: Codec + Clone + Default + 'static,
{
    config.validate().map_err(|detail| {
        TransportFailure::new(
            qs_service::TransportFailureKind::InvalidConfiguration,
            qs_service::RetryDisposition::Never,
            Some(endpoint.clone()),
            detail,
        )
    })?;
    if client_name.is_empty() || client_name.len() > 255 {
        return Err(TransportFailure::new(
            qs_service::TransportFailureKind::InvalidConfiguration,
            qs_service::RetryDisposition::Never,
            Some(endpoint.clone()),
            "client_name must contain 1 through 255 bytes",
        )
        .into());
    }

    match endpoint.scheme() {
        EndpointScheme::SharedMemory => {
            connect_shared_memory(endpoint, client_name, config, codec).await
        }
        EndpointScheme::Tcp => {
            if !endpoint.is_tcp_loopback() && !config.allow_insecure_non_loopback {
                return Err(TransportFailure::new(
                    qs_service::TransportFailureKind::PermissionDenied,
                    qs_service::RetryDisposition::Never,
                    Some(endpoint.clone()),
                    "non-loopback xrpc TCP requires allow_insecure_non_loopback=true",
                )
                .into());
            }
            let transport = TcpFrameTransport::connect(
                endpoint.socket_addr().map_err(|error| {
                    TransportFailure::new(
                        qs_service::TransportFailureKind::InvalidConfiguration,
                        qs_service::RetryDisposition::Never,
                        Some(endpoint.clone()),
                        error.to_string(),
                    )
                })?,
                tcp_config(config),
            )
            .await
            .map_err(|error| map_transport_error(error, Some(endpoint.clone())))?;
            XrpcClientSession::start(Box::new(transport), codec, endpoint.clone(), None)
        }
        EndpointScheme::Unix => {
            #[cfg(unix)]
            {
                let transport = UnixFrameTransport::connect(
                    endpoint.unix_path().map_err(|error| {
                        TransportFailure::new(
                            qs_service::TransportFailureKind::InvalidConfiguration,
                            qs_service::RetryDisposition::Never,
                            Some(endpoint.clone()),
                            error.to_string(),
                        )
                    })?,
                    unix_config(config),
                )
                .await
                .map_err(|error| map_transport_error(error, Some(endpoint.clone())))?;
                XrpcClientSession::start(Box::new(transport), codec, endpoint.clone(), None)
            }
            #[cfg(not(unix))]
            {
                Err(TransportFailure::new(
                    qs_service::TransportFailureKind::InvalidConfiguration,
                    qs_service::RetryDisposition::Never,
                    Some(endpoint.clone()),
                    "Unix endpoints are unavailable on this platform",
                )
                .into())
            }
        }
        EndpointScheme::Channel => Err(TransportFailure::new(
            qs_service::TransportFailureKind::InvalidConfiguration,
            qs_service::RetryDisposition::Never,
            Some(endpoint.clone()),
            "named Channel endpoints are test-only; use a connected transport pair",
        )
        .into()),
        EndpointScheme::Custom(scheme) => Err(TransportFailure::new(
            qs_service::TransportFailureKind::InvalidConfiguration,
            qs_service::RetryDisposition::Never,
            Some(endpoint.clone()),
            format!("no provider is registered for endpoint scheme '{scheme}'"),
        )
        .into()),
    }
}

async fn connect_shared_memory<C>(
    endpoint: &ServiceEndpoint,
    client_name: &str,
    config: &XrpcTransportConfig,
    codec: C,
) -> Result<XrpcClientSession<C>, XrpcProviderError>
where
    C: Codec + Clone + Default + 'static,
{
    let accept_name = format!("{}-accept", endpoint.address());
    let acceptor_transport = SharedMemoryFrameTransport::connect_client_with_config(
        &accept_name,
        shared_memory_config(config),
    )
    .map_err(|error| map_transport_error(error, Some(endpoint.clone())))?;
    let acceptor = XrpcClientSession::start(
        Box::new(acceptor_transport),
        codec.clone(),
        endpoint.clone(),
        None,
    )?;
    let response: ConnectResponse = acceptor
        .call(
            "connect",
            &ConnectRequest {
                client_name: client_name.to_string(),
            },
        )
        .await?;
    acceptor.close().await?;
    let (client_id, slot_name) = response.into_parts();
    let transport = SharedMemoryFrameTransport::connect_client_with_config(
        slot_name,
        shared_memory_config(config),
    )
    .map_err(|error| map_transport_error(error, Some(endpoint.clone())))?;
    XrpcClientSession::start(
        Box::new(transport),
        codec,
        endpoint.clone(),
        Some(client_id),
    )
}

pub(crate) fn shared_memory_config(config: &XrpcTransportConfig) -> SharedMemoryConfig {
    let mut value = SharedMemoryConfig::new()
        .with_buffer_size(config.buffer_bytes)
        .with_max_retries(config.maximum_retry_attempts);
    if let Some(timeout) = config.read_timeout {
        value = value.with_read_timeout(timeout);
    }
    if let Some(timeout) = config.write_timeout {
        value = value.with_write_timeout(timeout);
    }
    value
}

pub(crate) fn tcp_config(config: &XrpcTransportConfig) -> TcpConfig {
    TcpConfig::default()
        .with_max_message_size(config.maximum_message_bytes)
        .with_connect_timeout(config.connect_timeout)
        .with_read_timeout(config.read_timeout)
        .with_write_timeout(config.write_timeout)
        .with_nodelay(config.nodelay)
}

#[cfg(unix)]
pub(crate) fn unix_config(config: &XrpcTransportConfig) -> UnixConfig {
    UnixConfig::default()
        .with_max_message_size(config.maximum_message_bytes)
        .with_connect_timeout(config.connect_timeout)
        .with_read_timeout(config.read_timeout)
        .with_write_timeout(config.write_timeout)
}
