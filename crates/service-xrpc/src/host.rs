use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use qs_service::{EndpointScheme, ServiceEndpoint, TransportFailure};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use xrpc::{
    Codec, FrameTransport, MessageChannelAdapter, RpcServer, SharedMemoryConfig,
    SharedMemoryFrameTransport, TcpFrameTransportListener,
};

#[cfg(unix)]
use xrpc::UnixFrameTransportListener;

#[cfg(unix)]
use crate::client::unix_config;
use crate::client::{shared_memory_config, tcp_config};
use crate::config::XrpcTransportConfig;
use crate::error::{XrpcProviderError, map_rpc_error, map_transport_error};
use crate::shared_memory::{ConnectRequest, ConnectResponse, cleanup_owned_shared_memory};

#[derive(Debug, Clone)]
pub struct ConnectionContext {
    pub client_id: usize,
    pub client_name: Option<String>,
    pub endpoint: ServiceEndpoint,
}

pub trait XrpcServiceRegistrar<C>: Send + Sync + 'static
where
    C: Codec + Clone + Default + 'static,
{
    fn register(&self, server: &RpcServer<C>, context: &ConnectionContext);
}

impl<C, F> XrpcServiceRegistrar<C> for F
where
    C: Codec + Clone + Default + 'static,
    F: Fn(&RpcServer<C>, &ConnectionContext) + Send + Sync + 'static,
{
    fn register(&self, server: &RpcServer<C>, context: &ConnectionContext) {
        self(server, context);
    }
}

pub async fn serve_transport<C, T, R>(
    transport: T,
    codec: C,
    registrar: Arc<R>,
    context: ConnectionContext,
) -> Result<(), XrpcProviderError>
where
    C: Codec + Clone + Default + 'static,
    T: FrameTransport + 'static,
    R: XrpcServiceRegistrar<C>,
{
    let channel = Arc::new(MessageChannelAdapter::<_, C>::with_codec(transport));
    let server = RpcServer::with_codec(codec);
    registrar.register(&server, &context);
    server
        .serve(channel)
        .await
        .map_err(|error| map_rpc_error(error, Some(context.endpoint)))
}

pub async fn serve_host<C, R>(
    endpoint: ServiceEndpoint,
    config: XrpcTransportConfig,
    codec: C,
    registrar: Arc<R>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), XrpcProviderError>
where
    C: Codec + Clone + Default + 'static,
    R: XrpcServiceRegistrar<C>,
{
    config.validate().map_err(|detail| {
        TransportFailure::new(
            qs_service::TransportFailureKind::InvalidConfiguration,
            qs_service::RetryDisposition::Never,
            Some(endpoint.clone()),
            detail,
        )
    })?;
    match endpoint.scheme().clone() {
        EndpointScheme::SharedMemory => {
            serve_shared_memory(endpoint, config, codec, registrar, shutdown).await
        }
        EndpointScheme::Tcp => serve_tcp(endpoint, config, codec, registrar, shutdown).await,
        EndpointScheme::Unix => {
            #[cfg(unix)]
            {
                serve_unix(endpoint, config, codec, registrar, shutdown).await
            }
            #[cfg(not(unix))]
            {
                Err(TransportFailure::new(
                    qs_service::TransportFailureKind::InvalidConfiguration,
                    qs_service::RetryDisposition::Never,
                    Some(endpoint),
                    "Unix endpoints are unavailable on this platform",
                )
                .into())
            }
        }
        EndpointScheme::Channel => Err(TransportFailure::new(
            qs_service::TransportFailureKind::InvalidConfiguration,
            qs_service::RetryDisposition::Never,
            Some(endpoint),
            "Channel service hosts require an explicit connected transport pair",
        )
        .into()),
        EndpointScheme::Custom(scheme) => Err(TransportFailure::new(
            qs_service::TransportFailureKind::InvalidConfiguration,
            qs_service::RetryDisposition::Never,
            Some(endpoint),
            format!("no provider is registered for endpoint scheme '{scheme}'"),
        )
        .into()),
    }
}

async fn serve_shared_memory<C, R>(
    endpoint: ServiceEndpoint,
    config: XrpcTransportConfig,
    codec: C,
    registrar: Arc<R>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), XrpcProviderError>
where
    C: Codec + Clone + Default + 'static,
    R: XrpcServiceRegistrar<C>,
{
    let base = endpoint.address().to_string();
    cleanup_owned_shared_memory(&base).map_err(|error| {
        TransportFailure::new(
            qs_service::TransportFailureKind::Unavailable,
            qs_service::RetryDisposition::SafeBeforeInvocation,
            Some(endpoint.clone()),
            format!("failed to clean owned SHM resources: {error}"),
        )
    })?;
    let client_sequence = Arc::new(AtomicUsize::new(0));
    let semaphore = Arc::new(Semaphore::new(config.maximum_connections));
    let mut clients: Vec<JoinHandle<()>> = Vec::new();
    let acceptor_config = SharedMemoryConfig::new()
        .with_buffer_size(64 * 1024)
        .with_read_timeout(Duration::from_secs(2))
        .with_write_timeout(config.write_timeout.unwrap_or(Duration::from_secs(30)));
    let accept_name = format!("{base}-accept");

    loop {
        clients.retain(|handle| !handle.is_finished());
        if *shutdown.borrow() {
            break;
        }
        let acceptor_transport = match SharedMemoryFrameTransport::create_server(
            &accept_name,
            acceptor_config.clone(),
        ) {
            Ok(transport) => transport,
            Err(error) => {
                tracing::warn!(error = %error, endpoint = %endpoint.redacted(), "SHM acceptor creation failed; retrying");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            }
        };
        let server = RpcServer::with_codec(codec.clone());
        let spawned = Arc::new(tokio::sync::Mutex::new(Vec::<JoinHandle<()>>::new()));
        let spawned_handler = Arc::clone(&spawned);
        let registrar_handler = Arc::clone(&registrar);
        let sequence_handler = Arc::clone(&client_sequence);
        let semaphore_handler = Arc::clone(&semaphore);
        let endpoint_handler = endpoint.clone();
        let config_handler = config.clone();
        let codec_handler = codec.clone();
        let base_handler = base.clone();
        server.register_typed("connect", move |request: ConnectRequest| {
            let spawned = Arc::clone(&spawned_handler);
            let registrar = Arc::clone(&registrar_handler);
            let sequence = Arc::clone(&sequence_handler);
            let semaphore = Arc::clone(&semaphore_handler);
            let endpoint = endpoint_handler.clone();
            let config = config_handler.clone();
            let codec = codec_handler.clone();
            let base = base_handler.clone();
            async move {
                if request.client_name.is_empty() || request.client_name.len() > 255 {
                    return Err(xrpc::RpcError::ServerError(
                        "client_name must contain 1 through 255 bytes".to_string(),
                    ));
                }
                let permit = semaphore.try_acquire_owned().map_err(|_| {
                    xrpc::RpcError::ServerError("service connection limit reached".to_string())
                })?;
                let client_id = sequence.fetch_add(1, Ordering::SeqCst) + 1;
                let slot_name = format!("{base}-client-{client_id}");
                let transport = SharedMemoryFrameTransport::create_server(
                    &slot_name,
                    shared_memory_config(&config),
                )
                .map_err(xrpc::RpcError::from)?;
                let context = ConnectionContext {
                    client_id,
                    client_name: Some(request.client_name),
                    endpoint,
                };
                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = serve_transport(transport, codec, registrar, context).await
                    {
                        tracing::debug!(error = %error, "xrpc SHM client session ended");
                    }
                });
                spawned.lock().await.push(handle);
                Ok(ConnectResponse::new(client_id, slot_name))
            }
        });
        let acceptor_channel = Arc::new(MessageChannelAdapter::<_, C>::with_codec(
            acceptor_transport,
        ));
        tokio::select! {
            result = server.serve(acceptor_channel) => {
                if let Err(error) = result {
                    tracing::debug!(error = %error, "xrpc SHM acceptor session ended");
                }
            }
            _ = shutdown.changed() => {}
        }
        let mut spawned = spawned.lock().await;
        clients.append(&mut *spawned);
    }

    for handle in clients {
        handle.abort();
        let _ = handle.await;
    }
    let _ = cleanup_owned_shared_memory(&base);
    Ok(())
}

async fn serve_tcp<C, R>(
    endpoint: ServiceEndpoint,
    config: XrpcTransportConfig,
    codec: C,
    registrar: Arc<R>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), XrpcProviderError>
where
    C: Codec + Clone + Default + 'static,
    R: XrpcServiceRegistrar<C>,
{
    if !endpoint.is_tcp_loopback() && !config.allow_insecure_non_loopback {
        return Err(TransportFailure::new(
            qs_service::TransportFailureKind::PermissionDenied,
            qs_service::RetryDisposition::Never,
            Some(endpoint),
            "non-loopback xrpc TCP requires allow_insecure_non_loopback=true",
        )
        .into());
    }
    let listener = TcpFrameTransportListener::bind(
        endpoint.socket_addr().map_err(|error| {
            TransportFailure::new(
                qs_service::TransportFailureKind::InvalidConfiguration,
                qs_service::RetryDisposition::Never,
                Some(endpoint.clone()),
                error.to_string(),
            )
        })?,
        tcp_config(&config),
    )
    .await
    .map_err(|error| map_transport_error(error, Some(endpoint.clone())))?;
    serve_direct_listener(
        endpoint,
        config.maximum_connections,
        codec,
        registrar,
        &mut shutdown,
        || listener.accept(),
    )
    .await
}

#[cfg(unix)]
async fn serve_unix<C, R>(
    endpoint: ServiceEndpoint,
    config: XrpcTransportConfig,
    codec: C,
    registrar: Arc<R>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), XrpcProviderError>
where
    C: Codec + Clone + Default + 'static,
    R: XrpcServiceRegistrar<C>,
{
    let listener = UnixFrameTransportListener::bind(
        endpoint.unix_path().map_err(|error| {
            TransportFailure::new(
                qs_service::TransportFailureKind::InvalidConfiguration,
                qs_service::RetryDisposition::Never,
                Some(endpoint.clone()),
                error.to_string(),
            )
        })?,
        unix_config(&config),
    )
    .await
    .map_err(|error| map_transport_error(error, Some(endpoint.clone())))?;
    serve_direct_listener(
        endpoint,
        config.maximum_connections,
        codec,
        registrar,
        &mut shutdown,
        || listener.accept(),
    )
    .await
}

async fn serve_direct_listener<C, R, T, Fut, A>(
    endpoint: ServiceEndpoint,
    maximum_connections: usize,
    codec: C,
    registrar: Arc<R>,
    shutdown: &mut watch::Receiver<bool>,
    mut accept: A,
) -> Result<(), XrpcProviderError>
where
    C: Codec + Clone + Default + 'static,
    R: XrpcServiceRegistrar<C>,
    T: FrameTransport + 'static,
    Fut: std::future::Future<Output = xrpc::TransportResult<T>>,
    A: FnMut() -> Fut,
{
    let semaphore = Arc::new(Semaphore::new(maximum_connections));
    let sequence = AtomicUsize::new(0);
    let mut clients: Vec<JoinHandle<()>> = Vec::new();
    loop {
        clients.retain(|handle| !handle.is_finished());
        tokio::select! {
            result = accept() => {
                let transport = result.map_err(|error| map_transport_error(error, Some(endpoint.clone())))?;
                let permit = match Arc::clone(&semaphore).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        let _ = transport.close().await;
                        continue;
                    }
                };
                let context = ConnectionContext {
                    client_id: sequence.fetch_add(1, Ordering::SeqCst) + 1,
                    client_name: None,
                    endpoint: endpoint.clone(),
                };
                let codec = codec.clone();
                let registrar = Arc::clone(&registrar);
                clients.push(tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = serve_transport(transport, codec, registrar, context).await {
                        tracing::debug!(error = %error, "xrpc direct client session ended");
                    }
                }));
            }
            _ = shutdown.changed() => break,
        }
    }
    for handle in clients {
        handle.abort();
        let _ = handle.await;
    }
    Ok(())
}
