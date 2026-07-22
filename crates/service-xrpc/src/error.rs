use qs_service::{RetryDisposition, ServiceEndpoint, TransportFailure, TransportFailureKind};
use thiserror::Error;
use xrpc::{RpcError, TransportError};

#[derive(Debug, Error)]
pub enum XrpcProviderError {
    #[error(transparent)]
    Transport(#[from] TransportFailure),
    #[error("remote service error: {0}")]
    Remote(String),
    #[error("service protocol error: {0}")]
    Protocol(String),
    #[error("xrpc client task failed: {0}")]
    ClientTask(String),
}

pub fn map_transport_error(
    error: TransportError,
    endpoint: Option<ServiceEndpoint>,
) -> TransportFailure {
    let (kind, retry) = match &error {
        TransportError::MessageTooLarge { .. } => (
            TransportFailureKind::MessageTooLarge,
            RetryDisposition::Never,
        ),
        TransportError::Protocol(_) | TransportError::InvalidBufferState(_) => {
            (TransportFailureKind::Protocol, RetryDisposition::Never)
        }
        TransportError::Timeout { operation, .. } => {
            let kind = if operation.contains("connect") {
                TransportFailureKind::ConnectTimeout
            } else if operation.contains("send") || operation.contains("write") {
                TransportFailureKind::WriteTimeout
            } else {
                TransportFailureKind::ReadTimeout
            };
            (kind, RetryDisposition::SafeBeforeInvocation)
        }
        TransportError::ConnectionClosed | TransportError::NotConnected => (
            TransportFailureKind::ConnectionClosed,
            RetryDisposition::RequiresApplicationReconciliation,
        ),
        TransportError::ConnectionFailed { .. } | TransportError::SharedMemoryCreation { .. } => (
            TransportFailureKind::Unavailable,
            RetryDisposition::SafeBeforeInvocation,
        ),
        TransportError::SendFailed { .. } | TransportError::ReceiveFailed { .. } => (
            TransportFailureKind::Unavailable,
            RetryDisposition::RequiresApplicationReconciliation,
        ),
        _ => (
            TransportFailureKind::Internal,
            RetryDisposition::RequiresApplicationReconciliation,
        ),
    };
    TransportFailure::new(kind, retry, endpoint, error.to_string())
}

pub fn map_rpc_error(error: RpcError, endpoint: Option<ServiceEndpoint>) -> XrpcProviderError {
    match error {
        RpcError::Transport(error) => map_transport_error(error, endpoint).into(),
        RpcError::ConnectionClosed => TransportFailure::new(
            TransportFailureKind::ConnectionClosed,
            RetryDisposition::RequiresApplicationReconciliation,
            endpoint,
            "xrpc connection closed",
        )
        .into(),
        RpcError::ServerError(message) => XrpcProviderError::Remote(message),
        RpcError::Serialization(message) => XrpcProviderError::Protocol(message),
        RpcError::InvalidMessage(message)
        | RpcError::MethodNotFound(message)
        | RpcError::Timeout(message)
        | RpcError::ClientError(message)
        | RpcError::StreamError(message) => XrpcProviderError::Protocol(message),
        _ => XrpcProviderError::Protocol(error.to_string()),
    }
}
