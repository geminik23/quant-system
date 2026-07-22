use thiserror::Error;

use crate::ServiceEndpoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDisposition {
    Never,
    SafeBeforeInvocation,
    RequiresApplicationReconciliation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFailureKind {
    InvalidConfiguration,
    Unavailable,
    ConnectTimeout,
    ReadTimeout,
    WriteTimeout,
    ConnectionClosed,
    MessageTooLarge,
    Codec,
    Protocol,
    AuthenticationRequired,
    PermissionDenied,
    Internal,
}

#[derive(Debug, Clone, Error)]
#[error("{kind:?} at {endpoint_display}: {detail}")]
pub struct TransportFailure {
    pub kind: TransportFailureKind,
    pub retry: RetryDisposition,
    pub endpoint: Option<ServiceEndpoint>,
    pub detail: String,
    endpoint_display: String,
}

impl TransportFailure {
    pub fn new(
        kind: TransportFailureKind,
        retry: RetryDisposition,
        endpoint: Option<ServiceEndpoint>,
        detail: impl Into<String>,
    ) -> Self {
        let endpoint_display = endpoint
            .as_ref()
            .map(ServiceEndpoint::redacted)
            .unwrap_or_else(|| "<unknown endpoint>".to_string());
        Self {
            kind,
            retry,
            endpoint,
            detail: detail.into(),
            endpoint_display,
        }
    }
}
