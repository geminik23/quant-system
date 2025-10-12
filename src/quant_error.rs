use thiserror::Error;

#[derive(Debug, Error)]
pub enum QuantError {
    #[error("Symbol({0}) not founded")]
    SymbolNotFound(String),
    #[error(transparent)]
    CTraderError(#[from] cfix::types::Error),
    #[error("Channel send error")]
    Channel,
    #[error("Other error: {0}")]
    Other(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("TOML error: {0}")]
    TomlError(#[from] toml::de::Error),
    #[error("Send error: {0}")]
    SendError(String),
    #[error("gRPC status error: {0}")]
    GrpcStatus(#[from] tonic::Status),
}

pub type Result<T> = std::result::Result<T, QuantError>;
