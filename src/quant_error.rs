use thiserror::Error;

#[derive(Debug, Error)]
pub enum QuantError {
    #[error("RPC server error: {0}")]
    Rpc(#[from] mrpc::RpcError),
    #[error("Deserialization error: {0}")]
    Serde(#[from] rmp_serde::decode::Error),
    #[error("Channel send error")]
    Channel,
    #[error("Other error: {0}")]
    Other(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Msgpack decode error : {0}")]
    MsgpackDecodeError(#[from] rmpv::decode::Error),
    #[error("Msgpack encode error: {0}")]
    MsgpackEncodeError(#[from] rmpv::encode::Error),
    #[error("Send error: {0}")]
    SendError(String),

}


pub type Result<T> = std::result::Result<T, QuantError>;
