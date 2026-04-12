/// Signal parser error types.
#[derive(Debug, thiserror::Error)]
pub enum SignalParserError {
    #[error("Failed to parse timestamp '{0}': {1}")]
    TimestampParse(String, String),

    #[error("Parser '{parser}' failed on msg_id {msg_id}: {reason}")]
    ParseError {
        parser: String,
        msg_id: i64,
        reason: String,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Config error: {0}")]
    Config(String),
}
