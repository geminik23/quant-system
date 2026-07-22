use std::time::Duration;

/// Runtime settings shared across xrpc frame transports.
#[derive(Debug, Clone)]
pub struct XrpcTransportConfig {
    pub buffer_bytes: usize,
    pub maximum_message_bytes: usize,
    pub maximum_connections: usize,
    pub connect_timeout: Duration,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub maximum_retry_attempts: usize,
    pub nodelay: bool,
    pub allow_insecure_non_loopback: bool,
}

impl Default for XrpcTransportConfig {
    fn default() -> Self {
        Self {
            buffer_bytes: 16 * 1024 * 1024,
            maximum_message_bytes: 16 * 1024 * 1024,
            maximum_connections: 256,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Some(Duration::from_secs(300)),
            write_timeout: Some(Duration::from_secs(30)),
            maximum_retry_attempts: 3,
            nodelay: true,
            allow_insecure_non_loopback: false,
        }
    }
}

impl XrpcTransportConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.buffer_bytes < 4096 {
            return Err("buffer_bytes must be at least 4096".to_string());
        }
        if self.maximum_message_bytes < 4096 {
            return Err("maximum_message_bytes must be at least 4096".to_string());
        }
        if self.maximum_connections == 0 {
            return Err("maximum_connections must be positive".to_string());
        }
        if self.maximum_retry_attempts == 0 {
            return Err("maximum_retry_attempts must be positive".to_string());
        }
        Ok(())
    }
}
