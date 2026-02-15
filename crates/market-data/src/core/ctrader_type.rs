use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct CTraderFixConfig {
    pub username: String,
    pub password: String,
    pub server: String,
    pub sendercompid: String,
    pub ssl: bool,

    // Reconnection retry settings (all optional with defaults)
    pub retry_max_attempts: Option<u32>, // None or 0 = infinite retries
    pub retry_base_delay_secs: Option<u64>, // default: 2
    pub retry_max_delay_secs: Option<u64>, // default: 60
}

