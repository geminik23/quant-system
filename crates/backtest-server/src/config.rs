//! Server configuration loaded from TOML.

use serde::Deserialize;

/// Top-level server configuration.
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub server: ServerSection,
    pub database: DatabaseSection,
    pub symbols: SymbolsSection,
    pub profiles: ProfilesSection,
    #[serde(default)]
    pub logging: LoggingSection,
}

/// SHM transport settings.
#[derive(Debug, Deserialize)]
pub struct ServerSection {
    /// Base name for shared memory endpoints (e.g. "backtest").
    pub shm_name: String,
    /// Per-client SHM buffer size in bytes. Default: 16 MB.
    #[serde(default = "default_shm_buffer")]
    pub shm_buffer_size: usize,
}

/// Path to the Parquet data store root directory.
#[derive(Debug, Deserialize)]
pub struct DatabaseSection {
    /// Root directory for Parquet-partitioned market data.
    pub data_dir: String,
}

/// Path to the symbol registry TOML (F06).
#[derive(Debug, Deserialize)]
pub struct SymbolsSection {
    pub registry_path: String,
}

/// Path to the management profiles TOML (F09).
#[derive(Debug, Deserialize)]
pub struct ProfilesSection {
    pub profiles_path: String,
}

/// Logging configuration with sensible defaults.
#[derive(Debug, Deserialize)]
pub struct LoggingSection {
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LoggingSection {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

fn default_shm_buffer() -> usize {
    16 * 1024 * 1024
}

fn default_log_level() -> String {
    "info".into()
}

/// Load and parse a TOML config file into `ServerConfig`.
pub fn load_config(path: &str) -> crate::error::Result<ServerConfig> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| crate::error::BacktestServerError::Config(format!("{path}: {e}")))?;
    toml::from_str(&content)
        .map_err(|e| crate::error::BacktestServerError::Config(format!("{path}: {e}")))
}
