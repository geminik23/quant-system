//! Server configuration loaded from TOML.

use qs_service::ServiceEndpoint;
use serde::Deserialize;

/// Top-level server configuration.
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub server: ServerSection,
    pub database: DatabaseSection,
    pub symbols: SymbolsSection,
    #[serde(default)]
    pub instruments: InstrumentsSection,
    pub profiles: ProfilesSection,
    #[serde(default)]
    pub jobs: JobsSection,
    #[serde(default)]
    pub artifacts: ArtifactsSection,
    #[serde(default)]
    pub logging: LoggingSection,
}

/// Service listener and transport settings.
#[derive(Debug, Deserialize)]
pub struct ServerSection {
    /// Provider-neutral listener endpoint. Defaults to the legacy `shm_name`.
    #[serde(default)]
    pub endpoint: Option<ServiceEndpoint>,
    /// Deprecated compatibility name for the shared-memory endpoint.
    #[serde(default)]
    pub shm_name: String,
    /// Per-client buffer or maximum frame size. Default: 16 MB.
    #[serde(default = "default_shm_buffer")]
    pub shm_buffer_size: usize,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default)]
    pub allow_insecure_non_loopback: bool,
}

impl ServerSection {
    pub fn resolved_endpoint(&self) -> crate::error::Result<ServiceEndpoint> {
        let legacy = if self.shm_name.is_empty() {
            None
        } else {
            Some(
                ServiceEndpoint::shared_memory(self.shm_name.clone()).map_err(|error| {
                    crate::error::BacktestServerError::Config(error.to_string())
                })?,
            )
        };
        match (&self.endpoint, legacy) {
            (Some(endpoint), Some(legacy)) if endpoint != &legacy => {
                Err(crate::error::BacktestServerError::Config(format!(
                    "conflicting server.endpoint '{}' and deprecated server.shm_name '{}'",
                    endpoint, self.shm_name
                )))
            }
            (Some(endpoint), _) => Ok(endpoint.clone()),
            (None, Some(endpoint)) => Ok(endpoint),
            (None, None) => Err(crate::error::BacktestServerError::Config(
                "server.endpoint is required (or provide deprecated server.shm_name)".to_string(),
            )),
        }
    }
}

/// Path to the Parquet data store root directory.
#[derive(Debug, Deserialize)]
pub struct DatabaseSection {
    /// Root directory for Parquet-partitioned market data.
    pub data_dir: String,
}

/// Path to the symbol registry TOML.
#[derive(Debug, Deserialize)]
pub struct SymbolsSection {
    pub registry_path: String,
}

/// Instrument catalog and stored-series identity settings.
#[derive(Debug, Deserialize)]
pub struct InstrumentsSection {
    /// Optional strict instrument catalog document. The symbol registry is adapted when omitted.
    #[serde(default)]
    pub catalog_path: Option<String>,
    /// Optional broker-, exchange-, or repository-owned alias-resolution default.
    #[serde(default)]
    pub default_listing_venue: Option<String>,
    /// Identity of the historical market-data source bound to current partitions.
    #[serde(default = "default_market_data_source")]
    pub market_data_source: String,
}

impl Default for InstrumentsSection {
    fn default() -> Self {
        Self {
            catalog_path: None,
            default_listing_venue: None,
            market_data_source: default_market_data_source(),
        }
    }
}

/// Path to the management profiles TOML.
#[derive(Debug, Deserialize)]
pub struct ProfilesSection {
    pub profiles_path: String,
}

/// Async job retention and cleanup settings.
#[derive(Debug, Deserialize)]
pub struct JobsSection {
    /// Retain completed, failed, and cancelled jobs for this many seconds.
    #[serde(default = "default_job_retention_secs")]
    pub retention_secs: u64,
    /// Run terminal-job cleanup at this interval.
    #[serde(default = "default_job_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,
    /// Maximum queued, running, and retained terminal jobs.
    #[serde(default = "default_max_retained_jobs")]
    pub max_retained_jobs: usize,
}

impl Default for JobsSection {
    fn default() -> Self {
        Self {
            retention_secs: default_job_retention_secs(),
            cleanup_interval_secs: default_job_cleanup_interval_secs(),
            max_retained_jobs: default_max_retained_jobs(),
        }
    }
}

/// Large-result artifact storage settings.
#[derive(Debug, Deserialize)]
pub struct ArtifactsSection {
    /// Directory containing result artifact JSON files.
    #[serde(default = "default_artifact_directory")]
    pub directory: String,
    /// Largest result JSON payload that can be returned inline.
    #[serde(default = "default_artifact_inline_limit_bytes")]
    pub inline_limit_bytes: usize,
    /// Raw bytes returned by each artifact chunk RPC.
    #[serde(default = "default_artifact_chunk_size")]
    pub chunk_size: usize,
    /// Retain artifacts for this many seconds.
    #[serde(default = "default_artifact_retention_secs")]
    pub retention_secs: u64,
    /// Maximum total bytes retained by the artifact store.
    #[serde(default = "default_artifact_max_total_bytes")]
    pub max_total_bytes: u64,
}

impl Default for ArtifactsSection {
    fn default() -> Self {
        Self {
            directory: default_artifact_directory(),
            inline_limit_bytes: default_artifact_inline_limit_bytes(),
            chunk_size: default_artifact_chunk_size(),
            retention_secs: default_artifact_retention_secs(),
            max_total_bytes: default_artifact_max_total_bytes(),
        }
    }
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

fn default_market_data_source() -> String {
    "local-parquet".into()
}

fn default_shm_buffer() -> usize {
    16 * 1024 * 1024
}

fn default_max_connections() -> usize {
    256
}

fn default_job_retention_secs() -> u64 {
    3_600
}

fn default_job_cleanup_interval_secs() -> u64 {
    60
}

fn default_max_retained_jobs() -> usize {
    1_000
}

fn default_artifact_directory() -> String {
    "backtest-artifacts".into()
}

fn default_artifact_inline_limit_bytes() -> usize {
    12 * 1024 * 1024
}

fn default_artifact_chunk_size() -> usize {
    1024 * 1024
}

fn default_artifact_retention_secs() -> u64 {
    3_600
}

fn default_artifact_max_total_bytes() -> u64 {
    1024 * 1024 * 1024
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
