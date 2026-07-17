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
    pub jobs: JobsSection,
    #[serde(default)]
    pub artifacts: ArtifactsSection,
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

fn default_shm_buffer() -> usize {
    16 * 1024 * 1024
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
    "temp/backtest-artifacts".into()
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
