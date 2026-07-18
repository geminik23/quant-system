//! Filesystem-backed storage for large backtest result JSON artifacts.

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use nanoid::nanoid;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::rpc_types::{RESULT_FORMAT_VERSION, ResultArtifactRefMsg};

const ARTIFACT_EXTENSION: &str = "json";
const PART_EXTENSION: &str = "part";
const TRANSPORT_LIMIT_BYTES: usize = 16 * 1024 * 1024;
const TRANSPORT_RESPONSE_RESERVE_BYTES: usize = 64 * 1024;

/// One raw artifact chunk read from disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactChunk {
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub eof: bool,
}

/// Errors returned by the artifact store.
#[derive(Debug, Error)]
pub enum ArtifactStoreError {
    #[error("invalid artifact id")]
    InvalidId,
    #[error("artifact '{0}' was not found")]
    NotFound(String),
    #[error("artifact path escaped the configured store directory")]
    PathEscape,
    #[error("artifact offset {offset} exceeds byte length {byte_len}")]
    InvalidOffset { offset: u64, byte_len: u64 },
    #[error("artifact is {byte_len} bytes but the store capacity is {max_total_bytes} bytes")]
    CapacityExceeded { byte_len: u64, max_total_bytes: u64 },
    #[error(
        "artifact chunk size {chunk_size} cannot fit in the 16 MiB transport after base64 encoding"
    )]
    ChunkTooLarge { chunk_size: usize },
    #[error("artifact store I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Server-owned filesystem artifact store.
#[derive(Debug)]
pub struct ArtifactStore {
    root: PathBuf,
    inline_limit_bytes: usize,
    chunk_size: usize,
    retention: Duration,
    max_total_bytes: u64,
    lock: Mutex<()>,
}

impl ArtifactStore {
    pub fn new(
        directory: impl AsRef<Path>,
        inline_limit_bytes: usize,
        chunk_size: usize,
        retention: Duration,
        max_total_bytes: u64,
    ) -> Result<Self, ArtifactStoreError> {
        let encoded_chunk_len = chunk_size.div_ceil(3).saturating_mul(4);
        if chunk_size == 0
            || encoded_chunk_len
                > TRANSPORT_LIMIT_BYTES.saturating_sub(TRANSPORT_RESPONSE_RESERVE_BYTES)
        {
            return Err(ArtifactStoreError::ChunkTooLarge { chunk_size });
        }

        fs::create_dir_all(directory.as_ref())?;
        let root = fs::canonicalize(directory.as_ref())?;
        let store = Self {
            root,
            inline_limit_bytes,
            chunk_size,
            retention,
            max_total_bytes,
            lock: Mutex::new(()),
        };
        store.cleanup_expired()?;
        Ok(store)
    }

    pub fn inline_limit_bytes(&self) -> usize {
        self.inline_limit_bytes
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn persist_json(&self, bytes: &[u8]) -> Result<ResultArtifactRefMsg, ArtifactStoreError> {
        let byte_len = bytes.len() as u64;
        if byte_len > self.max_total_bytes {
            return Err(ArtifactStoreError::CapacityExceeded {
                byte_len,
                max_total_bytes: self.max_total_bytes,
            });
        }

        let _guard = self.lock.lock().unwrap_or_else(|error| error.into_inner());
        self.cleanup_expired_locked(SystemTime::now())?;
        self.reserve_capacity_locked(byte_len)?;

        let (artifact_id, part_path, final_path, mut file) = loop {
            let artifact_id = format!("result_{}", nanoid!(24));
            let part_path = self.path_for_id_with_extension(&artifact_id, PART_EXTENSION)?;
            let final_path = self.path_for_id_with_extension(&artifact_id, ARTIFACT_EXTENSION)?;
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&part_path)
            {
                Ok(file) => break (artifact_id, part_path, final_path, file),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        };

        let write_result = (|| -> std::io::Result<()> {
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&part_path, &final_path)
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&part_path);
            return Err(error.into());
        }

        Ok(ResultArtifactRefMsg {
            format_version: RESULT_FORMAT_VERSION,
            artifact_id,
            byte_len,
            sha256: sha256_hex(bytes),
            chunk_size: self.chunk_size as u64,
        })
    }

    pub fn read_chunk(
        &self,
        artifact_id: &str,
        offset: u64,
    ) -> Result<ArtifactChunk, ArtifactStoreError> {
        let _guard = self.lock.lock().unwrap_or_else(|error| error.into_inner());
        let path = self.existing_artifact_path(artifact_id)?;
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let byte_len = file.metadata()?.len();
        if offset > byte_len {
            return Err(ArtifactStoreError::InvalidOffset { offset, byte_len });
        }

        let read_len = (byte_len - offset).min(self.chunk_size as u64) as usize;
        let mut bytes = vec![0; read_len];
        file.seek(SeekFrom::Start(offset))?;
        if read_len > 0 {
            file.read_exact(&mut bytes)?;
        }
        file.set_modified(SystemTime::now())?;
        Ok(ArtifactChunk {
            offset,
            eof: offset + bytes.len() as u64 >= byte_len,
            bytes,
        })
    }

    pub fn delete(&self, artifact_id: &str) -> Result<bool, ArtifactStoreError> {
        let _guard = self.lock.lock().unwrap_or_else(|error| error.into_inner());
        match self.existing_artifact_path(artifact_id) {
            Ok(path) => {
                fs::remove_file(path)?;
                Ok(true)
            }
            Err(ArtifactStoreError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn cleanup_expired(&self) -> Result<usize, ArtifactStoreError> {
        let _guard = self.lock.lock().unwrap_or_else(|error| error.into_inner());
        self.cleanup_expired_locked(SystemTime::now())
    }

    fn reserve_capacity_locked(&self, incoming: u64) -> Result<(), ArtifactStoreError> {
        let total = self.artifact_bytes_locked()?;
        if total.saturating_add(incoming) > self.max_total_bytes {
            return Err(ArtifactStoreError::CapacityExceeded {
                byte_len: incoming,
                max_total_bytes: self.max_total_bytes,
            });
        }
        Ok(())
    }

    fn cleanup_expired_locked(&self, now: SystemTime) -> Result<usize, ArtifactStoreError> {
        let mut removed = 0;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.file_type().is_file() {
                continue;
            }
            let extension = path.extension().and_then(|value| value.to_str());
            if extension != Some(ARTIFACT_EXTENSION) && extension != Some(PART_EXTENSION) {
                continue;
            }
            let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            let age = now.duration_since(modified).unwrap_or_default();
            if age >= self.retention {
                fs::remove_file(path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn artifact_bytes_locked(&self) -> Result<u64, ArtifactStoreError> {
        let mut total = 0_u64;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some(ARTIFACT_EXTENSION) {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_file() {
                total = total.saturating_add(metadata.len());
            }
        }
        Ok(total)
    }

    fn existing_artifact_path(&self, artifact_id: &str) -> Result<PathBuf, ArtifactStoreError> {
        let path = self.path_for_id_with_extension(artifact_id, ARTIFACT_EXTENSION)?;
        let canonical = match fs::canonicalize(&path) {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ArtifactStoreError::NotFound(artifact_id.to_owned()));
            }
            Err(error) => return Err(error.into()),
        };
        if canonical.parent() != Some(self.root.as_path()) {
            return Err(ArtifactStoreError::PathEscape);
        }
        if !fs::symlink_metadata(&canonical)?.file_type().is_file() {
            return Err(ArtifactStoreError::NotFound(artifact_id.to_owned()));
        }
        Ok(canonical)
    }

    fn path_for_id_with_extension(
        &self,
        artifact_id: &str,
        extension: &str,
    ) -> Result<PathBuf, ArtifactStoreError> {
        validate_artifact_id(artifact_id)?;
        Ok(self.root.join(format!("{artifact_id}.{extension}")))
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validate_artifact_id(artifact_id: &str) -> Result<(), ArtifactStoreError> {
    if artifact_id.is_empty()
        || artifact_id.len() > 128
        || !artifact_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(ArtifactStoreError::InvalidId);
    }
    Ok(())
}
