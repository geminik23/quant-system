use std::fs;
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::Path;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use qs_backtest_api::{
    BacktestResultMsg, GetResultArtifactChunkRequest, GetResultArtifactChunkResponse,
    RESULT_FORMAT_VERSION, ResultArtifactRefMsg,
};
use sha2::{Digest, Sha256};
use tempfile::{Builder, NamedTempFile};
use thiserror::Error;

use crate::{ResultDocumentError, ResultIoLimits, decode_result_bytes};

pub struct ArtifactDownload {
    reference: ResultArtifactRefMsg,
    temporary: NamedTempFile,
    offset: u64,
    hasher: Sha256,
    saw_eof: bool,
}

impl std::fmt::Debug for ArtifactDownload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArtifactDownload")
            .field("artifact_id", &self.reference.artifact_id)
            .field("offset", &self.offset)
            .field("saw_eof", &self.saw_eof)
            .finish_non_exhaustive()
    }
}

impl ArtifactDownload {
    pub fn start(
        reference: ResultArtifactRefMsg,
        directory: &Path,
        limits: ResultIoLimits,
    ) -> Result<Self, ArtifactError> {
        validate_reference(&reference, limits)?;
        fs::create_dir_all(directory).map_err(ArtifactError::io)?;
        let temporary = Builder::new()
            .prefix(".qs-artifact-")
            .tempfile_in(directory)
            .map_err(ArtifactError::io)?;
        Ok(Self {
            reference,
            temporary,
            offset: 0,
            hasher: Sha256::new(),
            saw_eof: false,
        })
    }

    pub fn artifact_id(&self) -> &str {
        &self.reference.artifact_id
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn byte_len(&self) -> u64 {
        self.reference.byte_len
    }

    pub fn next_request(&self) -> GetResultArtifactChunkRequest {
        GetResultArtifactChunkRequest {
            artifact_id: self.reference.artifact_id.clone(),
            offset: self.offset,
        }
    }

    pub fn accept(
        &mut self,
        response: GetResultArtifactChunkResponse,
    ) -> Result<bool, ArtifactError> {
        if self.saw_eof {
            return Err(ArtifactError::ChunkAfterEof);
        }
        if !response.success {
            return Err(ArtifactError::Service(
                response
                    .error
                    .unwrap_or_else(|| "artifact chunk request failed".into()),
            ));
        }
        if response.artifact_id != self.reference.artifact_id {
            return Err(ArtifactError::ArtifactIdMismatch {
                expected: self.reference.artifact_id.clone(),
                actual: response.artifact_id,
            });
        }
        if response.offset != self.offset {
            return Err(ArtifactError::OffsetMismatch {
                expected: self.offset,
                actual: response.offset,
            });
        }
        let maximum_encoded = self.reference.chunk_size.div_ceil(3).saturating_mul(4);
        if response.data_base64.len() as u64 > maximum_encoded {
            return Err(ArtifactError::EncodedChunkTooLarge {
                actual: response.data_base64.len() as u64,
                maximum: maximum_encoded,
            });
        }
        let bytes = BASE64_STANDARD
            .decode(response.data_base64.as_bytes())
            .map_err(|error| ArtifactError::InvalidBase64(error.to_string()))?;
        if BASE64_STANDARD.encode(&bytes) != response.data_base64 {
            return Err(ArtifactError::NonCanonicalBase64);
        }
        if bytes.len() as u64 > self.reference.chunk_size {
            return Err(ArtifactError::ChunkTooLarge {
                actual: bytes.len() as u64,
                maximum: self.reference.chunk_size,
            });
        }
        if bytes.is_empty() && !response.eof {
            return Err(ArtifactError::EmptyNonFinalChunk);
        }
        let next = self
            .offset
            .checked_add(bytes.len() as u64)
            .ok_or(ArtifactError::LengthOverflow)?;
        if next > self.reference.byte_len {
            return Err(ArtifactError::ArtifactTooLong {
                expected: self.reference.byte_len,
                actual: next,
            });
        }
        if response.eof && next != self.reference.byte_len {
            return Err(ArtifactError::EarlyEof {
                expected: self.reference.byte_len,
                actual: next,
            });
        }
        if !response.eof && next == self.reference.byte_len {
            return Err(ArtifactError::MissingEof);
        }
        self.temporary
            .as_file_mut()
            .write_all(&bytes)
            .map_err(ArtifactError::io)?;
        self.hasher.update(&bytes);
        self.offset = next;
        self.saw_eof = response.eof;
        Ok(response.eof)
    }

    pub fn finish(
        mut self,
        limits: ResultIoLimits,
    ) -> Result<VerifiedArtifactPayload, ArtifactError> {
        if !self.saw_eof {
            return Err(ArtifactError::MissingEof);
        }
        if self.offset != self.reference.byte_len {
            return Err(ArtifactError::EarlyEof {
                expected: self.reference.byte_len,
                actual: self.offset,
            });
        }
        self.temporary
            .as_file_mut()
            .flush()
            .map_err(ArtifactError::io)?;
        self.temporary
            .as_file_mut()
            .sync_all()
            .map_err(ArtifactError::io)?;
        let actual_sha256 = format!("{:x}", self.hasher.finalize());
        if actual_sha256 != self.reference.sha256 {
            return Err(ArtifactError::DigestMismatch {
                expected: self.reference.sha256,
                actual: actual_sha256,
            });
        }
        let mut file = self.temporary.reopen().map_err(ArtifactError::io)?;
        file.seek(SeekFrom::Start(0)).map_err(ArtifactError::io)?;
        let mut bounded = std::io::Read::take(
            BufReader::new(file),
            limits.maximum_decoded_payload_bytes.saturating_add(1),
        );
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut bounded, &mut bytes).map_err(ArtifactError::io)?;
        if bytes.len() as u64 > limits.maximum_decoded_payload_bytes {
            return Err(ArtifactError::PayloadTooLarge {
                maximum: limits.maximum_decoded_payload_bytes,
            });
        }
        let result = match decode_result_bytes(&bytes)? {
            crate::OpenedResultFile::Legacy(result) => *result,
            crate::OpenedResultFile::Document(_) => {
                return Err(ArtifactError::UnexpectedDocumentPayload);
            }
        };
        Ok(VerifiedArtifactPayload {
            temporary: self.temporary,
            result,

            byte_len: self.offset,
        })
    }
}

pub struct VerifiedArtifactPayload {
    temporary: NamedTempFile,
    pub result: BacktestResultMsg,

    pub byte_len: u64,
}

impl std::fmt::Debug for VerifiedArtifactPayload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedArtifactPayload")
            .field("temporary_path", &self.temporary.path())
            .field("byte_len", &self.byte_len)
            .finish_non_exhaustive()
    }
}

fn validate_reference(
    reference: &ResultArtifactRefMsg,
    limits: ResultIoLimits,
) -> Result<(), ArtifactError> {
    limits.validate()?;
    if reference.format_version != RESULT_FORMAT_VERSION {
        return Err(ArtifactError::UnsupportedFormatVersion {
            actual: reference.format_version,
        });
    }
    if reference.artifact_id.trim().is_empty() {
        return Err(ArtifactError::EmptyArtifactId);
    }
    if reference.byte_len > limits.maximum_artifact_bytes {
        return Err(ArtifactError::ArtifactTooLarge {
            actual: reference.byte_len,
            maximum: limits.maximum_artifact_bytes,
        });
    }
    if reference.chunk_size == 0 || reference.chunk_size > limits.maximum_artifact_chunk_bytes {
        return Err(ArtifactError::InvalidChunkSize {
            actual: reference.chunk_size,
            maximum: limits.maximum_artifact_chunk_bytes,
        });
    }
    if reference.sha256.len() != 64
        || !reference
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ArtifactError::InvalidDigest);
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error(transparent)]
    ResultDocument(#[from] ResultDocumentError),
    #[error("unsupported artifact format version {actual}")]
    UnsupportedFormatVersion { actual: u32 },
    #[error("artifact id must not be empty")]
    EmptyArtifactId,
    #[error("artifact is {actual} bytes, exceeding the {maximum}-byte limit")]
    ArtifactTooLarge { actual: u64, maximum: u64 },
    #[error("artifact chunk size {actual} is invalid or exceeds the {maximum}-byte limit")]
    InvalidChunkSize { actual: u64, maximum: u64 },
    #[error("artifact digest must be a canonical lowercase SHA-256 value")]
    InvalidDigest,
    #[error("artifact service failure: {0}")]
    Service(String),
    #[error("artifact id mismatch: expected {expected}, got {actual}")]
    ArtifactIdMismatch { expected: String, actual: String },
    #[error("artifact offset mismatch: expected {expected}, got {actual}")]
    OffsetMismatch { expected: u64, actual: u64 },
    #[error("encoded artifact chunk is {actual} bytes, exceeding the {maximum}-byte bound")]
    EncodedChunkTooLarge { actual: u64, maximum: u64 },
    #[error("artifact chunk is not canonical base64")]
    NonCanonicalBase64,
    #[error("invalid artifact base64: {0}")]
    InvalidBase64(String),
    #[error("artifact chunk is {actual} bytes, exceeding the advertised {maximum} bytes")]
    ChunkTooLarge { actual: u64, maximum: u64 },
    #[error("artifact returned an empty non-final chunk")]
    EmptyNonFinalChunk,
    #[error("artifact length overflow")]
    LengthOverflow,
    #[error("artifact exceeded its advertised length: expected {expected}, got {actual}")]
    ArtifactTooLong { expected: u64, actual: u64 },
    #[error("artifact ended early: expected {expected}, got {actual}")]
    EarlyEof { expected: u64, actual: u64 },
    #[error("artifact reached its advertised length without EOF")]
    MissingEof,
    #[error("artifact returned a chunk after EOF")]
    ChunkAfterEof,
    #[error("artifact digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("decoded artifact exceeds the {maximum}-byte limit")]
    PayloadTooLarge { maximum: u64 },
    #[error("result artifact unexpectedly contained a Result Document envelope")]
    UnexpectedDocumentPayload,
    #[error("artifact I/O error: {0}")]
    Io(String),
}

impl ArtifactError {
    fn io(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}
