use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use qs_backtest_api::BacktestResultMsg;

use tempfile::{Builder, NamedTempFile};
use thiserror::Error;

use crate::{
    BacktestResultDocument, OpenedResultFile, OutputConflictPolicy, OutputTarget, ResultFileFormat,
    ResultIoLimits, decode_result_reader,
};

pub enum ResultOutput<'a> {
    Document(&'a BacktestResultDocument),
    Legacy(&'a BacktestResultMsg),
}

pub struct StagedOutput {
    temporary: NamedTempFile,
    target: OutputTarget,

    byte_len: u64,
}

impl std::fmt::Debug for StagedOutput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StagedOutput")
            .field("temporary_path", &self.temporary.path())
            .field("target", &self.target)
            .field("byte_len", &self.byte_len)
            .finish()
    }
}

impl StagedOutput {
    pub fn byte_len(&self) -> u64 {
        self.byte_len
    }

    pub fn target(&self) -> &OutputTarget {
        &self.target
    }

    pub fn temporary_path(&self) -> &Path {
        self.temporary.path()
    }

    pub fn retarget(
        mut self,
        target: OutputTarget,
        limits: ResultIoLimits,
    ) -> Result<Self, OutputError> {
        validate_target_format(self.target.format, target.format)?;
        let current_parent = self
            .temporary
            .path()
            .parent()
            .unwrap_or_else(|| Path::new("."));
        let next_parent = target
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if current_parent != next_parent {
            fs::create_dir_all(next_parent).map_err(OutputError::create_parent)?;
            let mut replacement = Builder::new()
                .prefix(".qs-result-part-")
                .tempfile_in(next_parent)
                .map_err(OutputError::create_temp)?;
            let source = self.temporary.reopen().map_err(OutputError::reopen)?;
            let copied = std::io::copy(
                &mut source.take(limits.maximum_result_document_bytes.saturating_add(1)),
                replacement.as_file_mut(),
            )
            .map_err(OutputError::write)?;
            if copied != self.byte_len || copied > limits.maximum_result_document_bytes {
                return Err(OutputError::DocumentTooLarge {
                    actual: copied,
                    maximum: limits.maximum_result_document_bytes,
                });
            }
            replacement
                .as_file_mut()
                .flush()
                .map_err(OutputError::write)?;
            replacement
                .as_file_mut()
                .sync_all()
                .map_err(OutputError::sync)?;
            let opened = open_result_path(replacement.path(), limits)?;
            validate_opened_format(&opened, target.format)?;
            self.temporary = replacement;
        }
        self.target = target;
        Ok(self)
    }

    pub fn commit(self, limits: ResultIoLimits) -> Result<OutputCommit, OutputError> {
        let target_path = self.target.path.clone();

        let byte_len = self.byte_len;
        match self.target.conflict {
            OutputConflictPolicy::FailIfExists => {
                match self.temporary.persist_noclobber(&target_path) {
                    Ok(file) => {
                        file.sync_all().map_err(OutputError::sync)?;
                        best_effort_sync_parent(&target_path);
                        let opened = open_result_path(&target_path, limits)?;
                        validate_opened_format(&opened, self.target.format)?;
                        Ok(OutputCommit::Committed(CommittedOutput {
                            path: target_path,
                            byte_len,
                        }))
                    }
                    Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                        Ok(OutputCommit::Conflict(Self {
                            temporary: error.file,
                            target: self.target,

                            byte_len,
                        }))
                    }
                    Err(error) => Err(OutputError::Persist(error.error.to_string())),
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum OutputCommit {
    Committed(CommittedOutput),
    Conflict(StagedOutput),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOutput {
    pub path: PathBuf,

    pub byte_len: u64,
}

pub fn stage_output(
    target: OutputTarget,
    output: ResultOutput<'_>,
    limits: ResultIoLimits,
) -> Result<StagedOutput, OutputError> {
    limits.validate()?;
    match (&output, target.format) {
        (ResultOutput::Document(_), ResultFileFormat::DocumentV1)
        | (ResultOutput::Legacy(_), ResultFileFormat::LegacyBareResult) => {}
        _ => return Err(OutputError::FormatMismatch),
    }

    let parent = target
        .path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(OutputError::create_parent)?;
    let prefix = target
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| format!(".{name}.qs-part-"))
        .unwrap_or_else(|| ".qs-result-part-".into());
    let mut temporary = Builder::new()
        .prefix(&prefix)
        .tempfile_in(parent)
        .map_err(OutputError::create_temp)?;

    let byte_len = {
        let mut writer = LimitedOutputWriter {
            inner: temporary.as_file_mut(),
            count: 0,
            maximum: limits.maximum_result_document_bytes,
        };
        match output {
            ResultOutput::Document(document) => {
                serde_json::to_writer(&mut writer, document)
                    .map_err(|error| OutputError::Encode(error.to_string()))?;
            }
            ResultOutput::Legacy(result) => {
                serde_json::to_writer(&mut writer, result)
                    .map_err(|error| OutputError::Encode(error.to_string()))?;
            }
        }
        writer.flush().map_err(OutputError::write)?;
        writer.count
    };
    temporary
        .as_file_mut()
        .sync_all()
        .map_err(OutputError::sync)?;

    let mut reopened = temporary.reopen().map_err(OutputError::reopen)?;
    reopened
        .seek(SeekFrom::Start(0))
        .map_err(OutputError::reopen)?;
    let opened = decode_result_reader(
        BufReader::new(reopened),
        limits.maximum_result_document_bytes,
    )?;
    validate_opened_format(&opened, target.format)?;
    Ok(StagedOutput {
        temporary,
        target,
        byte_len,
    })
}

struct LimitedOutputWriter<W> {
    inner: W,
    count: u64,
    maximum: u64,
}

impl<W: Write> Write for LimitedOutputWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self.count.saturating_add(bytes.len() as u64);
        if next > self.maximum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "result output exceeds configured document limit",
            ));
        }
        let written = self.inner.write(bytes)?;
        self.count = self.count.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub fn open_result_path(
    path: &Path,
    limits: ResultIoLimits,
) -> Result<OpenedResultFile, OutputError> {
    limits.validate()?;
    let file = File::open(path).map_err(OutputError::open)?;
    decode_result_reader(BufReader::new(file), limits.maximum_result_document_bytes)
        .map_err(OutputError::from)
}

fn validate_opened_format(
    opened: &OpenedResultFile,
    expected: ResultFileFormat,
) -> Result<(), OutputError> {
    if matches!(
        (opened, expected),
        (OpenedResultFile::Document(_), ResultFileFormat::DocumentV1)
            | (
                OpenedResultFile::Legacy(_),
                ResultFileFormat::LegacyBareResult
            )
    ) {
        Ok(())
    } else {
        Err(OutputError::FormatMismatch)
    }
}

fn validate_target_format(
    current: ResultFileFormat,
    next: ResultFileFormat,
) -> Result<(), OutputError> {
    if current == next {
        Ok(())
    } else {
        Err(OutputError::FormatMismatch)
    }
}

fn best_effort_sync_parent(path: &Path) {
    if let Some(parent) = path.parent()
        && let Ok(directory) = File::open(parent)
    {
        let _ = directory.sync_all();
    }
}

#[derive(Debug, Error)]
pub enum OutputError {
    #[error(transparent)]
    ResultDocument(#[from] crate::ResultDocumentError),
    #[error("selected result and output formats do not match")]
    FormatMismatch,
    #[error("result document is {actual} bytes, exceeding the {maximum}-byte limit")]
    DocumentTooLarge { actual: u64, maximum: u64 },
    #[error("failed to create output parent directory: {0}")]
    CreateParent(String),
    #[error("failed to create same-directory temporary output: {0}")]
    CreateTemp(String),
    #[error("failed to encode result output: {0}")]
    Encode(String),
    #[error("failed to write result output: {0}")]
    Write(String),
    #[error("failed to synchronize result output: {0}")]
    Sync(String),

    #[error("failed to reopen result output: {0}")]
    Reopen(String),
    #[error("failed to persist result output without replacement: {0}")]
    Persist(String),
    #[error("failed to open result output: {0}")]
    Open(String),
}

impl OutputError {
    fn create_parent(error: std::io::Error) -> Self {
        Self::CreateParent(error.to_string())
    }

    fn create_temp(error: std::io::Error) -> Self {
        Self::CreateTemp(error.to_string())
    }

    fn write(error: std::io::Error) -> Self {
        Self::Write(error.to_string())
    }

    fn sync(error: std::io::Error) -> Self {
        Self::Sync(error.to_string())
    }

    fn reopen(error: std::io::Error) -> Self {
        Self::Reopen(error.to_string())
    }

    fn open(error: std::io::Error) -> Self {
        Self::Open(error.to_string())
    }
}
