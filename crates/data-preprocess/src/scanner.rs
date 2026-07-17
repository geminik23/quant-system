//! Streaming cursors for ascending Parquet tick and bar partitions.

use std::collections::VecDeque;
use std::fs::{self, File, Metadata};
use std::io::{Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;

use crate::convert::{dataframe_to_bars, dataframe_to_ticks};
use crate::error::{DataError, Result};
use crate::models::{Bar, Tick};
use crate::parquet_store::ParquetStore;

/// Default maximum number of rows decoded by one cursor read.
pub const DEFAULT_PARQUET_SCAN_ROWS: usize = 65_536;

/// Inclusive timestamp bounds for a Parquet cursor.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ParquetScanBounds {
    pub from: Option<NaiveDateTime>,
    pub to: Option<NaiveDateTime>,
}

impl ParquetScanBounds {
    pub const fn new(from: Option<NaiveDateTime>, to: Option<NaiveDateTime>) -> Self {
        Self { from, to }
    }

    fn validate(self) -> Result<Self> {
        if let (Some(from), Some(to)) = (self.from, self.to)
            && from > to
        {
            return Err(DataError::InvalidScanBounds { from, to });
        }
        Ok(self)
    }

    fn contains(self, ts: NaiveDateTime) -> bool {
        self.from.is_none_or(|from| ts >= from) && self.to.is_none_or(|to| ts <= to)
    }
}

/// A decoded row paired with its physical ordinal in the described scan.
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetScannedRow<T> {
    pub row: T,
    pub source_row_ordinal: u64,
}

trait Timestamped {
    fn timestamp(&self) -> NaiveDateTime;
}

impl Timestamped for Tick {
    fn timestamp(&self) -> NaiveDateTime {
        self.ts
    }
}

impl Timestamped for Bar {
    fn timestamp(&self) -> NaiveDateTime {
        self.ts
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume_serial_number: Option<u32>,
    #[cfg(windows)]
    file_index: Option<u64>,
}

impl FileFingerprint {
    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            created: metadata.created().ok(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(windows)]
            volume_serial_number: metadata.volume_serial_number(),
            #[cfg(windows)]
            file_index: metadata.file_index(),
        }
    }
}

#[derive(Debug, Clone)]
struct PartitionDescriptor {
    path: PathBuf,
    fingerprint: FileFingerprint,
    row_count: usize,
    source_row_base: u64,
}

impl PartitionDescriptor {
    fn describe(path: PathBuf, source_row_base: u64) -> Result<Self> {
        let file = File::open(&path)?;
        let fingerprint = FileFingerprint::from_metadata(&file.metadata()?);
        ensure_path_matches(&path, &fingerprint)?;

        let mut reader = ParquetReader::new(file.try_clone()?);
        let row_count = reader.num_rows()?;
        ensure_file_matches(&path, &file, &fingerprint)?;
        ensure_path_matches(&path, &fingerprint)?;

        Ok(Self {
            path,
            fingerprint,
            row_count,
            source_row_base,
        })
    }

    fn validate(&self) -> Result<()> {
        ensure_path_matches(&self.path, &self.fingerprint)
    }

    fn open_generation(&self) -> Result<OpenPartition> {
        self.validate()?;
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(partition_changed(&self.path));
            }
            Err(error) => return Err(error.into()),
        };
        ensure_file_matches(&self.path, &file, &self.fingerprint)?;
        self.validate()?;
        Ok(OpenPartition {
            descriptor: self.clone(),
            file,
        })
    }

    fn ordinal(&self, row_offset: usize) -> Result<u64> {
        self.source_row_base
            .checked_add(
                u64::try_from(row_offset).map_err(|_| {
                    DataError::Other("Parquet source row ordinal exceeds u64".into())
                })?,
            )
            .ok_or_else(|| DataError::Other("Parquet source row ordinal overflow".into()))
    }
}

struct OpenPartition {
    descriptor: PartitionDescriptor,
    file: File,
}

impl OpenPartition {
    fn validate(&self) -> Result<()> {
        ensure_file_matches(
            &self.descriptor.path,
            &self.file,
            &self.descriptor.fingerprint,
        )?;
        self.descriptor.validate()
    }

    fn reader_file(&self) -> Result<File> {
        self.validate()?;
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }
}

fn partition_changed(path: &Path) -> DataError {
    DataError::ParquetPartitionChanged {
        path: path.display().to_string(),
    }
}

fn ensure_file_matches(path: &Path, file: &File, expected: &FileFingerprint) -> Result<()> {
    if FileFingerprint::from_metadata(&file.metadata()?) == *expected {
        Ok(())
    } else {
        Err(partition_changed(path))
    }
}

fn ensure_path_matches(path: &Path, expected: &FileFingerprint) -> Result<()> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(partition_changed(path));
        }
        Err(error) => return Err(error.into()),
    };
    if FileFingerprint::from_metadata(&metadata) == *expected {
        Ok(())
    } else {
        Err(partition_changed(path))
    }
}

#[derive(Debug, Clone)]
struct PartitionScan {
    partitions: Vec<PartitionDescriptor>,
    bounds: ParquetScanBounds,
}

impl PartitionScan {
    fn describe(
        directory: &Path,
        bounds: ParquetScanBounds,
        is_cancelled: &mut dyn FnMut() -> bool,
    ) -> Result<Self> {
        let bounds = bounds.validate()?;
        let partitions = list_partitions(directory, bounds, is_cancelled)?;
        Ok(Self { partitions, bounds })
    }

    fn validate(&self) -> Result<()> {
        for partition in &self.partitions {
            partition.validate()?;
        }
        Ok(())
    }
}

type PartitionLoader<T> = fn(File, usize, usize) -> Result<Vec<T>>;

struct PartitionCursor<T> {
    partitions: VecDeque<PartitionDescriptor>,
    current_partition: Option<OpenPartition>,
    current_rows: std::vec::IntoIter<ParquetScannedRow<T>>,
    bounds: ParquetScanBounds,
    rows_per_read: usize,
    partition_offset: usize,
    finish_current_partition: bool,
    last_read_ts: Option<NaiveDateTime>,
    load_partition: PartitionLoader<T>,
}

impl<T: Timestamped> PartitionCursor<T> {
    fn open(
        scan: PartitionScan,
        rows_per_read: usize,
        load_partition: PartitionLoader<T>,
    ) -> Result<Self> {
        if rows_per_read == 0 {
            return Err(DataError::InvalidScanReadSize);
        }
        scan.validate()?;
        Ok(Self {
            partitions: scan.partitions.into(),
            current_partition: None,
            current_rows: Vec::new().into_iter(),
            bounds: scan.bounds,
            rows_per_read,
            partition_offset: 0,
            finish_current_partition: false,
            last_read_ts: None,
            load_partition,
        })
    }

    fn next_row(
        &mut self,
        is_cancelled: &mut dyn FnMut() -> bool,
    ) -> Result<Option<ParquetScannedRow<T>>> {
        loop {
            ensure_not_cancelled(is_cancelled)?;
            if !self.current_rows.as_slice().is_empty() {
                self.current_partition
                    .as_ref()
                    .expect("buffered rows retain their open partition")
                    .validate()?;
                return Ok(self.current_rows.next());
            }

            if self.finish_current_partition {
                self.current_partition = None;
                self.partition_offset = 0;
                self.finish_current_partition = false;
            }

            if self.current_partition.is_none() {
                let Some(descriptor) = self.partitions.pop_front() else {
                    return Ok(None);
                };
                self.current_partition = Some(descriptor.open_generation()?);
            }

            let partition = self
                .current_partition
                .as_ref()
                .expect("current partition was opened");
            if self.partition_offset >= partition.descriptor.row_count {
                self.finish_current_partition = true;
                continue;
            }

            ensure_not_cancelled(is_cancelled)?;
            let rows_to_read = self
                .rows_per_read
                .min(partition.descriptor.row_count - self.partition_offset);
            let reader = partition.reader_file()?;
            let rows = (self.load_partition)(reader, self.partition_offset, rows_to_read)?;
            ensure_not_cancelled(is_cancelled)?;
            partition.validate()?;
            if rows.len() != rows_to_read {
                return Err(DataError::Other(format!(
                    "Parquet slice {} at offset {} returned {} rows instead of {}",
                    partition.descriptor.path.display(),
                    self.partition_offset,
                    rows.len(),
                    rows_to_read
                )));
            }

            let read_offset = self.partition_offset;
            let (keep_len, last_read_ts, exceeded_upper_bound) = validate_monotonic_prefix(
                self.last_read_ts,
                &rows,
                &partition.descriptor.path,
                self.bounds.to,
            )?;
            self.last_read_ts = last_read_ts;
            self.partition_offset = self
                .partition_offset
                .checked_add(rows.len())
                .ok_or_else(|| DataError::Other("Parquet scan row offset overflow".into()))?;
            self.finish_current_partition =
                exceeded_upper_bound || self.partition_offset >= partition.descriptor.row_count;
            if exceeded_upper_bound {
                self.partitions.clear();
            }

            let descriptor = &partition.descriptor;
            let mut bounded_rows = Vec::with_capacity(keep_len);
            for (index, row) in rows.into_iter().take(keep_len).enumerate() {
                if self.bounds.contains(row.timestamp()) {
                    bounded_rows.push(ParquetScannedRow {
                        row,
                        source_row_ordinal: descriptor.ordinal(read_offset + index)?,
                    });
                }
            }
            self.current_rows = bounded_rows.into_iter();
        }
    }

    fn remaining_partitions(&self) -> usize {
        self.partitions.len() + usize::from(self.current_partition.is_some())
    }

    fn rows_per_read(&self) -> usize {
        self.rows_per_read
    }
}

fn validate_monotonic_prefix<T: Timestamped>(
    mut previous: Option<NaiveDateTime>,
    rows: &[T],
    path: &Path,
    inclusive_to: Option<NaiveDateTime>,
) -> Result<(usize, Option<NaiveDateTime>, bool)> {
    for (index, row) in rows.iter().enumerate() {
        let current = row.timestamp();
        if let Some(previous) = previous
            && current < previous
        {
            return Err(DataError::NonMonotonicParquetData {
                path: path.display().to_string(),
                previous,
                current,
            });
        }
        if inclusive_to.is_some_and(|to| current > to) {
            return Ok((index, previous, true));
        }
        previous = Some(current);
    }
    Ok((rows.len(), previous, false))
}

fn find_latest_row<T, P>(
    scan: &PartitionScan,
    rows_per_read: usize,
    load_partition: PartitionLoader<T>,
    mut predicate: P,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<Option<ParquetScannedRow<T>>>
where
    T: Timestamped,
    P: FnMut(&T) -> bool,
{
    if rows_per_read == 0 {
        return Err(DataError::InvalidScanReadSize);
    }
    scan.validate()?;
    let mut later_first_ts = None;

    for descriptor in scan.partitions.iter().rev() {
        ensure_not_cancelled(is_cancelled)?;
        let partition = descriptor.open_generation()?;
        let mut end = descriptor.row_count;
        while end > 0 {
            ensure_not_cancelled(is_cancelled)?;
            let start = end.saturating_sub(rows_per_read);
            let reader = partition.reader_file()?;
            let rows = load_partition(reader, start, end - start)?;
            ensure_not_cancelled(is_cancelled)?;
            partition.validate()?;
            if rows.len() != end - start {
                return Err(DataError::Other(format!(
                    "Parquet reverse slice {} at offset {} returned {} rows instead of {}",
                    descriptor.path.display(),
                    start,
                    rows.len(),
                    end - start
                )));
            }
            validate_monotonic_rows(None, &rows, &descriptor.path)?;
            if let (Some(last), Some(later)) =
                (rows.last().map(Timestamped::timestamp), later_first_ts)
                && last > later
            {
                return Err(DataError::NonMonotonicParquetData {
                    path: descriptor.path.display().to_string(),
                    previous: last,
                    current: later,
                });
            }
            if let Some(first) = rows.first().map(Timestamped::timestamp) {
                later_first_ts = Some(first);
            }

            for (index, row) in rows.into_iter().enumerate().rev() {
                let timestamp = row.timestamp();
                if scan.bounds.from.is_some_and(|from| timestamp < from) {
                    return Ok(None);
                }
                if scan.bounds.contains(timestamp) && predicate(&row) {
                    return Ok(Some(ParquetScannedRow {
                        row,
                        source_row_ordinal: descriptor.ordinal(start + index)?,
                    }));
                }
            }
            end = start;
        }
    }

    ensure_not_cancelled(is_cancelled)?;
    Ok(None)
}

/// An immutable description of bounded tick partitions.
#[derive(Debug, Clone)]
pub struct ParquetTickScan {
    inner: PartitionScan,
}

impl ParquetTickScan {
    /// Describe tick partitions and bind each path to its current file fingerprint.
    pub fn describe(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        bounds: ParquetScanBounds,
    ) -> Result<Self> {
        Self::describe_cancellable(root, exchange, symbol, bounds, || false)
    }

    /// Describe tick partitions with cooperative cancellation.
    pub fn describe_cancellable<F>(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        bounds: ParquetScanBounds,
        mut is_cancelled: F,
    ) -> Result<Self>
    where
        F: FnMut() -> bool,
    {
        let directory = root
            .as_ref()
            .join("ticks")
            .join(format!("exchange={exchange}"))
            .join(format!("symbol={symbol}"));
        Ok(Self {
            inner: PartitionScan::describe(&directory, bounds, &mut is_cancelled)?,
        })
    }

    /// Open a cursor against the file generations bound by this description.
    pub fn cursor(&self) -> Result<ParquetTickCursor> {
        self.cursor_with_read_size(DEFAULT_PARQUET_SCAN_ROWS)
    }

    /// Open a cursor with a maximum number of rows decoded per read.
    pub fn cursor_with_read_size(&self, rows_per_read: usize) -> Result<ParquetTickCursor> {
        Ok(ParquetTickCursor {
            inner: PartitionCursor::open(self.inner.clone(), rows_per_read, read_tick_partition)?,
        })
    }

    /// Find the latest valid quote inside the inclusive scan bounds.
    pub fn latest_valid_tick_cancellable<F>(
        &self,
        mut is_cancelled: F,
    ) -> Result<Option<ParquetScannedRow<Tick>>>
    where
        F: FnMut() -> bool,
    {
        find_latest_row(
            &self.inner,
            DEFAULT_PARQUET_SCAN_ROWS,
            read_tick_partition,
            tick_has_valid_quote,
            &mut is_cancelled,
        )
    }

    /// Find the latest valid quote strictly before `before` and inside the scan bounds.
    pub fn latest_valid_tick_before_cancellable<F>(
        &self,
        before: NaiveDateTime,
        mut is_cancelled: F,
    ) -> Result<Option<ParquetScannedRow<Tick>>>
    where
        F: FnMut() -> bool,
    {
        find_latest_row(
            &self.inner,
            DEFAULT_PARQUET_SCAN_ROWS,
            read_tick_partition,
            |tick| tick.ts < before && tick_has_valid_quote(tick),
            &mut is_cancelled,
        )
    }
}

/// Partition-at-a-time cursor over ascending tick rows.
pub struct ParquetTickCursor {
    inner: PartitionCursor<Tick>,
}

impl ParquetTickCursor {
    /// Open a tick cursor directly from a Parquet data root.
    pub fn open(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        bounds: ParquetScanBounds,
    ) -> Result<Self> {
        Self::open_with_read_size(root, exchange, symbol, bounds, DEFAULT_PARQUET_SCAN_ROWS)
    }

    /// Open a tick cursor with a maximum number of rows decoded per read.
    pub fn open_with_read_size(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        bounds: ParquetScanBounds,
        rows_per_read: usize,
    ) -> Result<Self> {
        Self::open_cancellable_with_read_size(root, exchange, symbol, bounds, rows_per_read, || {
            false
        })
    }

    /// Open a tick cursor while checking cancellation during partition discovery.
    pub fn open_cancellable<F>(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        bounds: ParquetScanBounds,
        is_cancelled: F,
    ) -> Result<Self>
    where
        F: FnMut() -> bool,
    {
        Self::open_cancellable_with_read_size(
            root,
            exchange,
            symbol,
            bounds,
            DEFAULT_PARQUET_SCAN_ROWS,
            is_cancelled,
        )
    }

    /// Open a bounded tick cursor with cancellable partition discovery.
    pub fn open_cancellable_with_read_size<F>(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        bounds: ParquetScanBounds,
        rows_per_read: usize,
        is_cancelled: F,
    ) -> Result<Self>
    where
        F: FnMut() -> bool,
    {
        ParquetTickScan::describe_cancellable(root, exchange, symbol, bounds, is_cancelled)?
            .cursor_with_read_size(rows_per_read)
    }

    /// Read the next ascending tick.
    pub fn next_tick(&mut self) -> Result<Option<Tick>> {
        self.next_tick_with_ordinal()
            .map(|row| row.map(|row| row.row))
    }

    /// Read the next ascending tick with its physical source-row ordinal.
    pub fn next_tick_with_ordinal(&mut self) -> Result<Option<ParquetScannedRow<Tick>>> {
        self.next_tick_with_ordinal_cancellable(|| false)
    }

    /// Read the next ascending tick with cooperative cancellation.
    pub fn next_tick_cancellable<F>(&mut self, is_cancelled: F) -> Result<Option<Tick>>
    where
        F: FnMut() -> bool,
    {
        self.next_tick_with_ordinal_cancellable(is_cancelled)
            .map(|row| row.map(|row| row.row))
    }

    /// Read the next tick and source ordinal with cooperative cancellation.
    pub fn next_tick_with_ordinal_cancellable<F>(
        &mut self,
        mut is_cancelled: F,
    ) -> Result<Option<ParquetScannedRow<Tick>>>
    where
        F: FnMut() -> bool,
    {
        self.inner.next_row(&mut is_cancelled)
    }

    /// Number of date partitions still active or pending.
    pub fn remaining_partitions(&self) -> usize {
        self.inner.remaining_partitions()
    }

    /// Maximum number of rows decoded by one Parquet read.
    pub fn rows_per_read(&self) -> usize {
        self.inner.rows_per_read()
    }
}

/// An immutable description of bounded bar partitions.
#[derive(Debug, Clone)]
pub struct ParquetBarScan {
    inner: PartitionScan,
}

impl ParquetBarScan {
    /// Describe bar partitions and bind each path to its current file fingerprint.
    pub fn describe(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        timeframe: &str,
        bounds: ParquetScanBounds,
    ) -> Result<Self> {
        Self::describe_cancellable(root, exchange, symbol, timeframe, bounds, || false)
    }

    /// Describe bar partitions with cooperative cancellation.
    pub fn describe_cancellable<F>(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        timeframe: &str,
        bounds: ParquetScanBounds,
        mut is_cancelled: F,
    ) -> Result<Self>
    where
        F: FnMut() -> bool,
    {
        let directory = root
            .as_ref()
            .join("bars")
            .join(format!("exchange={exchange}"))
            .join(format!("symbol={symbol}"))
            .join(format!("timeframe={timeframe}"));
        Ok(Self {
            inner: PartitionScan::describe(&directory, bounds, &mut is_cancelled)?,
        })
    }

    /// Open a cursor against the file generations bound by this description.
    pub fn cursor(&self) -> Result<ParquetBarCursor> {
        self.cursor_with_read_size(DEFAULT_PARQUET_SCAN_ROWS)
    }

    /// Open a cursor with a maximum number of rows decoded per read.
    pub fn cursor_with_read_size(&self, rows_per_read: usize) -> Result<ParquetBarCursor> {
        Ok(ParquetBarCursor {
            inner: PartitionCursor::open(self.inner.clone(), rows_per_read, read_bar_partition)?,
        })
    }

    /// Find the latest bar with a valid close inside the inclusive scan bounds.
    pub fn latest_valid_bar_cancellable<F>(
        &self,
        mut is_cancelled: F,
    ) -> Result<Option<ParquetScannedRow<Bar>>>
    where
        F: FnMut() -> bool,
    {
        find_latest_row(
            &self.inner,
            DEFAULT_PARQUET_SCAN_ROWS,
            read_bar_partition,
            |bar| bar.close.is_finite() && bar.close > 0.0,
            &mut is_cancelled,
        )
    }
}

/// Partition-at-a-time cursor over ascending bar rows.
pub struct ParquetBarCursor {
    inner: PartitionCursor<Bar>,
}

impl ParquetBarCursor {
    /// Open a bar cursor directly from a Parquet data root.
    pub fn open(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        timeframe: &str,
        bounds: ParquetScanBounds,
    ) -> Result<Self> {
        Self::open_with_read_size(
            root,
            exchange,
            symbol,
            timeframe,
            bounds,
            DEFAULT_PARQUET_SCAN_ROWS,
        )
    }

    /// Open a bar cursor with a maximum number of rows decoded per read.
    pub fn open_with_read_size(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        timeframe: &str,
        bounds: ParquetScanBounds,
        rows_per_read: usize,
    ) -> Result<Self> {
        Self::open_cancellable_with_read_size(
            root,
            exchange,
            symbol,
            timeframe,
            bounds,
            rows_per_read,
            || false,
        )
    }

    /// Open a bar cursor while checking cancellation during partition discovery.
    pub fn open_cancellable<F>(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        timeframe: &str,
        bounds: ParquetScanBounds,
        is_cancelled: F,
    ) -> Result<Self>
    where
        F: FnMut() -> bool,
    {
        Self::open_cancellable_with_read_size(
            root,
            exchange,
            symbol,
            timeframe,
            bounds,
            DEFAULT_PARQUET_SCAN_ROWS,
            is_cancelled,
        )
    }

    /// Open a bounded bar cursor with cancellable partition discovery.
    pub fn open_cancellable_with_read_size<F>(
        root: impl AsRef<Path>,
        exchange: &str,
        symbol: &str,
        timeframe: &str,
        bounds: ParquetScanBounds,
        rows_per_read: usize,
        is_cancelled: F,
    ) -> Result<Self>
    where
        F: FnMut() -> bool,
    {
        ParquetBarScan::describe_cancellable(
            root,
            exchange,
            symbol,
            timeframe,
            bounds,
            is_cancelled,
        )?
        .cursor_with_read_size(rows_per_read)
    }

    /// Read the next ascending bar.
    pub fn next_bar(&mut self) -> Result<Option<Bar>> {
        self.next_bar_with_ordinal()
            .map(|row| row.map(|row| row.row))
    }

    /// Read the next ascending bar with its physical source-row ordinal.
    pub fn next_bar_with_ordinal(&mut self) -> Result<Option<ParquetScannedRow<Bar>>> {
        self.next_bar_with_ordinal_cancellable(|| false)
    }

    /// Read the next ascending bar with cooperative cancellation.
    pub fn next_bar_cancellable<F>(&mut self, is_cancelled: F) -> Result<Option<Bar>>
    where
        F: FnMut() -> bool,
    {
        self.next_bar_with_ordinal_cancellable(is_cancelled)
            .map(|row| row.map(|row| row.row))
    }

    /// Read the next bar and source ordinal with cooperative cancellation.
    pub fn next_bar_with_ordinal_cancellable<F>(
        &mut self,
        mut is_cancelled: F,
    ) -> Result<Option<ParquetScannedRow<Bar>>>
    where
        F: FnMut() -> bool,
    {
        self.inner.next_row(&mut is_cancelled)
    }

    /// Number of date partitions still active or pending.
    pub fn remaining_partitions(&self) -> usize {
        self.inner.remaining_partitions()
    }

    /// Maximum number of rows decoded by one Parquet read.
    pub fn rows_per_read(&self) -> usize {
        self.inner.rows_per_read()
    }
}

impl ParquetStore {
    /// Create an ascending tick cursor over this store.
    pub fn scan_ticks(
        &self,
        exchange: &str,
        symbol: &str,
        bounds: ParquetScanBounds,
    ) -> Result<ParquetTickCursor> {
        ParquetTickCursor::open(self.root_path(), exchange, symbol, bounds)
    }

    /// Create an ascending tick cursor with cancellable partition discovery.
    pub fn scan_ticks_cancellable<F>(
        &self,
        exchange: &str,
        symbol: &str,
        bounds: ParquetScanBounds,
        is_cancelled: F,
    ) -> Result<ParquetTickCursor>
    where
        F: FnMut() -> bool,
    {
        ParquetTickCursor::open_cancellable(
            self.root_path(),
            exchange,
            symbol,
            bounds,
            is_cancelled,
        )
    }

    /// Create an ascending bar cursor over this store.
    pub fn scan_bars(
        &self,
        exchange: &str,
        symbol: &str,
        timeframe: &str,
        bounds: ParquetScanBounds,
    ) -> Result<ParquetBarCursor> {
        ParquetBarCursor::open(self.root_path(), exchange, symbol, timeframe, bounds)
    }

    /// Create an ascending bar cursor with cancellable partition discovery.
    pub fn scan_bars_cancellable<F>(
        &self,
        exchange: &str,
        symbol: &str,
        timeframe: &str,
        bounds: ParquetScanBounds,
        is_cancelled: F,
    ) -> Result<ParquetBarCursor>
    where
        F: FnMut() -> bool,
    {
        ParquetBarCursor::open_cancellable(
            self.root_path(),
            exchange,
            symbol,
            timeframe,
            bounds,
            is_cancelled,
        )
    }
}

fn list_partitions(
    directory: &Path,
    bounds: ParquetScanBounds,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<Vec<PartitionDescriptor>> {
    ensure_not_cancelled(is_cancelled)?;
    if !directory.exists() {
        return Ok(Vec::new());
    }

    let from_date = bounds.from.map(|ts| ts.date());
    let to_date = bounds.to.map(|ts| ts.date());
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        ensure_not_cancelled(is_cancelled)?;
        let path = entry?.path();
        if !path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            continue;
        }

        let stem = path
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or_else(|| DataError::InvalidDatePartition(path.display().to_string()))?;
        let date = NaiveDate::parse_from_str(stem, "%Y-%m-%d")
            .map_err(|_| DataError::InvalidDatePartition(path.display().to_string()))?;
        if date.format("%Y-%m-%d").to_string() != stem {
            return Err(DataError::InvalidDatePartition(path.display().to_string()));
        }
        if from_date.is_some_and(|from| date < from) || to_date.is_some_and(|to| date > to) {
            continue;
        }
        paths.push((date, path));
    }
    paths.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let mut partitions = Vec::with_capacity(paths.len());
    let mut source_row_base = 0u64;
    for (_, path) in paths {
        ensure_not_cancelled(is_cancelled)?;
        let descriptor = PartitionDescriptor::describe(path, source_row_base)?;
        source_row_base =
            source_row_base
                .checked_add(u64::try_from(descriptor.row_count).map_err(|_| {
                    DataError::Other("Parquet partition row count exceeds u64".into())
                })?)
                .ok_or_else(|| DataError::Other("Parquet source row ordinal overflow".into()))?;
        partitions.push(descriptor);
    }
    ensure_not_cancelled(is_cancelled)?;
    Ok(partitions)
}

fn validate_monotonic_rows<T: Timestamped>(
    mut previous: Option<NaiveDateTime>,
    rows: &[T],
    path: &Path,
) -> Result<()> {
    for row in rows {
        let current = row.timestamp();
        if let Some(previous) = previous
            && current < previous
        {
            return Err(DataError::NonMonotonicParquetData {
                path: path.display().to_string(),
                previous,
                current,
            });
        }
        previous = Some(current);
    }
    Ok(())
}

fn ensure_not_cancelled(is_cancelled: &mut dyn FnMut() -> bool) -> Result<()> {
    if is_cancelled() {
        Err(DataError::Cancelled)
    } else {
        Ok(())
    }
}

fn read_tick_partition(file: File, offset: usize, rows: usize) -> Result<Vec<Tick>> {
    let dataframe = ParquetReader::new(file)
        .with_slice(Some((offset, rows)))
        .finish()?;
    dataframe_to_ticks(&dataframe)
}

fn read_bar_partition(file: File, offset: usize, rows: usize) -> Result<Vec<Bar>> {
    let dataframe = ParquetReader::new(file)
        .with_slice(Some((offset, rows)))
        .finish()?;
    dataframe_to_bars(&dataframe)
}

fn tick_has_valid_quote(tick: &Tick) -> bool {
    match (tick.bid, tick.ask) {
        (Some(bid), Some(ask)) => {
            bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0 && bid <= ask
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::convert::ticks_to_dataframe;
    use crate::models::Timeframe;

    fn ts(day: u32, hour: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, day)
            .unwrap()
            .and_hms_opt(hour, 0, 0)
            .unwrap()
    }

    fn temp_root(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "qs-data-preprocess-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn tick(at: NaiveDateTime) -> Tick {
        Tick {
            exchange: "test".into(),
            symbol: "EURUSD".into(),
            ts: at,
            bid: Some(1.0),
            ask: Some(1.1),
            last: None,
            volume: None,
            flags: None,
        }
    }

    fn bar(at: NaiveDateTime) -> Bar {
        Bar {
            exchange: "test".into(),
            symbol: "EURUSD".into(),
            timeframe: Timeframe::M1,
            ts: at,
            open: 1.0,
            high: 1.2,
            low: 0.9,
            close: 1.1,
            tick_vol: 10,
            volume: 10,
            spread: 1,
        }
    }

    #[test]
    fn tick_cursor_scans_partitions_in_ascending_bounded_order() {
        let root = temp_root("ticks");
        let store = ParquetStore::open(&root).unwrap();
        store
            .insert_ticks(&[tick(ts(2, 1)), tick(ts(1, 2)), tick(ts(1, 1))])
            .unwrap();

        let mut cursor = store
            .scan_ticks(
                "test",
                "EURUSD",
                ParquetScanBounds::new(Some(ts(1, 2)), Some(ts(2, 1))),
            )
            .unwrap();
        assert_eq!(cursor.remaining_partitions(), 2);
        assert_eq!(cursor.next_tick().unwrap().unwrap().ts, ts(1, 2));
        assert_eq!(cursor.next_tick().unwrap().unwrap().ts, ts(2, 1));
        assert!(cursor.next_tick().unwrap().is_none());

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn bar_cursor_scans_ordered_date_partitions() {
        let root = temp_root("bars");
        let store = ParquetStore::open(&root).unwrap();
        store.insert_bars(&[bar(ts(2, 1)), bar(ts(1, 1))]).unwrap();

        let mut cursor = store
            .scan_bars("test", "EURUSD", "1m", ParquetScanBounds::default())
            .unwrap();
        assert_eq!(cursor.next_bar().unwrap().unwrap().ts, ts(1, 1));
        assert_eq!(cursor.next_bar().unwrap().unwrap().ts, ts(2, 1));
        assert!(cursor.next_bar().unwrap().is_none());

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cursor_reads_large_partitions_through_bounded_slices() {
        let root = temp_root("slices");
        let store = ParquetStore::open(&root).unwrap();
        store
            .insert_ticks(&[tick(ts(1, 1)), tick(ts(1, 2)), tick(ts(1, 3))])
            .unwrap();
        let mut cursor = ParquetTickCursor::open_with_read_size(
            &root,
            "test",
            "EURUSD",
            ParquetScanBounds::default(),
            1,
        )
        .unwrap();

        assert_eq!(cursor.rows_per_read(), 1);
        assert_eq!(cursor.next_tick().unwrap().unwrap().ts, ts(1, 1));
        assert_eq!(cursor.next_tick().unwrap().unwrap().ts, ts(1, 2));
        assert_eq!(cursor.next_tick().unwrap().unwrap().ts, ts(1, 3));
        assert!(cursor.next_tick().unwrap().is_none());
        assert!(matches!(
            ParquetTickCursor::open_with_read_size(
                &root,
                "test",
                "EURUSD",
                ParquetScanBounds::default(),
                0,
            ),
            Err(DataError::InvalidScanReadSize)
        ));

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cursor_cancellation_does_not_consume_the_partition() {
        let root = temp_root("cancel");
        let store = ParquetStore::open(&root).unwrap();
        store.insert_ticks(&[tick(ts(1, 1))]).unwrap();
        let mut cursor = store
            .scan_ticks("test", "EURUSD", ParquetScanBounds::default())
            .unwrap();
        let mut checks = 0;

        let error = cursor
            .next_tick_cancellable(|| {
                checks += 1;
                checks == 3
            })
            .unwrap_err();
        assert!(matches!(error, DataError::Cancelled));
        assert_eq!(cursor.next_tick().unwrap().unwrap().ts, ts(1, 1));

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cursor_rejects_non_monotonic_partition_rows_before_emitting_them() {
        let root = temp_root("monotonic");
        let path = root
            .join("ticks")
            .join("exchange=test")
            .join("symbol=EURUSD")
            .join("2026-01-01.parquet");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut dataframe = ticks_to_dataframe(&[tick(ts(1, 2)), tick(ts(1, 1))]).unwrap();
        ParquetWriter::new(File::create(&path).unwrap())
            .finish(&mut dataframe)
            .unwrap();

        let mut cursor =
            ParquetTickCursor::open(&root, "test", "EURUSD", ParquetScanBounds::default()).unwrap();
        assert!(matches!(
            cursor.next_tick(),
            Err(DataError::NonMonotonicParquetData {
                previous,
                current,
                ..
            }) if previous == ts(1, 2) && current == ts(1, 1)
        ));

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cursor_reports_physical_ordinals_across_filtered_rows_and_partitions() {
        let root = temp_root("ordinals");
        let store = ParquetStore::open(&root).unwrap();
        store
            .insert_ticks(&[
                tick(ts(1, 1)),
                tick(ts(1, 2)),
                tick(ts(1, 3)),
                tick(ts(2, 1)),
            ])
            .unwrap();
        let mut cursor = ParquetTickCursor::open_with_read_size(
            &root,
            "test",
            "EURUSD",
            ParquetScanBounds::new(Some(ts(1, 2)), None),
            2,
        )
        .unwrap();

        let rows = [
            cursor.next_tick_with_ordinal().unwrap().unwrap(),
            cursor.next_tick_with_ordinal().unwrap().unwrap(),
            cursor.next_tick_with_ordinal().unwrap().unwrap(),
        ];
        assert_eq!(
            rows.map(|row| (row.row.ts, row.source_row_ordinal)),
            [(ts(1, 2), 1), (ts(1, 3), 2), (ts(2, 1), 3)]
        );

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn running_cursor_fails_if_atomic_replacement_changes_its_partition() {
        let root = temp_root("replace-running");
        let store = ParquetStore::open(&root).unwrap();
        store
            .insert_ticks(&[tick(ts(1, 1)), tick(ts(1, 2)), tick(ts(1, 3))])
            .unwrap();
        let mut cursor = ParquetTickCursor::open_with_read_size(
            &root,
            "test",
            "EURUSD",
            ParquetScanBounds::default(),
            1,
        )
        .unwrap();
        assert_eq!(cursor.next_tick().unwrap().unwrap().ts, ts(1, 1));
        let partition_path = root
            .join("ticks")
            .join("exchange=test")
            .join("symbol=EURUSD")
            .join("2026-01-01.parquet");
        #[cfg(unix)]
        let original_inode = {
            use std::os::unix::fs::MetadataExt as _;
            fs::metadata(&partition_path).unwrap().ino()
        };

        store.insert_ticks(&[tick(ts(1, 4))]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            assert_ne!(original_inode, fs::metadata(&partition_path).unwrap().ino());
        }
        assert!(
            fs::read_dir(partition_path.parent().unwrap())
                .unwrap()
                .all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp"))
        );
        assert!(matches!(
            cursor.next_tick(),
            Err(DataError::ParquetPartitionChanged { .. })
        ));

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn described_scan_rejects_replacement_before_reopen() {
        let root = temp_root("replace-described");
        let store = ParquetStore::open(&root).unwrap();
        store.insert_ticks(&[tick(ts(1, 1))]).unwrap();
        let scan = ParquetTickScan::describe(&root, "test", "EURUSD", ParquetScanBounds::default())
            .unwrap();

        store.insert_ticks(&[tick(ts(1, 2))]).unwrap();
        assert!(matches!(
            scan.cursor(),
            Err(DataError::ParquetPartitionChanged { .. })
        ));

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn upper_bound_stops_at_the_first_later_monotonic_row() {
        let root = temp_root("upper-bound");
        let store = ParquetStore::open(&root).unwrap();
        store
            .insert_ticks(&[
                tick(ts(1, 1)),
                tick(ts(1, 2)),
                tick(ts(1, 3)),
                tick(ts(1, 4)),
            ])
            .unwrap();
        let mut cursor = ParquetTickCursor::open_with_read_size(
            &root,
            "test",
            "EURUSD",
            ParquetScanBounds::new(None, Some(ts(1, 2))),
            3,
        )
        .unwrap();

        assert_eq!(cursor.next_tick().unwrap().unwrap().ts, ts(1, 1));
        assert_eq!(cursor.next_tick().unwrap().unwrap().ts, ts(1, 2));
        assert!(cursor.next_tick().unwrap().is_none());
        assert_eq!(cursor.remaining_partitions(), 0);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn reverse_chunks_find_the_latest_valid_tick_without_materializing_the_day() {
        let root = temp_root("latest-reverse");
        let store = ParquetStore::open(&root).unwrap();
        let mut invalid = tick(ts(1, 4));
        invalid.ask = None;
        store
            .insert_ticks(&[tick(ts(1, 1)), tick(ts(1, 2)), tick(ts(1, 3)), invalid])
            .unwrap();
        let scan = ParquetTickScan::describe(&root, "test", "EURUSD", ParquetScanBounds::default())
            .unwrap();

        let latest = find_latest_row(
            &scan.inner,
            2,
            read_tick_partition,
            tick_has_valid_quote,
            &mut || false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(latest.row.ts, ts(1, 3));
        assert_eq!(latest.source_row_ordinal, 2);

        fs::remove_dir_all(root).ok();
    }
}
