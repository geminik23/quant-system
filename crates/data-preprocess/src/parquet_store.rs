//! Parquet-based storage backend for tick and bar data.
//!
//! Uses Hive-style directory partitioning:
//!   {root}/ticks/exchange={ex}/symbol={sym}/{date}.parquet
//!   {root}/bars/exchange={ex}/symbol={sym}/timeframe={tf}/{date}.parquet

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::NaiveDateTime;
use polars::prelude::*;

use crate::convert::{
    bars_to_dataframe, dataframe_to_bars, dataframe_to_ticks, ndt_to_date_string,
    ticks_to_dataframe,
};
use crate::error::{DataError, Result};
use crate::models::{Bar, BarQueryOpts, QueryOpts, StatRow, Tick};

/// Parquet-based storage backend for tick and bar data.
pub struct ParquetStore {
    root: PathBuf,
}

impl ParquetStore {
    /// Open a Parquet data store rooted at the given directory, creating it if needed.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    // ── Import ──────────────────────────────────────────────────

    /// Import ticks, deduplicating against existing data per date partition.
    /// Returns the number of rows actually inserted (after dedup).
    pub fn insert_ticks(&self, ticks: &[Tick]) -> Result<usize> {
        if ticks.is_empty() {
            return Ok(0);
        }

        // Group ticks by (exchange, symbol, date)
        let mut groups: HashMap<(String, String, String), Vec<&Tick>> = HashMap::new();
        for tick in ticks {
            let date = ndt_to_date_string(&tick.ts);
            let key = (tick.exchange.clone(), tick.symbol.clone(), date);
            groups.entry(key).or_default().push(tick);
        }

        let mut total_inserted = 0usize;

        for ((exchange, symbol, date), group_ticks) in &groups {
            let dir = self.tick_dir(exchange, symbol);
            fs::create_dir_all(&dir)?;
            let file_path = dir.join(format!("{date}.parquet"));

            let owned: Vec<Tick> = group_ticks.iter().map(|t| (*t).clone()).collect();
            let new_df = ticks_to_dataframe(&owned)?;

            if file_path.exists() {
                let existing_df = read_parquet_file(&file_path)?;
                let existing_count = existing_df.height();
                let combined = concat_and_dedup_ticks(existing_df, new_df)?;
                total_inserted += combined.height().saturating_sub(existing_count);
                write_parquet_file(&file_path, &mut combined.clone())?;
            } else {
                let deduped = dedup_ticks(new_df)?;
                total_inserted += deduped.height();
                write_parquet_file(&file_path, &mut deduped.clone())?;
            }
        }

        Ok(total_inserted)
    }

    /// Import bars, deduplicating against existing data per date partition.
    /// Returns the number of rows actually inserted (after dedup).
    pub fn insert_bars(&self, bars: &[Bar]) -> Result<usize> {
        if bars.is_empty() {
            return Ok(0);
        }

        // Group bars by (exchange, symbol, timeframe, date)
        let mut groups: HashMap<(String, String, String, String), Vec<&Bar>> = HashMap::new();
        for bar in bars {
            let date = ndt_to_date_string(&bar.ts);
            let key = (
                bar.exchange.clone(),
                bar.symbol.clone(),
                bar.timeframe.as_str().to_string(),
                date,
            );
            groups.entry(key).or_default().push(bar);
        }

        let mut total_inserted = 0usize;

        for ((exchange, symbol, timeframe, date), group_bars) in &groups {
            let dir = self.bar_dir(&exchange, &symbol, &timeframe);
            fs::create_dir_all(&dir)?;
            let file_path = dir.join(format!("{date}.parquet"));

            let owned: Vec<Bar> = group_bars.iter().map(|b| (*b).clone()).collect();
            let new_df = bars_to_dataframe(&owned)?;

            if file_path.exists() {
                let existing_df = read_parquet_file(&file_path)?;
                let existing_count = existing_df.height();
                let combined = concat_and_dedup_bars(existing_df, new_df)?;
                total_inserted += combined.height().saturating_sub(existing_count);
                write_parquet_file(&file_path, &mut combined.clone())?;
            } else {
                let deduped = dedup_bars(new_df)?;
                total_inserted += deduped.height();
                write_parquet_file(&file_path, &mut deduped.clone())?;
            }
        }

        Ok(total_inserted)
    }

    // ── Query ───────────────────────────────────────────────────

    /// Query ticks for a given exchange+symbol, optionally filtered by date range.
    /// Returns (ticks, total_count_matching_filters).
    pub fn query_ticks(&self, opts: &QueryOpts) -> Result<(Vec<Tick>, u64)> {
        let dir = self.tick_dir(&opts.exchange, &opts.symbol);
        if !dir.exists() {
            return Ok((Vec::new(), 0));
        }

        let files = list_date_files(&dir, opts.from, opts.to)?;
        if files.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let mut all_dfs: Vec<DataFrame> = Vec::new();
        for file in &files {
            let df = read_parquet_file(file)?;
            all_dfs.push(df);
        }
        let mut combined = concat_dataframes(all_dfs)?;

        // Apply timestamp filters
        combined = apply_ts_filter(combined, opts.from, opts.to)?;

        // Sort by ts ascending
        combined = combined.sort(["ts"], SortMultipleOptions::default())?;

        let total = combined.height() as u64;

        // Apply limit/tail/descending
        combined = apply_pagination(combined, opts.limit, opts.tail, opts.descending)?;

        let ticks = dataframe_to_ticks(&combined)?;
        Ok((ticks, total))
    }

    /// Query bars for a given exchange+symbol+timeframe, optionally filtered by date range.
    /// Returns (bars, total_count_matching_filters).
    pub fn query_bars(&self, opts: &BarQueryOpts) -> Result<(Vec<Bar>, u64)> {
        let dir = self.bar_dir(&opts.exchange, &opts.symbol, &opts.timeframe);
        if !dir.exists() {
            return Ok((Vec::new(), 0));
        }

        let files = list_date_files(&dir, opts.from, opts.to)?;
        if files.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let mut all_dfs: Vec<DataFrame> = Vec::new();
        for file in &files {
            let df = read_parquet_file(file)?;
            all_dfs.push(df);
        }
        let mut combined = concat_dataframes(all_dfs)?;

        // Apply timestamp filters
        combined = apply_ts_filter(combined, opts.from, opts.to)?;

        // Sort by ts ascending
        combined = combined.sort(["ts"], SortMultipleOptions::default())?;

        let total = combined.height() as u64;

        // Apply limit/tail/descending
        combined = apply_pagination(combined, opts.limit, opts.tail, opts.descending)?;

        let bars = dataframe_to_bars(&combined)?;
        Ok((bars, total))
    }

    // ── Delete ──────────────────────────────────────────────────

    /// Delete ticks matching exchange+symbol, optionally within a date range.
    pub fn delete_ticks(
        &self,
        exchange: &str,
        symbol: &str,
        from: Option<NaiveDateTime>,
        to: Option<NaiveDateTime>,
    ) -> Result<usize> {
        let dir = self.tick_dir(exchange, symbol);
        if !dir.exists() {
            return Ok(0);
        }
        delete_from_partition(&dir, from, to)
    }

    /// Delete bars matching exchange+symbol+timeframe, optionally within a date range.
    pub fn delete_bars(
        &self,
        exchange: &str,
        symbol: &str,
        timeframe: &str,
        from: Option<NaiveDateTime>,
        to: Option<NaiveDateTime>,
    ) -> Result<usize> {
        let dir = self.bar_dir(exchange, symbol, timeframe);
        if !dir.exists() {
            return Ok(0);
        }
        delete_from_partition(&dir, from, to)
    }

    /// Delete ALL data (ticks + bars) for an exchange+symbol pair.
    pub fn delete_symbol(&self, exchange: &str, symbol: &str) -> Result<(usize, usize)> {
        let tick_count = self.count_rows_in_dir(&self.tick_dir(exchange, symbol));
        let bar_count = self.count_all_bars_for_symbol(exchange, symbol);

        // Remove tick directory
        let tick_dir = self.tick_dir(exchange, symbol);
        if tick_dir.exists() {
            fs::remove_dir_all(&tick_dir)?;
        }

        // Remove bar directories for all timeframes
        let bar_sym_dir = self
            .root
            .join("bars")
            .join(format!("exchange={exchange}"))
            .join(format!("symbol={symbol}"));
        if bar_sym_dir.exists() {
            fs::remove_dir_all(&bar_sym_dir)?;
        }

        Ok((tick_count, bar_count))
    }

    /// Delete ALL data for an entire exchange.
    pub fn delete_exchange(&self, exchange: &str) -> Result<(usize, usize)> {
        let tick_ex_dir = self.root.join("ticks").join(format!("exchange={exchange}"));
        let bar_ex_dir = self.root.join("bars").join(format!("exchange={exchange}"));

        let tick_count = self.count_rows_recursive(&tick_ex_dir);
        let bar_count = self.count_rows_recursive(&bar_ex_dir);

        if tick_ex_dir.exists() {
            fs::remove_dir_all(&tick_ex_dir)?;
        }
        if bar_ex_dir.exists() {
            fs::remove_dir_all(&bar_ex_dir)?;
        }

        Ok((tick_count, bar_count))
    }

    // ── Stats ───────────────────────────────────────────────────

    /// Summary statistics across all data, optionally filtered by exchange and/or symbol.
    pub fn stats(&self, exchange: Option<&str>, symbol: Option<&str>) -> Result<Vec<StatRow>> {
        let mut rows = Vec::new();

        // Collect tick stats
        self.collect_tick_stats(&mut rows, exchange, symbol)?;

        // Collect bar stats
        self.collect_bar_stats(&mut rows, exchange, symbol)?;

        // Sort by exchange, symbol, data_type
        rows.sort_by(|a, b| {
            a.exchange
                .cmp(&b.exchange)
                .then(a.symbol.cmp(&b.symbol))
                .then(a.data_type.cmp(&b.data_type))
        });

        Ok(rows)
    }

    /// Total size of all Parquet files under the data root (bytes).
    pub fn total_size(&self) -> Option<u64> {
        let mut total = 0u64;
        for entry in walkdir(&self.root) {
            if entry.extension().map_or(false, |e| e == "parquet") {
                if let Ok(meta) = fs::metadata(&entry) {
                    total += meta.len();
                }
            }
        }
        if total == 0 { None } else { Some(total) }
    }

    // ── Private helpers ─────────────────────────────────────────

    /// Build tick directory path for a given exchange+symbol.
    fn tick_dir(&self, exchange: &str, symbol: &str) -> PathBuf {
        self.root
            .join("ticks")
            .join(format!("exchange={exchange}"))
            .join(format!("symbol={symbol}"))
    }

    /// Build bar directory path for a given exchange+symbol+timeframe.
    fn bar_dir(&self, exchange: &str, symbol: &str, timeframe: &str) -> PathBuf {
        self.root
            .join("bars")
            .join(format!("exchange={exchange}"))
            .join(format!("symbol={symbol}"))
            .join(format!("timeframe={timeframe}"))
    }

    /// Count total rows across all parquet files in a directory.
    fn count_rows_in_dir(&self, dir: &Path) -> usize {
        if !dir.exists() {
            return 0;
        }
        let mut count = 0;
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map_or(false, |e| e == "parquet") {
                    if let Ok(df) = read_parquet_file(&path) {
                        count += df.height();
                    }
                }
            }
        }
        count
    }

    /// Count total rows recursively across all parquet files under a directory.
    fn count_rows_recursive(&self, dir: &Path) -> usize {
        if !dir.exists() {
            return 0;
        }
        let mut count = 0;
        for path in walkdir(dir) {
            if path.extension().map_or(false, |e| e == "parquet") {
                if let Ok(df) = read_parquet_file(&path) {
                    count += df.height();
                }
            }
        }
        count
    }

    /// Count all bar rows for a given exchange+symbol across all timeframes.
    fn count_all_bars_for_symbol(&self, exchange: &str, symbol: &str) -> usize {
        let bar_sym_dir = self
            .root
            .join("bars")
            .join(format!("exchange={exchange}"))
            .join(format!("symbol={symbol}"));
        self.count_rows_recursive(&bar_sym_dir)
    }

    /// Collect tick stats from the directory tree.
    fn collect_tick_stats(
        &self,
        rows: &mut Vec<StatRow>,
        exchange_filter: Option<&str>,
        symbol_filter: Option<&str>,
    ) -> Result<()> {
        let ticks_dir = self.root.join("ticks");
        if !ticks_dir.exists() {
            return Ok(());
        }

        for (exchange, symbol, dir) in self.iter_exchange_symbol_dirs(&ticks_dir)? {
            if let Some(ef) = exchange_filter {
                if exchange != ef {
                    continue;
                }
            }
            if let Some(sf) = symbol_filter {
                if symbol != sf {
                    continue;
                }
            }

            let (count, ts_min, ts_max) = self.aggregate_parquet_stats(&dir)?;
            if count > 0 {
                rows.push(StatRow {
                    exchange,
                    symbol,
                    data_type: "tick".to_string(),
                    count,
                    ts_min: ts_min.unwrap_or_default(),
                    ts_max: ts_max.unwrap_or_default(),
                });
            }
        }

        Ok(())
    }

    /// Collect bar stats from the directory tree.
    fn collect_bar_stats(
        &self,
        rows: &mut Vec<StatRow>,
        exchange_filter: Option<&str>,
        symbol_filter: Option<&str>,
    ) -> Result<()> {
        let bars_dir = self.root.join("bars");
        if !bars_dir.exists() {
            return Ok(());
        }

        for (exchange, symbol, timeframe, dir) in self.iter_exchange_symbol_tf_dirs(&bars_dir)? {
            if let Some(ef) = exchange_filter {
                if exchange != ef {
                    continue;
                }
            }
            if let Some(sf) = symbol_filter {
                if symbol != sf {
                    continue;
                }
            }

            let (count, ts_min, ts_max) = self.aggregate_parquet_stats(&dir)?;
            if count > 0 {
                rows.push(StatRow {
                    exchange,
                    symbol,
                    data_type: format!("bar ({timeframe})"),
                    count,
                    ts_min: ts_min.unwrap_or_default(),
                    ts_max: ts_max.unwrap_or_default(),
                });
            }
        }

        Ok(())
    }

    /// Iterate over exchange/symbol directories under a top-level dir.
    fn iter_exchange_symbol_dirs(&self, base: &Path) -> Result<Vec<(String, String, PathBuf)>> {
        let mut result = Vec::new();
        if !base.exists() {
            return Ok(result);
        }

        for ex_entry in fs::read_dir(base)?.flatten() {
            let ex_path = ex_entry.path();
            if !ex_path.is_dir() {
                continue;
            }
            let exchange =
                parse_partition_value(ex_path.file_name().unwrap().to_str().unwrap_or(""));
            if exchange.is_empty() {
                continue;
            }

            for sym_entry in fs::read_dir(&ex_path)?.flatten() {
                let sym_path = sym_entry.path();
                if !sym_path.is_dir() {
                    continue;
                }
                let symbol =
                    parse_partition_value(sym_path.file_name().unwrap().to_str().unwrap_or(""));
                if symbol.is_empty() {
                    continue;
                }
                result.push((exchange.clone(), symbol, sym_path));
            }
        }

        Ok(result)
    }

    /// Iterate over exchange/symbol/timeframe directories under a top-level dir.
    fn iter_exchange_symbol_tf_dirs(
        &self,
        base: &Path,
    ) -> Result<Vec<(String, String, String, PathBuf)>> {
        let mut result = Vec::new();
        if !base.exists() {
            return Ok(result);
        }

        for ex_entry in fs::read_dir(base)?.flatten() {
            let ex_path = ex_entry.path();
            if !ex_path.is_dir() {
                continue;
            }
            let exchange =
                parse_partition_value(ex_path.file_name().unwrap().to_str().unwrap_or(""));
            if exchange.is_empty() {
                continue;
            }

            for sym_entry in fs::read_dir(&ex_path)?.flatten() {
                let sym_path = sym_entry.path();
                if !sym_path.is_dir() {
                    continue;
                }
                let symbol =
                    parse_partition_value(sym_path.file_name().unwrap().to_str().unwrap_or(""));
                if symbol.is_empty() {
                    continue;
                }

                for tf_entry in fs::read_dir(&sym_path)?.flatten() {
                    let tf_path = tf_entry.path();
                    if !tf_path.is_dir() {
                        continue;
                    }
                    let timeframe =
                        parse_partition_value(tf_path.file_name().unwrap().to_str().unwrap_or(""));
                    if timeframe.is_empty() {
                        continue;
                    }
                    result.push((exchange.clone(), symbol.clone(), timeframe, tf_path));
                }
            }
        }

        Ok(result)
    }

    /// Read all parquet files in a directory and aggregate row count + min/max ts.
    fn aggregate_parquet_stats(
        &self,
        dir: &Path,
    ) -> Result<(u64, Option<NaiveDateTime>, Option<NaiveDateTime>)> {
        let mut total_count = 0u64;
        let mut global_min: Option<i64> = None;
        let mut global_max: Option<i64> = None;

        if !dir.exists() {
            return Ok((0, None, None));
        }

        for entry in fs::read_dir(dir)?.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "parquet") {
                let df = read_parquet_file(&path)?;
                total_count += df.height() as u64;

                if df.height() > 0 {
                    let ts_col = df.column("ts").ok().and_then(|c| c.datetime().ok());
                    if let Some(ts) = ts_col {
                        if let Some(min_val) = ts.min() {
                            global_min =
                                Some(global_min.map_or(min_val, |cur: i64| cur.min(min_val)));
                        }
                        if let Some(max_val) = ts.max() {
                            global_max =
                                Some(global_max.map_or(max_val, |cur: i64| cur.max(max_val)));
                        }
                    }
                }
            }
        }

        let ts_min = global_min.map(micros_to_ndt);
        let ts_max = global_max.map(micros_to_ndt);

        Ok((total_count, ts_min, ts_max))
    }
}

// ── Free functions ──────────────────────────────────────────────

/// Parse a Hive partition value from a directory name like "exchange=ctrader".
fn parse_partition_value(dir_name: &str) -> String {
    dir_name
        .split_once('=')
        .map(|(_, v)| v.to_string())
        .unwrap_or_default()
}

/// List parquet files in a directory, optionally filtered by date range in filename.
fn list_date_files(
    dir: &Path,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let from_date = from.map(|d| d.format("%Y-%m-%d").to_string());
    let to_date = to.map(|d| d.format("%Y-%m-%d").to_string());

    for entry in fs::read_dir(dir)?.flatten() {
        let path = entry.path();
        if path.extension().map_or(false, |e| e == "parquet") {
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");

            // Filename-level date pruning
            let dominated_by_from = from_date.as_ref().map_or(false, |fd| stem < fd.as_str());
            let past_to = to_date.as_ref().map_or(false, |td| stem > td.as_str());

            if !dominated_by_from && !past_to {
                files.push(path);
            }
        }
    }

    files.sort();
    Ok(files)
}

/// Read a single Parquet file into a DataFrame.
fn read_parquet_file(path: &Path) -> Result<DataFrame> {
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    Ok(df)
}

/// Write a DataFrame to a Parquet file with zstd compression.
fn write_parquet_file(path: &Path, df: &mut DataFrame) -> Result<()> {
    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(df)?;
    Ok(())
}

/// Concat two tick DataFrames, dedup on (exchange, symbol, ts), sort by ts.
fn concat_and_dedup_ticks(existing: DataFrame, new: DataFrame) -> Result<DataFrame> {
    let combined = concat_dataframes(vec![existing, new])?;
    dedup_ticks(combined)
}

/// Dedup a tick DataFrame on (exchange, symbol, ts) keeping first, sort by ts.
fn dedup_ticks(df: DataFrame) -> Result<DataFrame> {
    let cols: Vec<String> = vec!["exchange".into(), "symbol".into(), "ts".into()];
    let deduped = df
        .unique_stable(Some(&cols), UniqueKeepStrategy::First, None)?
        .sort(["ts"], SortMultipleOptions::default())?;
    Ok(deduped)
}

/// Concat two bar DataFrames, dedup on (exchange, symbol, timeframe, ts), sort by ts.
fn concat_and_dedup_bars(existing: DataFrame, new: DataFrame) -> Result<DataFrame> {
    let combined = concat_dataframes(vec![existing, new])?;
    dedup_bars(combined)
}

/// Dedup a bar DataFrame on (exchange, symbol, timeframe, ts) keeping first, sort by ts.
fn dedup_bars(df: DataFrame) -> Result<DataFrame> {
    let cols: Vec<String> = vec![
        "exchange".into(),
        "symbol".into(),
        "timeframe".into(),
        "ts".into(),
    ];
    let deduped = df
        .unique_stable(Some(&cols), UniqueKeepStrategy::First, None)?
        .sort(["ts"], SortMultipleOptions::default())?;
    Ok(deduped)
}

/// Vertically concatenate multiple DataFrames.
fn concat_dataframes(dfs: Vec<DataFrame>) -> Result<DataFrame> {
    if dfs.is_empty() {
        return Err(DataError::Other("no dataframes to concat".into()));
    }
    if dfs.len() == 1 {
        return Ok(dfs.into_iter().next().unwrap());
    }
    let lazy_frames: Vec<LazyFrame> = dfs.into_iter().map(|df| df.lazy()).collect();
    let combined = polars::prelude::concat(lazy_frames, Default::default())?.collect()?;
    Ok(combined)
}

/// Apply timestamp range filter to a DataFrame with a "ts" datetime column.
fn apply_ts_filter(
    df: DataFrame,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<DataFrame> {
    if from.is_none() && to.is_none() {
        return Ok(df);
    }

    let mut lf = df.lazy();

    if let Some(f) = from {
        let from_micros = f.and_utc().timestamp_micros();
        lf = lf.filter(
            col("ts")
                .gt_eq(lit(from_micros).cast(DataType::Datetime(TimeUnit::Microseconds, None))),
        );
    }
    if let Some(t) = to {
        let to_micros = t.and_utc().timestamp_micros();
        lf = lf.filter(
            col("ts").lt_eq(lit(to_micros).cast(DataType::Datetime(TimeUnit::Microseconds, None))),
        );
    }

    Ok(lf.collect()?)
}

/// Apply limit, tail, and descending pagination to a sorted DataFrame.
fn apply_pagination(
    df: DataFrame,
    limit: usize,
    tail: bool,
    descending: bool,
) -> Result<DataFrame> {
    let result = if tail {
        // Take last N rows, then optionally reverse for descending
        let n = limit.min(df.height());
        let tailed = df.tail(Some(n));
        if descending {
            tailed.sort(
                ["ts"],
                SortMultipleOptions::default().with_order_descending(true),
            )?
        } else {
            tailed
        }
    } else if descending {
        // Take first N from descending sort
        let sorted = df.sort(
            ["ts"],
            SortMultipleOptions::default().with_order_descending(true),
        )?;
        sorted.head(Some(limit))
    } else {
        df.head(Some(limit))
    };
    Ok(result)
}

/// Delete rows from a date-partitioned directory, optionally within a date range.
fn delete_from_partition(
    dir: &Path,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<usize> {
    if from.is_none() && to.is_none() {
        // Delete everything in the directory
        let count = count_all_rows_in_dir(dir);
        // Remove all parquet files but keep the directory
        for entry in fs::read_dir(dir)?.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "parquet") {
                fs::remove_file(&path)?;
            }
        }
        return Ok(count);
    }

    let files = list_date_files(dir, from, to)?;
    let mut total_deleted = 0usize;

    for file_path in &files {
        let df = read_parquet_file(file_path)?;
        let original_count = df.height();

        // Filter to keep rows OUTSIDE the delete range
        let filtered = apply_ts_filter_inverted(df, from, to)?;

        if filtered.height() == 0 {
            // All rows deleted — remove the file
            fs::remove_file(file_path)?;
            total_deleted += original_count;
        } else if filtered.height() < original_count {
            // Partial deletion — rewrite the file
            total_deleted += original_count - filtered.height();
            write_parquet_file(file_path, &mut filtered.clone())?;
        }
        // else: no rows matched the range in this file
    }

    Ok(total_deleted)
}

/// Filter to keep rows OUTSIDE a timestamp range (inverse of apply_ts_filter).
fn apply_ts_filter_inverted(
    df: DataFrame,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) -> Result<DataFrame> {
    let mut lf = df.lazy();

    match (from, to) {
        (Some(f), Some(t)) => {
            let from_micros = f.and_utc().timestamp_micros();
            let to_micros = t.and_utc().timestamp_micros();
            let from_lit = lit(from_micros).cast(DataType::Datetime(TimeUnit::Microseconds, None));
            let to_lit = lit(to_micros).cast(DataType::Datetime(TimeUnit::Microseconds, None));
            // Keep rows where ts < from OR ts > to
            lf = lf.filter(col("ts").lt(from_lit).or(col("ts").gt(to_lit)));
        }
        (Some(f), None) => {
            let from_micros = f.and_utc().timestamp_micros();
            let from_lit = lit(from_micros).cast(DataType::Datetime(TimeUnit::Microseconds, None));
            lf = lf.filter(col("ts").lt(from_lit));
        }
        (None, Some(t)) => {
            let to_micros = t.and_utc().timestamp_micros();
            let to_lit = lit(to_micros).cast(DataType::Datetime(TimeUnit::Microseconds, None));
            lf = lf.filter(col("ts").gt(to_lit));
        }
        (None, None) => {}
    }

    Ok(lf.collect()?)
}

/// Count all rows across parquet files in a directory (non-recursive).
fn count_all_rows_in_dir(dir: &Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "parquet") {
                if let Ok(df) = read_parquet_file(&path) {
                    count += df.height();
                }
            }
        }
    }
    count
}

/// Recursively walk a directory and collect all file paths.
fn walkdir(dir: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if !dir.exists() {
        return result;
    }
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                result.extend(walkdir(&path));
            } else {
                result.push(path);
            }
        }
    }
    result
}

/// Convert microsecond epoch to NaiveDateTime.
fn micros_to_ndt(micros: i64) -> NaiveDateTime {
    let secs = micros / 1_000_000;
    let nsecs = ((micros % 1_000_000) * 1_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs)
        .map(|dt| dt.naive_utc())
        .unwrap_or_default()
}
