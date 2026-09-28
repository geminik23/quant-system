use std::path::Path;

use chrono::NaiveDateTime;
use duckdb::{Connection, OptionalExt, params};

use crate::error::{DataError, Result};
use crate::models::*;

// DuckDB doesn't implement ToSql/FromSql for chrono types,
// so we serialize timestamps as strings in ISO format.
const TS_FMT: &str = "%Y-%m-%d %H:%M:%S%.f";
const TS_FMT_NO_FRAC: &str = "%Y-%m-%d %H:%M:%S";

fn ndt_to_string(ndt: &NaiveDateTime) -> String {
    ndt.format(TS_FMT).to_string()
}

fn string_to_ndt(s: &str) -> Result<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, TS_FMT)
        .or_else(|_| NaiveDateTime::parse_from_str(s, TS_FMT_NO_FRAC))
        .map_err(|e| DataError::InvalidTimestamp(format!("{s}: {e}")))
}

/// DuckDB-backed storage for ticks and bars.
pub struct Database {
    conn: Connection,
    db_path: Option<String>,
}

impl Database {
    /// Open (or create) a DuckDB database at the given path.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        let db = Self {
            conn,
            db_path: Some(path.display().to_string()),
        };
        db.init_schema()?;
        Ok(db)
    }

    /// Open an in-memory database (for testing).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Self {
            conn,
            db_path: None,
        };
        db.init_schema()?;
        Ok(db)
    }

    /// Create tables if they don't exist.
    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS ticks (
                exchange    VARCHAR NOT NULL,
                symbol      VARCHAR NOT NULL,
                ts          VARCHAR NOT NULL,
                bid         DOUBLE,
                ask         DOUBLE,
                last        DOUBLE,
                volume      DOUBLE,
                flags       INTEGER,
                UNIQUE (exchange, symbol, ts)
            );

            CREATE TABLE IF NOT EXISTS bars (
                exchange    VARCHAR NOT NULL,
                symbol      VARCHAR NOT NULL,
                timeframe   VARCHAR NOT NULL,
                ts          VARCHAR NOT NULL,
                open        DOUBLE NOT NULL,
                high        DOUBLE NOT NULL,
                low         DOUBLE NOT NULL,
                close       DOUBLE NOT NULL,
                tick_vol    BIGINT DEFAULT 0,
                volume      BIGINT DEFAULT 0,
                spread      INTEGER DEFAULT 0,
                UNIQUE (exchange, symbol, timeframe, ts)
            );

            CREATE TABLE IF NOT EXISTS ordered_ticks (
                exchange VARCHAR NOT NULL, symbol VARCHAR NOT NULL, ts VARCHAR NOT NULL,
                bid DOUBLE, ask DOUBLE, last DOUBLE, volume DOUBLE, flags INTEGER,
                source_ordinal BIGINT NOT NULL, source_identity VARCHAR, provider_sequence BIGINT,
                UNIQUE(exchange, symbol, source_ordinal)
            );

            CREATE TABLE IF NOT EXISTS price_bars (
                exchange VARCHAR NOT NULL, symbol VARCHAR NOT NULL, timeframe VARCHAR NOT NULL,
                timeframe_seconds BIGINT NOT NULL, ts VARCHAR NOT NULL, available_at VARCHAR NOT NULL,
                open DOUBLE NOT NULL, high DOUBLE NOT NULL, low DOUBLE NOT NULL, close DOUBLE NOT NULL,
                tick_count BIGINT, spread INTEGER,
                UNIQUE(exchange, symbol, timeframe_seconds, ts)
            );

            CREATE TABLE IF NOT EXISTS series_descriptors (
                exchange VARCHAR NOT NULL, symbol VARCHAR NOT NULL, timeframe_seconds BIGINT NOT NULL,
                descriptor_json VARCHAR NOT NULL,
                UNIQUE(exchange, symbol, timeframe_seconds)
            );
            ",
        )?;
        Ok(())
    }

    // ── Insert ──

    /// Bulk insert ticks using INSERT OR IGNORE for dedup.
    /// Returns the number of rows actually inserted.
    pub fn insert_ticks(&self, ticks: &[Tick]) -> Result<usize> {
        if ticks.is_empty() {
            return Ok(0);
        }
        let exchange = &ticks[0].exchange;
        let symbol = &ticks[0].symbol;
        let count_before = self.count_ticks(exchange, symbol)?;

        self.conn.execute_batch("BEGIN TRANSACTION")?;
        let mut stmt = self.conn.prepare(
            "INSERT OR IGNORE INTO ticks (exchange, symbol, ts, bid, ask, last, volume, flags)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )?;
        for tick in ticks {
            let ts_str = ndt_to_string(&tick.ts);
            stmt.execute(params![
                tick.exchange,
                tick.symbol,
                ts_str,
                tick.bid,
                tick.ask,
                tick.last,
                tick.volume,
                tick.flags,
            ])?;
        }
        drop(stmt);
        self.conn.execute_batch("COMMIT")?;

        let count_after = self.count_ticks(exchange, symbol)?;
        Ok((count_after - count_before) as usize)
    }

    pub fn insert_stored_ticks(&self, ticks: &[StoredTick]) -> Result<usize> {
        let mut inserted = 0usize;
        for row in ticks {
            row.validate()?;
            let ordinal = i64::try_from(row.source_ordinal)
                .map_err(|_| DataError::Other("source ordinal exceeds DuckDB BIGINT".into()))?;
            if let (Some(source), Some(sequence)) = (&row.source_identity, row.provider_sequence) {
                let mut stmt=self.conn.prepare("SELECT exchange,symbol,ts,bid,ask,last,volume,flags FROM ordered_ticks WHERE source_identity=? AND provider_sequence=?")?;
                let mut rows = stmt.query(params![
                    source,
                    i64::try_from(sequence).map_err(|_| DataError::Other(
                        "provider sequence exceeds DuckDB BIGINT".into()
                    ))?
                ])?;
                if let Some(existing) = rows.next()? {
                    let tick = Tick {
                        exchange: existing.get(0)?,
                        symbol: existing.get(1)?,
                        ts: string_to_ndt(&existing.get::<_, String>(2)?)?,
                        bid: existing.get(3)?,
                        ask: existing.get(4)?,
                        last: existing.get(5)?,
                        volume: existing.get(6)?,
                        flags: existing.get(7)?,
                    };
                    if tick == row.tick {
                        continue;
                    } else {
                        return Err(DataError::Other(
                            "conflicting payload for provider sequence".into(),
                        ));
                    }
                }
            }
            let changed=self.conn.execute("INSERT INTO ordered_ticks(exchange,symbol,ts,bid,ask,last,volume,flags,source_ordinal,source_identity,provider_sequence) VALUES(?,?,?,?,?,?,?,?,?,?,?)",params![row.tick.exchange,row.tick.symbol,ndt_to_string(&row.tick.ts),row.tick.bid,row.tick.ask,row.tick.last,row.tick.volume,row.tick.flags,ordinal,row.source_identity,row.provider_sequence.map(i64::try_from).transpose().map_err(|_|DataError::Other("provider sequence exceeds DuckDB BIGINT".into()))?])?;
            inserted += changed;
        }
        Ok(inserted)
    }

    pub fn query_stored_ticks(&self, exchange: &str, symbol: &str) -> Result<Vec<StoredTick>> {
        let mut stmt=self.conn.prepare("SELECT exchange,symbol,ts,bid,ask,last,volume,flags,source_ordinal,source_identity,provider_sequence FROM ordered_ticks WHERE exchange=? AND symbol=? ORDER BY ts,source_ordinal")?;
        let rows = stmt.query_map(params![exchange, symbol], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get::<_, i64>(8)?,
                row.get(9)?,
                row.get::<_, Option<i64>>(10)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (
                exchange,
                symbol,
                ts,
                bid,
                ask,
                last,
                volume,
                flags,
                ordinal,
                source_identity,
                provider_sequence,
            ) = row?;
            result.push(StoredTick {
                tick: Tick {
                    exchange,
                    symbol,
                    ts: string_to_ndt(&ts)?,
                    bid,
                    ask,
                    last,
                    volume,
                    flags,
                },
                source_ordinal: u64::try_from(ordinal)
                    .map_err(|_| DataError::Other("negative stored ordinal".into()))?,
                source_identity,
                provider_sequence: provider_sequence
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| DataError::Other("negative provider sequence".into()))?,
            })
        }
        Ok(result)
    }

    pub fn insert_price_bars(
        &self,
        descriptor: &SeriesDescriptor,
        bars: &[PriceBar],
    ) -> Result<usize> {
        descriptor.validate()?;
        if !descriptor.verified {
            return Err(DataError::Other(
                "new DuckDB price bars require verified descriptor".into(),
            ));
        }
        let seconds = i64::try_from(descriptor.timeframe_seconds)
            .map_err(|_| DataError::Other("timeframe exceeds DuckDB BIGINT".into()))?;
        let encoded = serde_json::to_string(descriptor)
            .map_err(|error| DataError::Other(error.to_string()))?;
        let existing:Option<String>=self.conn.query_row("SELECT descriptor_json FROM series_descriptors WHERE exchange=? AND symbol=? AND timeframe_seconds=?",params![descriptor.exchange,descriptor.symbol,seconds],|row|row.get(0)).optional()?;
        if existing.as_ref().is_some_and(|value| value != &encoded) {
            return Err(DataError::Other("DuckDB series descriptor conflict".into()));
        }
        if existing.is_none() {
            self.conn.execute(
                "INSERT INTO series_descriptors VALUES(?,?,?,?)",
                params![descriptor.exchange, descriptor.symbol, seconds, encoded],
            )?;
        }
        let mut inserted = 0;
        for bar in bars {
            bar.validate()?;
            let count = bar
                .tick_count
                .map(i64::try_from)
                .transpose()
                .map_err(|_| DataError::Other("tick count exceeds DuckDB BIGINT".into()))?;
            inserted+=self.conn.execute("INSERT OR IGNORE INTO price_bars(exchange,symbol,timeframe,timeframe_seconds,ts,available_at,open,high,low,close,tick_count,spread) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",params![bar.exchange,bar.symbol,bar.timeframe.as_str(),seconds,ndt_to_string(&bar.ts),ndt_to_string(&bar.available_at),bar.open,bar.high,bar.low,bar.close,count,bar.spread])?;
        }
        Ok(inserted)
    }

    pub fn query_price_bars(&self, descriptor: &SeriesDescriptor) -> Result<Vec<PriceBar>> {
        descriptor.validate()?;
        let seconds = i64::try_from(descriptor.timeframe_seconds)
            .map_err(|_| DataError::Other("timeframe exceeds DuckDB BIGINT".into()))?;
        let encoded = serde_json::to_string(descriptor)
            .map_err(|error| DataError::Other(error.to_string()))?;
        let stored:Option<String>=self.conn.query_row("SELECT descriptor_json FROM series_descriptors WHERE exchange=? AND symbol=? AND timeframe_seconds=?",params![descriptor.exchange,descriptor.symbol,seconds],|row|row.get(0)).optional()?;
        if stored.as_ref().is_some_and(|value| value != &encoded) {
            return Err(DataError::Other("DuckDB series descriptor conflict".into()));
        }
        let mut stmt=self.conn.prepare("SELECT timeframe,ts,available_at,open,high,low,close,tick_count,spread FROM price_bars WHERE exchange=? AND symbol=? AND timeframe_seconds=? ORDER BY ts")?;
        let rows = stmt.query_map(
            params![descriptor.exchange, descriptor.symbol, seconds],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get(8)?,
                ))
            },
        )?;
        let mut result = Vec::new();
        for row in rows {
            let (tf, ts, available_at, open, high, low, close, count, spread) = row?;
            result.push(PriceBar {
                exchange: descriptor.exchange.clone(),
                symbol: descriptor.symbol.clone(),
                timeframe: Timeframe::parse(&tf)?,
                ts: string_to_ndt(&ts)?,
                available_at: string_to_ndt(&available_at)?,
                open,
                high,
                low,
                close,
                tick_count: count
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| DataError::Other("negative DuckDB count".into()))?,
                spread,
            })
        }
        Ok(result)
    }

    /// Bulk insert bars using INSERT OR IGNORE for dedup.
    /// Returns the number of rows actually inserted.
    pub fn insert_bars(&self, bars: &[Bar]) -> Result<usize> {
        if bars.is_empty() {
            return Ok(0);
        }
        let exchange = &bars[0].exchange;
        let symbol = &bars[0].symbol;
        let timeframe = bars[0].timeframe.as_str();
        let count_before = self.count_bars(exchange, symbol, timeframe)?;

        self.conn.execute_batch("BEGIN TRANSACTION")?;
        let mut stmt = self.conn.prepare(
            "INSERT OR IGNORE INTO bars
             (exchange, symbol, timeframe, ts, open, high, low, close, tick_vol, volume, spread)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )?;
        for bar in bars {
            let ts_str = ndt_to_string(&bar.ts);
            stmt.execute(params![
                bar.exchange,
                bar.symbol,
                bar.timeframe.as_str(),
                ts_str,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.tick_vol,
                bar.volume,
                bar.spread,
            ])?;
        }
        drop(stmt);
        self.conn.execute_batch("COMMIT")?;

        let count_after = self.count_bars(exchange, symbol, timeframe)?;
        Ok((count_after - count_before) as usize)
    }

    // ── Delete ──

    /// Delete ticks matching exchange+symbol, optionally within a date range.
    pub fn delete_ticks(
        &self,
        exchange: &str,
        symbol: &str,
        from: Option<NaiveDateTime>,
        to: Option<NaiveDateTime>,
    ) -> Result<usize> {
        let mut sql = "DELETE FROM ticks WHERE exchange = ? AND symbol = ?".to_string();
        let mut p: Vec<Box<dyn duckdb::types::ToSql>> =
            vec![Box::new(exchange.to_string()), Box::new(symbol.to_string())];
        if let Some(f) = from {
            sql.push_str(" AND ts >= ?");
            p.push(Box::new(ndt_to_string(&f)));
        }
        if let Some(t) = to {
            sql.push_str(" AND ts <= ?");
            p.push(Box::new(ndt_to_string(&t)));
        }
        let refs: Vec<&dyn duckdb::types::ToSql> = p.iter().map(|b| b.as_ref()).collect();
        Ok(self.conn.execute(&sql, refs.as_slice())?)
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
        let mut sql =
            "DELETE FROM bars WHERE exchange = ? AND symbol = ? AND timeframe = ?".to_string();
        let mut p: Vec<Box<dyn duckdb::types::ToSql>> = vec![
            Box::new(exchange.to_string()),
            Box::new(symbol.to_string()),
            Box::new(timeframe.to_string()),
        ];
        if let Some(f) = from {
            sql.push_str(" AND ts >= ?");
            p.push(Box::new(ndt_to_string(&f)));
        }
        if let Some(t) = to {
            sql.push_str(" AND ts <= ?");
            p.push(Box::new(ndt_to_string(&t)));
        }
        let refs: Vec<&dyn duckdb::types::ToSql> = p.iter().map(|b| b.as_ref()).collect();
        Ok(self.conn.execute(&sql, refs.as_slice())?)
    }

    /// Delete ALL data (ticks + bars) for an exchange+symbol pair.
    pub fn delete_symbol(&self, exchange: &str, symbol: &str) -> Result<(usize, usize)> {
        let t = self.conn.execute(
            "DELETE FROM ticks WHERE exchange = ? AND symbol = ?",
            params![exchange, symbol],
        )?;
        let b = self.conn.execute(
            "DELETE FROM bars WHERE exchange = ? AND symbol = ?",
            params![exchange, symbol],
        )?;
        Ok((t, b))
    }

    /// Delete ALL data for an entire exchange.
    pub fn delete_exchange(&self, exchange: &str) -> Result<(usize, usize)> {
        let t = self
            .conn
            .execute("DELETE FROM ticks WHERE exchange = ?", params![exchange])?;
        let b = self
            .conn
            .execute("DELETE FROM bars WHERE exchange = ?", params![exchange])?;
        Ok((t, b))
    }

    // ── Query ──

    /// Get summary statistics, optionally filtered by exchange and/or symbol.
    pub fn stats(&self, exchange: Option<&str>, symbol: Option<&str>) -> Result<Vec<StatRow>> {
        let where_clause = match (exchange, symbol) {
            (Some(_), Some(_)) => "WHERE exchange = ? AND symbol = ?",
            (Some(_), None) => "WHERE exchange = ?",
            (None, Some(_)) => "WHERE symbol = ?",
            (None, None) => "",
        };

        let sql = format!(
            "SELECT exchange, symbol, 'tick' as data_type, COUNT(*) as count,
                    MIN(ts) as ts_min, MAX(ts) as ts_max
             FROM ticks {where_clause}
             GROUP BY exchange, symbol
             UNION ALL
             SELECT exchange, symbol, 'bar (' || timeframe || ')' as data_type, COUNT(*) as count,
                    MIN(ts) as ts_min, MAX(ts) as ts_max
             FROM bars {where_clause}
             GROUP BY exchange, symbol, timeframe
             ORDER BY exchange, symbol, data_type"
        );

        let map_row = |row: &duckdb::Row| -> std::result::Result<StatRow, duckdb::Error> {
            let ts_min_str: String = row.get(4)?;
            let ts_max_str: String = row.get(5)?;
            Ok(StatRow {
                exchange: row.get(0)?,
                symbol: row.get(1)?,
                data_type: row.get(2)?,
                count: row.get::<_, i64>(3)? as u64,
                ts_min: string_to_ndt(&ts_min_str).unwrap_or_default(),
                ts_max: string_to_ndt(&ts_max_str).unwrap_or_default(),
            })
        };

        let mut stmt = self.conn.prepare(&sql)?;

        // Bind params for both halves of the UNION ALL
        let rows: Vec<StatRow> = match (exchange, symbol) {
            (Some(ex), Some(sym)) => stmt
                .query_map(params![ex, sym, ex, sym], map_row)?
                .filter_map(|r| r.ok())
                .collect(),
            (Some(ex), None) => stmt
                .query_map(params![ex, ex], map_row)?
                .filter_map(|r| r.ok())
                .collect(),
            (None, Some(sym)) => stmt
                .query_map(params![sym, sym], map_row)?
                .filter_map(|r| r.ok())
                .collect(),
            (None, None) => stmt
                .query_map([], map_row)?
                .filter_map(|r| r.ok())
                .collect(),
        };
        Ok(rows)
    }

    /// Query ticks with filtering and pagination.
    pub fn query_ticks(&self, opts: &QueryOpts) -> Result<(Vec<Tick>, u64)> {
        let total = self.count_filtered(
            "ticks",
            &opts.exchange,
            &opts.symbol,
            None,
            opts.from,
            opts.to,
        )?;

        let order = if opts.descending { "DESC" } else { "ASC" };
        let (mut where_parts, mut bind_vals) = base_where(&opts.exchange, &opts.symbol);
        append_ts_filters(&mut where_parts, &mut bind_vals, opts.from, opts.to);
        let where_sql = where_parts.join(" AND ");

        let sql = if opts.tail {
            format!(
                "SELECT * FROM (
                    SELECT exchange, symbol, ts, bid, ask, last, volume, flags
                    FROM ticks WHERE {where_sql} ORDER BY ts DESC LIMIT ?
                 ) sub ORDER BY ts {order}"
            )
        } else {
            format!(
                "SELECT exchange, symbol, ts, bid, ask, last, volume, flags
                 FROM ticks WHERE {where_sql} ORDER BY ts {order} LIMIT ?"
            )
        };
        bind_vals.push(BVal::Int(opts.limit as i64));

        let mut stmt = self.conn.prepare(&sql)?;
        let ticks = exec_query(&mut stmt, &bind_vals, |row| {
            let ts_str: String = row.get(2)?;
            Ok(Tick {
                exchange: row.get(0)?,
                symbol: row.get(1)?,
                ts: string_to_ndt(&ts_str).unwrap_or_default(),
                bid: row.get(3)?,
                ask: row.get(4)?,
                last: row.get(5)?,
                volume: row.get(6)?,
                flags: row.get(7)?,
            })
        })?;
        Ok((ticks, total))
    }

    /// Query bars with filtering and pagination.
    pub fn query_bars(&self, opts: &BarQueryOpts) -> Result<(Vec<Bar>, u64)> {
        let total = self.count_filtered(
            "bars",
            &opts.exchange,
            &opts.symbol,
            Some(&opts.timeframe),
            opts.from,
            opts.to,
        )?;

        let order = if opts.descending { "DESC" } else { "ASC" };
        let (mut where_parts, mut bind_vals) = base_where(&opts.exchange, &opts.symbol);
        where_parts.push("timeframe = ?".to_string());
        bind_vals.push(BVal::Str(opts.timeframe.clone()));
        append_ts_filters(&mut where_parts, &mut bind_vals, opts.from, opts.to);
        let where_sql = where_parts.join(" AND ");

        let sql = if opts.tail {
            format!(
                "SELECT * FROM (
                    SELECT exchange, symbol, timeframe, ts, open, high, low, close,
                           tick_vol, volume, spread
                    FROM bars WHERE {where_sql} ORDER BY ts DESC LIMIT ?
                 ) sub ORDER BY ts {order}"
            )
        } else {
            format!(
                "SELECT exchange, symbol, timeframe, ts, open, high, low, close,
                        tick_vol, volume, spread
                 FROM bars WHERE {where_sql} ORDER BY ts {order} LIMIT ?"
            )
        };
        bind_vals.push(BVal::Int(opts.limit as i64));

        let mut stmt = self.conn.prepare(&sql)?;
        let bars = exec_query(&mut stmt, &bind_vals, |row| {
            let tf_str: String = row.get(2)?;
            let ts_str: String = row.get(3)?;
            Ok(Bar {
                exchange: row.get(0)?,
                symbol: row.get(1)?,
                timeframe: Timeframe::parse(&tf_str).unwrap_or(Timeframe::M1),
                ts: string_to_ndt(&ts_str).unwrap_or_default(),
                open: row.get(4)?,
                high: row.get(5)?,
                low: row.get(6)?,
                close: row.get(7)?,
                tick_vol: row.get(8)?,
                volume: row.get(9)?,
                spread: row.get(10)?,
            })
        })?;
        Ok((bars, total))
    }

    /// Get database file size in bytes (None for in-memory).
    pub fn file_size(&self) -> Option<u64> {
        self.db_path
            .as_ref()
            .and_then(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
    }

    // ── Private helpers ──

    fn count_ticks(&self, exchange: &str, symbol: &str) -> Result<u64> {
        let c: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM ticks WHERE exchange = ? AND symbol = ?",
            params![exchange, symbol],
            |row| row.get(0),
        )?;
        Ok(c as u64)
    }

    fn count_bars(&self, exchange: &str, symbol: &str, timeframe: &str) -> Result<u64> {
        let c: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM bars WHERE exchange = ? AND symbol = ? AND timeframe = ?",
            params![exchange, symbol, timeframe],
            |row| row.get(0),
        )?;
        Ok(c as u64)
    }

    fn count_filtered(
        &self,
        table: &str,
        exchange: &str,
        symbol: &str,
        timeframe: Option<&str>,
        from: Option<NaiveDateTime>,
        to: Option<NaiveDateTime>,
    ) -> Result<u64> {
        let (mut parts, mut vals) = base_where(exchange, symbol);
        if let Some(tf) = timeframe {
            parts.push("timeframe = ?".to_string());
            vals.push(BVal::Str(tf.to_string()));
        }
        append_ts_filters(&mut parts, &mut vals, from, to);
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE {}",
            table,
            parts.join(" AND ")
        );
        count_with_binds(&self.conn, &sql, &vals)
    }
}

// ── Bind-value helpers ──
// We use a small enum so we can build dynamic param lists at runtime.

enum BVal {
    Str(String),
    Int(i64),
}

fn base_where(exchange: &str, symbol: &str) -> (Vec<String>, Vec<BVal>) {
    (
        vec!["exchange = ?".to_string(), "symbol = ?".to_string()],
        vec![
            BVal::Str(exchange.to_string()),
            BVal::Str(symbol.to_string()),
        ],
    )
}

fn append_ts_filters(
    parts: &mut Vec<String>,
    vals: &mut Vec<BVal>,
    from: Option<NaiveDateTime>,
    to: Option<NaiveDateTime>,
) {
    if let Some(f) = from {
        parts.push("ts >= ?".to_string());
        vals.push(BVal::Str(ndt_to_string(&f)));
    }
    if let Some(t) = to {
        parts.push("ts <= ?".to_string());
        vals.push(BVal::Str(ndt_to_string(&t)));
    }
}

/// Convert BVal slice into boxed ToSql trait objects, then ref-slice for duckdb.
fn to_dyn_params(binds: &[BVal]) -> Vec<Box<dyn duckdb::types::ToSql>> {
    binds
        .iter()
        .map(|b| -> Box<dyn duckdb::types::ToSql> {
            match b {
                BVal::Str(s) => Box::new(s.clone()),
                BVal::Int(n) => Box::new(*n),
            }
        })
        .collect()
}

/// Execute a SELECT with dynamic binds and map each row.
fn exec_query<T, F>(stmt: &mut duckdb::Statement, binds: &[BVal], map_fn: F) -> Result<Vec<T>>
where
    F: Fn(&duckdb::Row) -> std::result::Result<T, duckdb::Error>,
{
    let params = to_dyn_params(binds);
    let refs: Vec<&dyn duckdb::types::ToSql> = params.iter().map(|b| b.as_ref()).collect();
    let rows = stmt.query_map(refs.as_slice(), &map_fn)?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

/// Execute a COUNT(*) query with dynamic binds.
fn count_with_binds(conn: &Connection, sql: &str, binds: &[BVal]) -> Result<u64> {
    let params = to_dyn_params(binds);
    let refs: Vec<&dyn duckdb::types::ToSql> = params.iter().map(|b| b.as_ref()).collect();
    let c: i64 = conn.query_row(sql, refs.as_slice(), |row| row.get(0))?;
    Ok(c as u64)
}
