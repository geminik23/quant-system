use std::path::Path;

use chrono::NaiveDateTime;
use duckdb::{Connection, params};

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

/// Query parameters for tick view commands.
pub struct QueryOpts {
    pub exchange: String,
    pub symbol: String,
    pub from: Option<NaiveDateTime>,
    pub to: Option<NaiveDateTime>,
    pub limit: usize,
    pub tail: bool,
    pub descending: bool,
}

/// Query parameters for bar view commands.
pub struct BarQueryOpts {
    pub exchange: String,
    pub symbol: String,
    pub timeframe: String,
    pub from: Option<NaiveDateTime>,
    pub to: Option<NaiveDateTime>,
    pub limit: usize,
    pub tail: bool,
    pub descending: bool,
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
