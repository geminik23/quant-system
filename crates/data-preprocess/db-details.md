# Database Schema Reference

This document describes the DuckDB database schema used by `data-preprocess`. If you are building a backtest engine or analysis tool that reads from the `market_data.duckdb` file directly, this is what you need.

## Database File

- **Engine**: DuckDB (embedded, single-file)
- **Default path**: `./market_data.duckdb` (configurable via `--db` flag or `DATA_PREPROCESS_DB` env var)
- **Access**: Open read-only from your app while the CLI is not writing. DuckDB supports concurrent readers.

## Tables

### `ticks`

Raw bid/ask/last price ticks with millisecond precision.

| Column     | Type    | Nullable | Description                              |
|------------|---------|----------|------------------------------------------|
| `exchange` | VARCHAR | NOT NULL | Broker / exchange name, always lowercase (`ctrader`, `binance`, `coinbase`) |
| `symbol`   | VARCHAR | NOT NULL | Instrument name, always uppercase (`BTCUSD`, `EURUSD`, `XAUUSD`) |
| `ts`       | VARCHAR | NOT NULL | Timestamp in UTC, ISO 8601 format (see [Timestamp Format](#timestamp-format)) |
| `bid`      | DOUBLE  | nullable | Bid price. `NULL` if not present in source data |
| `ask`      | DOUBLE  | nullable | Ask price. `NULL` if not present in source data |
| `last`     | DOUBLE  | nullable | Last traded price. `NULL` if not present in source data |
| `volume`   | DOUBLE  | nullable | Tick volume. `NULL` if not present in source data |
| `flags`    | INTEGER | nullable | Source-specific flags (e.g. CTrader tick type). `NULL` if not present |

**Unique constraint**: `(exchange, symbol, ts)`

### `bars`

OHLCV candle bars at various timeframes.

| Column     | Type    | Nullable | Description                              |
|------------|---------|----------|------------------------------------------|
| `exchange` | VARCHAR | NOT NULL | Broker / exchange name, always lowercase |
| `symbol`   | VARCHAR | NOT NULL | Instrument name, always uppercase        |
| `timeframe`| VARCHAR | NOT NULL | Bar period (see [Timeframe Values](#timeframe-values)) |
| `ts`       | VARCHAR | NOT NULL | Bar open time in UTC, ISO 8601 format    |
| `open`     | DOUBLE  | NOT NULL | Open price                               |
| `high`     | DOUBLE  | NOT NULL | High price                               |
| `low`      | DOUBLE  | NOT NULL | Low price                                |
| `close`    | DOUBLE  | NOT NULL | Close price                              |
| `tick_vol` | BIGINT  | DEFAULT 0| Tick volume (number of ticks in the bar)  |
| `volume`   | BIGINT  | DEFAULT 0| Real volume (0 if not available)          |
| `spread`   | INTEGER | DEFAULT 0| Spread in points (0 if not available)     |

**Unique constraint**: `(exchange, symbol, timeframe, ts)`

## Timestamp Format

All timestamps are stored as **VARCHAR** strings in UTC, using ISO 8601 format:

```
2026-02-16 17:00:00.083000000    (ticks — with sub-second precision)
2026-02-20 22:45:00              (bars — second precision)
```

Timestamps are lexicographically sortable, so string comparison (`>=`, `<=`) works correctly for time range queries.

**Parsing in your code:**

| Language | Format string                    |
|----------|----------------------------------|
| Rust     | `%Y-%m-%d %H:%M:%S%.f` (chrono)  |
| Python   | `%Y-%m-%d %H:%M:%S.%f` or `%Y-%m-%d %H:%M:%S` |
| SQL      | `CAST(ts AS TIMESTAMP)` if you need native timestamp ops |

## Timeframe Values

The `timeframe` column in the `bars` table uses these string values:

| Value | Meaning    |
|-------|------------|
| `1m`  | 1 minute   |
| `3m`  | 3 minutes  |
| `5m`  | 5 minutes  |
| `15m` | 15 minutes |
| `30m` | 30 minutes |
| `1h`  | 1 hour     |
| `4h`  | 4 hours    |
| `1d`  | 1 day      |
| `1w`  | 1 week     |
| `1M`  | 1 month    |

## Query Examples

### Bulk load ticks for backtesting

The typical backtest query — one exchange, one symbol, full time range:

```sql
SELECT ts, bid, ask, last, volume
FROM ticks
WHERE exchange = 'ctrader'
  AND symbol = 'BTCUSD'
  AND ts >= '2026-02-16 00:00:00'
  AND ts <= '2026-02-21 23:59:59'
ORDER BY ts;
```

### Bulk load bars for backtesting

```sql
SELECT ts, open, high, low, close, tick_vol, volume, spread
FROM bars
WHERE exchange = 'ctrader'
  AND symbol = 'BTCUSD'
  AND timeframe = '1m'
  AND ts >= '2026-02-20 00:00:00'
  AND ts <= '2026-02-21 23:59:59'
ORDER BY ts;
```

### List all available exchange+symbol combinations

```sql
SELECT DISTINCT exchange, symbol FROM ticks
UNION
SELECT DISTINCT exchange, symbol FROM bars
ORDER BY exchange, symbol;
```

### Count rows per exchange+symbol

```sql
SELECT exchange, symbol, COUNT(*) as rows
FROM ticks
GROUP BY exchange, symbol
ORDER BY exchange, symbol;
```

### Get date range for a specific dataset

```sql
SELECT MIN(ts) as first_tick, MAX(ts) as last_tick
FROM ticks
WHERE exchange = 'ctrader' AND symbol = 'BTCUSD';
```

## Client Examples

### Python (via `duckdb` package)

```python
import duckdb

conn = duckdb.connect("market_data.duckdb", read_only=True)

df = conn.execute("""
    SELECT ts, bid, ask
    FROM ticks
    WHERE exchange = 'ctrader'
      AND symbol = 'BTCUSD'
      AND ts >= '2026-02-16'
      AND ts <= '2026-02-21'
    ORDER BY ts
""").fetchdf()  # returns a pandas DataFrame

print(f"Loaded {len(df)} ticks")
print(df.head())
```

### Rust (via `duckdb` crate)

```rust
use duckdb::{Connection, params};

let conn = Connection::open_with_flags(
    "market_data.duckdb",
    duckdb::Config::default(),
)?;

let mut stmt = conn.prepare(
    "SELECT ts, bid, ask FROM ticks
     WHERE exchange = ? AND symbol = ?
       AND ts >= ? AND ts <= ?
     ORDER BY ts"
)?;

let rows = stmt.query_map(
    params!["ctrader", "BTCUSD", "2026-02-16", "2026-02-21"],
    |row| {
        Ok((
            row.get::<_, String>(0)?,  // ts
            row.get::<_, Option<f64>>(1)?,  // bid
            row.get::<_, Option<f64>>(2)?,  // ask
        ))
    },
)?;

for row in rows {
    let (ts, bid, ask) = row?;
    // feed into backtest engine
}
```

### Using the library crate directly

```rust
use data_preprocess::{Database, db::QueryOpts};

let db = Database::open("market_data.duckdb".as_ref())?;
let (ticks, total) = db.query_ticks(&QueryOpts {
    exchange: "ctrader".into(),
    symbol: "BTCUSD".into(),
    from: Some("2026-02-16T00:00:00".parse().unwrap()),
    to: Some("2026-02-21T23:59:59".parse().unwrap()),
    limit: 10_000_000,
    tail: false,
    descending: false,
})?;

println!("Loaded {} of {} total ticks", ticks.len(), total);
```

## Data Conventions

| Rule | Detail |
|------|--------|
| Exchange names | Always **lowercase**: `ctrader`, `binance`, `coinbase` |
| Symbol names | Always **UPPERCASE**: `BTCUSD`, `EURUSD`, `XAUUSD` |
| Timestamps | Always **UTC**. Source timezone is converted on import |
| Deduplication | `INSERT OR IGNORE` — duplicate `(exchange, symbol, ts)` rows are silently skipped |
| Nullable fields | `bid`, `ask`, `last`, `volume`, `flags` in ticks may be `NULL` |
| Bar fields | `open`, `high`, `low`, `close` are always present. `tick_vol`, `volume`, `spread` default to `0` |

## Notes

- The database file can be opened read-only by multiple processes simultaneously.
- Do not write to the database from your app — use the `data-preprocess` CLI for all writes to maintain schema and dedup invariants.
- For very large exports, DuckDB supports `COPY ... TO 'output.parquet' (FORMAT PARQUET)` natively.