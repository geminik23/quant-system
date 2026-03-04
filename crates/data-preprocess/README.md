# data-preprocess

Historical market data storage and preprocessing CLI. Imports tick and OHLCV bar data from CSV files into Parquet (default) or DuckDB storage, with support for multiple exchanges, deduplication, querying, and management.

## Storage Backends

| Backend | Feature Flag | Default | Build Time | Description |
|---------|-------------|---------|------------|-------------|
| **Parquet + Polars** | `parquet` | ✅ Yes | ~30s | Hive-partitioned Parquet files. No C++ compilation. zstd compressed. |
| **DuckDB** | `duckdb-backend` | No | ~150s | Embedded columnar database. Opt-in for SQL exploration. |

### Parquet Directory Layout (Hive-Style Partitioning)

```
{data_dir}/
├── ticks/
│   └── exchange={exchange}/
│       └── symbol={symbol}/
│           ├── 2026-01-15.parquet
│           └── 2026-01-16.parquet
└── bars/
    └── exchange={exchange}/
        └── symbol={symbol}/
            └── timeframe={timeframe}/
                ├── 2026-01-15.parquet
                └── 2026-01-16.parquet
```

Each file covers one date for one exchange+symbol (or exchange+symbol+timeframe for bars). Files are sorted by timestamp ascending and compressed with zstd.

## Quick Start

```bash
# Build (default: parquet backend)
cargo build -p qs-data-preprocess

# Import tick data (symbol extracted from filename, UTC+2 default)
data-preprocess input tick --exchange ctrader BTCUSD_202602161900_202602210954.csv

# Import bar data (timeframe required)
data-preprocess input bar --exchange ctrader --timeframe 1m BTCUSD_M1_202602210045_202602211009.csv

# View statistics
data-preprocess stats

# Query ticks
data-preprocess view tick --exchange ctrader --symbol BTCUSD --limit 20 --tail

# Query bars
data-preprocess view bar --exchange ctrader --symbol BTCUSD --timeframe 1m --limit 20

# Remove data
data-preprocess remove tick --exchange ctrader --symbol BTCUSD --from 2026-02-16 --to 2026-02-18
data-preprocess remove symbol --exchange ctrader BTCUSD
data-preprocess remove exchange binance

# Run tests (parquet only, default)
cargo test -p qs-data-preprocess

# Run tests (both backends)
cargo test -p qs-data-preprocess --features duckdb-backend

# Use DuckDB backend at runtime
data-preprocess --backend duckdb --db market_data.duckdb stats
```

## CLI Reference

```
data-preprocess [OPTIONS] <COMMAND>

Global options:
  --backend <parquet|duckdb>   Storage backend [default: parquet]
  --data-dir <PATH>            Root directory for Parquet files [default: market_data]
                               Also reads DATA_PREPROCESS_DIR env var
  --db <PATH>                  Path to DuckDB file (duckdb backend only) [default: market_data.duckdb]
                               Also reads DATA_PREPROCESS_DB env var

Commands:
  input          Import market data from CSV file(s)
  remove         Remove data by exchange / symbol / type / date range
  stats          Show summary statistics
  view           Query and display stored data
```

### `input tick`

```
data-preprocess input tick [OPTIONS] <FILES>...

  -e, --exchange <EX>      Exchange name (REQUIRED)
      --symbol <SYM>       Override symbol (default: from filename)
      --tz-offset <TZ>     Source timezone offset [default: +02:00]
```

### `input bar`

```
data-preprocess input bar [OPTIONS] <FILES>...

  -e, --exchange <EX>      Exchange name (REQUIRED)
  -t, --timeframe <TF>     Timeframe: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M (REQUIRED)
      --symbol <SYM>       Override symbol (default: from filename)
      --tz-offset <TZ>     Source timezone offset [default: +02:00]
```

### `stats`

```
data-preprocess stats [--exchange <EX>] [--symbol <SYM>]
```

### `view tick` / `view bar`

```
data-preprocess view tick -e <EX> --symbol <SYM> [--from <DT>] [--to <DT>] [--limit N] [--tail] [--desc]
data-preprocess view bar  -e <EX> --symbol <SYM> -t <TF> [--from <DT>] [--to <DT>] [--limit N] [--tail] [--desc]
```

### `remove`

```
data-preprocess remove tick     -e <EX> --symbol <SYM> [--from <DT>] [--to <DT>]
data-preprocess remove bar      -e <EX> --symbol <SYM> -t <TF> [--from <DT>] [--to <DT>]
data-preprocess remove symbol   -e <EX> <SYMBOL>
data-preprocess remove exchange <EXCHANGE>
```

## Input CSV Formats

### Tick CSV

Tab-delimited, with header. Filename convention: `{SYMBOL}_*.csv`

```
<DATE>	<TIME>	<BID>	<ASK>	<LAST>	<VOLUME>	<FLAGS>
2026.02.16	19:00:00.083	67849.69	67861.69			6
```

### Bar CSV

Tab-delimited, with header. Filename convention: `{SYMBOL}_*.csv`

```
<DATE>	<TIME>	<OPEN>	<HIGH>	<LOW>	<CLOSE>	<TICKVOL>	<VOL>	<SPREAD>
2026.02.21	00:45:00	67932.44	67934.19	67888.89	67910.24	184	0	1200
```

## Data Conventions

- **Exchanges** are always stored lowercase (`ctrader`, `binance`)
- **Symbols** are always stored uppercase (`BTCUSD`, `EURUSD`)
- **Timestamps** are stored in UTC — source timezone is converted on import
- **Deduplication** uses `(exchange, symbol, ts)` for ticks and `(exchange, symbol, timeframe, ts)` for bars
  - Parquet: read-merge-write per date partition file (bounded to one file per dedup operation)
  - DuckDB: `INSERT OR IGNORE` with UNIQUE constraints

## Library Usage

### Parquet backend (default)

```rust
use data_preprocess::{ParquetStore, models::{QueryOpts, BarQueryOpts}};

let store = ParquetStore::open("market_data")?;

// Import ticks
let inserted = store.insert_ticks(&ticks)?;

// Query ticks
let (ticks, total) = store.query_ticks(&QueryOpts {
    exchange: "ctrader".into(),
    symbol: "BTCUSD".into(),
    from: None,
    to: None,
    limit: 1000,
    tail: false,
    descending: false,
})?;

// Stats
let stats = store.stats(None, None)?;

// Delete
let deleted = store.delete_ticks("ctrader", "BTCUSD", None, None)?;
let (tick_count, bar_count) = store.delete_symbol("ctrader", "BTCUSD")?;
```

### DuckDB backend (opt-in)

Requires `features = ["duckdb-backend"]` in your `Cargo.toml`.

```rust
use data_preprocess::{Database, models::{QueryOpts, BarQueryOpts}};

let db = Database::open("market_data.duckdb".as_ref())?;
let (ticks, total) = db.query_ticks(&QueryOpts {
    exchange: "ctrader".into(),
    symbol: "BTCUSD".into(),
    from: None,
    to: None,
    limit: 1000,
    tail: false,
    descending: false,
})?;
```

### Consumer crates (models only, no backend)

For crates that only need the type definitions (`Tick`, `Bar`, `Timeframe`, `QueryOpts`), disable default features to avoid pulling in Polars:

```toml
[dependencies]
qs-data-preprocess = { path = "../data-preprocess", default-features = false }
```

## Feature Flags

| Feature | Dependencies | Use Case |
|---------|-------------|----------|
| `parquet` (default) | `polars` | Parquet read/write with Polars |
| `duckdb-backend` | `duckdb` | DuckDB embedded database |
| _(none)_ | — | Model types + parsers only (fastest build) |

## API Parity

Both backends provide the same logical operations with identical return types:

| Operation | `ParquetStore` | `Database` |
|-----------|---------------|-----------|
| Open | `open(root_dir)` | `open(file_path)` |
| Insert ticks | `insert_ticks(&[Tick]) -> usize` | `insert_ticks(&[Tick]) -> usize` |
| Insert bars | `insert_bars(&[Bar]) -> usize` | `insert_bars(&[Bar]) -> usize` |
| Query ticks | `query_ticks(&QueryOpts) -> (Vec<Tick>, u64)` | `query_ticks(&QueryOpts) -> (Vec<Tick>, u64)` |
| Query bars | `query_bars(&BarQueryOpts) -> (Vec<Bar>, u64)` | `query_bars(&BarQueryOpts) -> (Vec<Bar>, u64)` |
| Delete ticks | `delete_ticks(ex, sym, from, to) -> usize` | `delete_ticks(ex, sym, from, to) -> usize` |
| Delete bars | `delete_bars(ex, sym, tf, from, to) -> usize` | `delete_bars(ex, sym, tf, from, to) -> usize` |
| Delete symbol | `delete_symbol(ex, sym) -> (usize, usize)` | `delete_symbol(ex, sym) -> (usize, usize)` |
| Delete exchange | `delete_exchange(ex) -> (usize, usize)` | `delete_exchange(ex) -> (usize, usize)` |
| Stats | `stats(ex?, sym?) -> Vec<StatRow>` | `stats(ex?, sym?) -> Vec<StatRow>` |
| Size | `total_size() -> Option<u64>` | `file_size() -> Option<u64>` |

## Used By

| Crate | How |
|-------|-----|
| `qs-backtest` | `default-features = false` — imports `Tick`, `Bar`, `Timeframe`, `QueryOpts`, `BarQueryOpts` model types only (no Polars/DuckDB). Uses `ticks_to_feed()` / `bars_to_feed()` converters. |
| `qs-backtest-server` | Full `parquet` feature — opens `ParquetStore` at runtime to query ticks/bars for backtest requests. Uses `stats()` for the `list_symbols` RPC. |

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)