# data-preprocess

Historical market data storage and preprocessing CLI. Imports tick and OHLCV bar data from CSV files into a local DuckDB database, with support for multiple exchanges, deduplication, querying, and management.

## Features

- Import tick (bid/ask/last) and bar (OHLCV) data from tab-delimited CSV files
- DuckDB embedded storage — single-file, no server required
- Exchange-partitioned data — same symbol on different exchanges stored independently
- Automatic deduplication on import (idempotent re-imports)
- Timezone conversion from source offset to UTC
- Symbol auto-extraction from filenames
- Query and display stored data with filtering, pagination, and sort options
- Delete by exchange, symbol, timeframe, or date range

## Quick Start

```bash
# Build
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

# Run tests
cargo test -p qs-data-preprocess
```

## CLI Reference

```
data-preprocess [--db <PATH>] <COMMAND>

Global options:
  --db <PATH>    Path to DuckDB file [default: market_data.duckdb]
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
- **Deduplication** uses `INSERT OR IGNORE` on `(exchange, symbol, ts)` for ticks and `(exchange, symbol, timeframe, ts)` for bars

For full schema details, query examples, and client integration guides (Python, Rust), see [db-details.md](db-details.md).

## Library Usage

The crate also exposes a library for programmatic access:

```rust
use data_preprocess::{Database, db::QueryOpts};

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

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)