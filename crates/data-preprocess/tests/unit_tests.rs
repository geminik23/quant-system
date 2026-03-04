use chrono::{FixedOffset, NaiveDate, NaiveDateTime};
use std::io::Write;

use data_preprocess::models::*;
use data_preprocess::parser::{
    extract_symbol_from_filename, normalize_exchange, parse_datetime_arg, parse_datetime_to_utc,
    parse_tz_offset,
};

// ── Helper ──

fn ndt(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, mi, s)
        .unwrap()
}

fn ndt_ms(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32, ms: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_milli_opt(h, mi, s, ms)
        .unwrap()
}

fn make_tick(exchange: &str, symbol: &str, ts: NaiveDateTime, bid: f64, ask: f64) -> Tick {
    Tick {
        exchange: exchange.into(),
        symbol: symbol.into(),
        ts,
        bid: Some(bid),
        ask: Some(ask),
        last: None,
        volume: None,
        flags: None,
    }
}

fn make_bar(exchange: &str, symbol: &str, tf: Timeframe, ts: NaiveDateTime) -> Bar {
    Bar {
        exchange: exchange.into(),
        symbol: symbol.into(),
        timeframe: tf,
        ts,
        open: 100.0,
        high: 110.0,
        low: 90.0,
        close: 105.0,
        tick_vol: 200,
        volume: 0,
        spread: 10,
    }
}

// ── Parser: filename, exchange normalization, tz offset, datetime ──

#[test]
fn parser_extract_symbol_and_normalize() {
    // extract_symbol_from_filename
    let p = std::path::PathBuf::from("BTCUSD_202602161900_202602210954.csv");
    assert_eq!(extract_symbol_from_filename(&p).unwrap(), "BTCUSD");

    let p2 = std::path::PathBuf::from("xauusd_M1_202602210045_202602211009.csv");
    assert_eq!(extract_symbol_from_filename(&p2).unwrap(), "XAUUSD");

    // single-segment name (no underscore)
    let p3 = std::path::PathBuf::from("EURUSD.csv");
    assert_eq!(extract_symbol_from_filename(&p3).unwrap(), "EURUSD");

    // empty stem before underscore should error
    let p4 = std::path::PathBuf::from("_data.csv");
    assert!(extract_symbol_from_filename(&p4).is_err());

    // normalize_exchange
    assert_eq!(normalize_exchange("CTrader"), "ctrader");
    assert_eq!(normalize_exchange(" Binance "), "binance");
    assert_eq!(normalize_exchange("COINBASE"), "coinbase");
}

#[test]
fn parser_tz_offset_and_datetime_conversion() {
    // parse_tz_offset
    let plus2 = parse_tz_offset("+02:00").unwrap();
    assert_eq!(plus2, FixedOffset::east_opt(7200).unwrap());

    let minus5 = parse_tz_offset("-05:00").unwrap();
    assert_eq!(minus5, FixedOffset::west_opt(18000).unwrap());

    let utc = parse_tz_offset("+00:00").unwrap();
    assert_eq!(utc, FixedOffset::east_opt(0).unwrap());

    assert!(parse_tz_offset("invalid").is_err());
    assert!(parse_tz_offset("+25:00").is_err());

    // parse_datetime_to_utc: UTC+2 → UTC (subtract 2h)
    let result = parse_datetime_to_utc("2026.02.16", "19:00:00.083", &plus2).unwrap();
    assert_eq!(result, ndt_ms(2026, 2, 16, 17, 0, 0, 83));

    // Without milliseconds
    let result2 = parse_datetime_to_utc("2026.02.16", "19:00:00", &plus2).unwrap();
    assert_eq!(result2, ndt(2026, 2, 16, 17, 0, 0));

    // UTC+0 → no shift
    let result3 = parse_datetime_to_utc("2026.02.16", "19:00:00", &utc).unwrap();
    assert_eq!(result3, ndt(2026, 2, 16, 19, 0, 0));

    // parse_datetime_arg (CLI format)
    let dt1 = parse_datetime_arg("2026-02-16").unwrap();
    assert_eq!(dt1, ndt(2026, 2, 16, 0, 0, 0));

    let dt2 = parse_datetime_arg("2026-02-16T17:30:00").unwrap();
    assert_eq!(dt2, ndt(2026, 2, 16, 17, 30, 0));

    assert!(parse_datetime_arg("not-a-date").is_err());
}

// ── Models: Timeframe ──

#[test]
fn timeframe_parse_and_display() {
    // Forward parsing
    assert_eq!(Timeframe::parse("1m").unwrap(), Timeframe::M1);
    assert_eq!(Timeframe::parse("M1").unwrap(), Timeframe::M1);
    assert_eq!(Timeframe::parse("5m").unwrap(), Timeframe::M5);
    assert_eq!(Timeframe::parse("15m").unwrap(), Timeframe::M15);
    assert_eq!(Timeframe::parse("1h").unwrap(), Timeframe::H1);
    assert_eq!(Timeframe::parse("H4").unwrap(), Timeframe::H4);
    assert_eq!(Timeframe::parse("4h").unwrap(), Timeframe::H4);
    assert_eq!(Timeframe::parse("1d").unwrap(), Timeframe::D1);
    assert_eq!(Timeframe::parse("1w").unwrap(), Timeframe::W1);
    assert_eq!(Timeframe::parse("MN1").unwrap(), Timeframe::MN1);

    // Invalid
    assert!(Timeframe::parse("2h").is_err());
    assert!(Timeframe::parse("").is_err());

    // as_str round-trip
    assert_eq!(Timeframe::M1.as_str(), "1m");
    assert_eq!(Timeframe::H4.as_str(), "4h");
    assert_eq!(Timeframe::MN1.as_str(), "1M");

    // Display
    assert_eq!(format!("{}", Timeframe::D1), "1d");
}

// ── Parser: tick CSV ──

#[test]
fn tick_csv_parse_and_empty_fields() {
    let dir = std::env::temp_dir().join("dp_test_tick_csv");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("BTCUSD_test.csv");

    {
        let mut f = std::fs::File::create(&path).unwrap();
        // Header + 3 rows: normal, empty LAST/VOLUME, malformed date
        writeln!(f, "<DATE>\t<TIME>\t<BID>\t<ASK>\t<LAST>\t<VOLUME>\t<FLAGS>").unwrap();
        writeln!(f, "2026.02.16\t19:00:00.083\t67849.69\t67861.69\t\t\t6").unwrap();
        writeln!(
            f,
            "2026.02.16\t19:00:00.165\t67849.35\t67861.35\t100.0\t50.0\t"
        )
        .unwrap();
        writeln!(f, "bad-date\t19:00:00\t1.0\t2.0\t\t\t").unwrap();
    }

    let offset = parse_tz_offset("+02:00").unwrap();
    let (ticks, warnings) =
        data_preprocess::parser::tick_csv::parse_tick_csv(&path, "ctrader", "BTCUSD", &offset)
            .unwrap();

    // 2 good rows, 1 malformed (warning)
    assert_eq!(ticks.len(), 2);
    assert_eq!(warnings.len(), 1);

    // First tick
    assert_eq!(ticks[0].exchange, "ctrader");
    assert_eq!(ticks[0].symbol, "BTCUSD");
    assert_eq!(ticks[0].ts, ndt_ms(2026, 2, 16, 17, 0, 0, 83));
    assert!((ticks[0].bid.unwrap() - 67849.69).abs() < 0.001);
    assert!(ticks[0].last.is_none());
    assert!(ticks[0].volume.is_none());
    assert_eq!(ticks[0].flags, Some(6));

    // Second tick: LAST and VOLUME present, FLAGS empty
    assert!((ticks[1].last.unwrap() - 100.0).abs() < 0.001);
    assert!((ticks[1].volume.unwrap() - 50.0).abs() < 0.001);
    assert!(ticks[1].flags.is_none());

    std::fs::remove_dir_all(&dir).ok();
}

// ── Parser: bar CSV ──

#[test]
fn bar_csv_parse() {
    let dir = std::env::temp_dir().join("dp_test_bar_csv");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("BTCUSD_M1_test.csv");

    {
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(
            f,
            "<DATE>\t<TIME>\t<OPEN>\t<HIGH>\t<LOW>\t<CLOSE>\t<TICKVOL>\t<VOL>\t<SPREAD>"
        )
        .unwrap();
        writeln!(
            f,
            "2026.02.21\t00:45:00\t67932.44\t67934.19\t67888.89\t67910.24\t184\t0\t1200"
        )
        .unwrap();
        writeln!(
            f,
            "2026.02.21\t00:46:00\t67910.23\t67956.25\t67905.09\t67924.23\t249\t0\t1200"
        )
        .unwrap();
    }

    let offset = parse_tz_offset("+02:00").unwrap();
    let (bars, warnings) = data_preprocess::parser::bar_csv::parse_bar_csv(
        &path,
        "ctrader",
        "BTCUSD",
        Timeframe::M1,
        &offset,
    )
    .unwrap();

    assert!(warnings.is_empty());
    assert_eq!(bars.len(), 2);
    assert_eq!(bars[0].exchange, "ctrader");
    assert_eq!(bars[0].symbol, "BTCUSD");
    assert_eq!(bars[0].timeframe, Timeframe::M1);
    // 00:45 UTC+2 = 22:45 previous day UTC
    assert_eq!(bars[0].ts, ndt(2026, 2, 20, 22, 45, 0));
    assert!((bars[0].open - 67932.44).abs() < 0.01);
    assert_eq!(bars[0].tick_vol, 184);
    assert_eq!(bars[0].spread, 1200);

    std::fs::remove_dir_all(&dir).ok();
}

// ══════════════════════════════════════════════════════════════════
//  DuckDB backend tests (feature-gated)
// ══════════════════════════════════════════════════════════════════

#[cfg(feature = "duckdb-backend")]
mod duckdb_tests {
    use super::*;
    use data_preprocess::db::Database;

    #[test]
    fn db_insert_ticks_and_dedup() {
        let db = Database::open_in_memory().unwrap();
        let ts1 = ndt_ms(2026, 2, 16, 17, 0, 0, 83);
        let ts2 = ndt_ms(2026, 2, 16, 17, 0, 0, 165);

        let ticks = vec![
            make_tick("ctrader", "BTCUSD", ts1, 67849.69, 67861.69),
            make_tick("ctrader", "BTCUSD", ts2, 67849.35, 67861.35),
        ];
        assert_eq!(db.insert_ticks(&ticks).unwrap(), 2);

        // Re-insert same ticks → all skipped
        assert_eq!(db.insert_ticks(&ticks).unwrap(), 0);

        // Insert one new + one dup → only 1 inserted
        let ts3 = ndt_ms(2026, 2, 16, 17, 0, 0, 300);
        let mixed = vec![
            make_tick("ctrader", "BTCUSD", ts1, 67849.69, 67861.69), // dup
            make_tick("ctrader", "BTCUSD", ts3, 67800.00, 67812.00), // new
        ];
        assert_eq!(db.insert_ticks(&mixed).unwrap(), 1);
    }

    #[test]
    fn db_insert_ticks_same_symbol_diff_exchange() {
        let db = Database::open_in_memory().unwrap();
        let ts = ndt_ms(2026, 2, 16, 17, 0, 0, 83);

        let tick_ctrader = make_tick("ctrader", "BTCUSD", ts, 67849.69, 67861.69);
        let tick_binance = make_tick("binance", "BTCUSD", ts, 67850.00, 67860.00);

        assert_eq!(db.insert_ticks(&[tick_ctrader]).unwrap(), 1);
        assert_eq!(db.insert_ticks(&[tick_binance]).unwrap(), 1);

        // Both exist independently
        let stats = db.stats(None, Some("BTCUSD")).unwrap();
        assert_eq!(stats.len(), 2); // one for ctrader, one for binance
    }

    #[test]
    fn db_insert_bars_and_dedup() {
        let db = Database::open_in_memory().unwrap();
        let ts1 = ndt(2026, 2, 20, 22, 45, 0);
        let ts2 = ndt(2026, 2, 20, 22, 46, 0);

        let bars = vec![
            make_bar("ctrader", "BTCUSD", Timeframe::M1, ts1),
            make_bar("ctrader", "BTCUSD", Timeframe::M1, ts2),
        ];
        assert_eq!(db.insert_bars(&bars).unwrap(), 2);

        // Dedup: same exchange+symbol+timeframe+ts
        assert_eq!(db.insert_bars(&bars).unwrap(), 0);

        // Same symbol+ts, different timeframe → not a dup
        let bar_h1 = make_bar("ctrader", "BTCUSD", Timeframe::H1, ts1);
        assert_eq!(db.insert_bars(&[bar_h1]).unwrap(), 1);
    }

    #[test]
    fn db_insert_bars_same_symbol_diff_exchange() {
        let db = Database::open_in_memory().unwrap();
        let ts = ndt(2026, 2, 20, 22, 45, 0);

        let bar_ct = make_bar("ctrader", "BTCUSD", Timeframe::M1, ts);
        let bar_bn = make_bar("binance", "BTCUSD", Timeframe::M1, ts);

        assert_eq!(db.insert_bars(&[bar_ct]).unwrap(), 1);
        assert_eq!(db.insert_bars(&[bar_bn]).unwrap(), 1);

        let stats = db.stats(None, Some("BTCUSD")).unwrap();
        assert_eq!(stats.len(), 2);
    }

    #[test]
    fn db_delete_ticks_and_bars() {
        let db = Database::open_in_memory().unwrap();
        let ts1 = ndt(2026, 2, 16, 17, 0, 0);
        let ts2 = ndt(2026, 2, 17, 10, 0, 0);
        let ts3 = ndt(2026, 2, 18, 12, 0, 0);

        let ticks = vec![
            make_tick("ctrader", "BTCUSD", ts1, 100.0, 101.0),
            make_tick("ctrader", "BTCUSD", ts2, 102.0, 103.0),
            make_tick("ctrader", "BTCUSD", ts3, 104.0, 105.0),
        ];
        db.insert_ticks(&ticks).unwrap();

        // Delete with date range: only ts2 falls in [2026-02-17, 2026-02-17]
        let from = ndt(2026, 2, 17, 0, 0, 0);
        let to = ndt(2026, 2, 17, 23, 59, 59);
        let deleted = db
            .delete_ticks("ctrader", "BTCUSD", Some(from), Some(to))
            .unwrap();
        assert_eq!(deleted, 1);

        // Delete all remaining
        let deleted = db.delete_ticks("ctrader", "BTCUSD", None, None).unwrap();
        assert_eq!(deleted, 2);

        // Bar delete with timeframe filter
        let bars = vec![
            make_bar("ctrader", "BTCUSD", Timeframe::M1, ts1),
            make_bar("ctrader", "BTCUSD", Timeframe::H1, ts1),
        ];
        db.insert_bars(&bars).unwrap();

        let deleted = db
            .delete_bars("ctrader", "BTCUSD", "1m", None, None)
            .unwrap();
        assert_eq!(deleted, 1);
        let deleted = db
            .delete_bars("ctrader", "BTCUSD", "1h", None, None)
            .unwrap();
        assert_eq!(deleted, 1);
    }

    #[test]
    fn db_delete_symbol_and_exchange() {
        let db = Database::open_in_memory().unwrap();
        let ts = ndt(2026, 2, 16, 17, 0, 0);

        // Setup: 2 exchanges, 2 symbols each
        db.insert_ticks(&[make_tick("ctrader", "BTCUSD", ts, 1.0, 2.0)])
            .unwrap();
        db.insert_ticks(&[make_tick("ctrader", "EURUSD", ts, 1.1, 1.2)])
            .unwrap();
        db.insert_bars(&[make_bar("ctrader", "BTCUSD", Timeframe::M1, ts)])
            .unwrap();
        db.insert_ticks(&[make_tick("binance", "BTCUSD", ts, 1.0, 2.0)])
            .unwrap();
        db.insert_bars(&[make_bar("binance", "BTCUSD", Timeframe::M1, ts)])
            .unwrap();

        // Delete symbol: ctrader/BTCUSD (ticks + bars)
        let (t, b) = db.delete_symbol("ctrader", "BTCUSD").unwrap();
        assert_eq!(t, 1);
        assert_eq!(b, 1);

        // ctrader/EURUSD still exists
        let stats = db.stats(Some("ctrader"), None).unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].symbol, "EURUSD");

        // binance data untouched
        let stats = db.stats(Some("binance"), None).unwrap();
        assert_eq!(stats.len(), 2); // tick + bar

        // Delete entire exchange: binance
        let (t, b) = db.delete_exchange("binance").unwrap();
        assert_eq!(t, 1);
        assert_eq!(b, 1);

        // Only ctrader/EURUSD remains
        let all = db.stats(None, None).unwrap();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn db_stats_filters() {
        let db = Database::open_in_memory().unwrap();
        let ts = ndt(2026, 2, 16, 17, 0, 0);

        db.insert_ticks(&[make_tick("ctrader", "BTCUSD", ts, 1.0, 2.0)])
            .unwrap();
        db.insert_ticks(&[make_tick("ctrader", "EURUSD", ts, 1.1, 1.2)])
            .unwrap();
        db.insert_ticks(&[make_tick("binance", "BTCUSD", ts, 1.0, 2.0)])
            .unwrap();
        db.insert_bars(&[make_bar("ctrader", "BTCUSD", Timeframe::M1, ts)])
            .unwrap();

        // No filter → 4 rows
        let all = db.stats(None, None).unwrap();
        assert_eq!(all.len(), 4);

        // Filter by exchange
        let ct = db.stats(Some("ctrader"), None).unwrap();
        assert_eq!(ct.len(), 3); // BTCUSD tick, EURUSD tick, BTCUSD bar(1m)

        // Filter by symbol
        let btc = db.stats(None, Some("BTCUSD")).unwrap();
        assert_eq!(btc.len(), 3); // ctrader tick, binance tick, ctrader bar(1m)

        // Filter by both
        let ct_btc = db.stats(Some("ctrader"), Some("BTCUSD")).unwrap();
        assert_eq!(ct_btc.len(), 2); // tick + bar(1m)
    }

    #[test]
    fn db_query_ticks_and_bars() {
        let db = Database::open_in_memory().unwrap();
        let ts1 = ndt(2026, 2, 16, 10, 0, 0);
        let ts2 = ndt(2026, 2, 16, 11, 0, 0);
        let ts3 = ndt(2026, 2, 16, 12, 0, 0);
        let ts4 = ndt(2026, 2, 16, 13, 0, 0);
        let ts5 = ndt(2026, 2, 16, 14, 0, 0);

        let ticks: Vec<Tick> = vec![ts1, ts2, ts3, ts4, ts5]
            .into_iter()
            .enumerate()
            .map(|(i, ts)| make_tick("ctrader", "BTCUSD", ts, 100.0 + i as f64, 101.0 + i as f64))
            .collect();
        db.insert_ticks(&ticks).unwrap();

        // Basic query: limit 3
        let (rows, total) = db
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: None,
                to: None,
                limit: 3,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 5);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].ts, ts1); // ascending, first 3

        // Tail: last 2
        let (rows, _) = db
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: None,
                to: None,
                limit: 2,
                tail: true,
                descending: false,
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ts, ts4);
        assert_eq!(rows[1].ts, ts5);

        // Date range filter
        let (rows, total) = db
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: Some(ts2),
                to: Some(ts4),
                limit: 50,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 3); // ts2, ts3, ts4
        assert_eq!(rows.len(), 3);

        // Bar query
        let bars: Vec<Bar> = vec![ts1, ts2, ts3]
            .into_iter()
            .map(|ts| make_bar("ctrader", "BTCUSD", Timeframe::M1, ts))
            .collect();
        db.insert_bars(&bars).unwrap();

        let (rows, total) = db
            .query_bars(&BarQueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                timeframe: "1m".into(),
                from: None,
                to: None,
                limit: 2,
                tail: false,
                descending: true,
            })
            .unwrap();
        assert_eq!(total, 3);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ts, ts3); // descending

        // Tail + descending for bars
        let (rows, _) = db
            .query_bars(&BarQueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                timeframe: "1m".into(),
                from: None,
                to: None,
                limit: 2,
                tail: true,
                descending: true,
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ts, ts3); // tail grabs last 2, then sort desc
        assert_eq!(rows[1].ts, ts2);
    }

    #[test]
    fn db_empty_insert() {
        let db = Database::open_in_memory().unwrap();
        assert_eq!(db.insert_ticks(&[]).unwrap(), 0);
        assert_eq!(db.insert_bars(&[]).unwrap(), 0);
    }
}

// ══════════════════════════════════════════════════════════════════
//  Parquet backend tests
// ══════════════════════════════════════════════════════════════════

#[cfg(feature = "parquet")]
mod parquet_tests {
    use super::*;
    use data_preprocess::ParquetStore;
    use std::path::PathBuf;

    /// Create a unique temp directory for each test to avoid interference.
    fn temp_store(name: &str) -> (ParquetStore, PathBuf) {
        let dir = std::env::temp_dir()
            .join("dp_parquet_test")
            .join(name)
            .join(format!("{}", std::process::id()));
        // Clean up from any prior run
        let _ = std::fs::remove_dir_all(&dir);
        let store = ParquetStore::open(&dir).unwrap();
        (store, dir)
    }

    fn cleanup(dir: &PathBuf) {
        let _ = std::fs::remove_dir_all(dir);
    }

    // ── Insert + dedup ──

    #[test]
    fn parquet_insert_ticks_and_dedup() {
        let (store, dir) = temp_store("insert_ticks_dedup");

        let ts1 = ndt_ms(2026, 2, 16, 17, 0, 0, 83);
        let ts2 = ndt_ms(2026, 2, 16, 17, 0, 0, 165);

        let ticks = vec![
            make_tick("ctrader", "BTCUSD", ts1, 67849.69, 67861.69),
            make_tick("ctrader", "BTCUSD", ts2, 67849.35, 67861.35),
        ];
        assert_eq!(store.insert_ticks(&ticks).unwrap(), 2);

        // Re-insert same ticks → all skipped
        assert_eq!(store.insert_ticks(&ticks).unwrap(), 0);

        // Insert one new + one dup → only 1 inserted
        let ts3 = ndt_ms(2026, 2, 16, 17, 0, 0, 300);
        let mixed = vec![
            make_tick("ctrader", "BTCUSD", ts1, 67849.69, 67861.69), // dup
            make_tick("ctrader", "BTCUSD", ts3, 67800.00, 67812.00), // new
        ];
        assert_eq!(store.insert_ticks(&mixed).unwrap(), 1);

        cleanup(&dir);
    }

    #[test]
    fn parquet_insert_ticks_same_symbol_diff_exchange() {
        let (store, dir) = temp_store("ticks_diff_exchange");

        let ts = ndt_ms(2026, 2, 16, 17, 0, 0, 83);

        let tick_ctrader = make_tick("ctrader", "BTCUSD", ts, 67849.69, 67861.69);
        let tick_binance = make_tick("binance", "BTCUSD", ts, 67850.00, 67860.00);

        assert_eq!(store.insert_ticks(&[tick_ctrader]).unwrap(), 1);
        assert_eq!(store.insert_ticks(&[tick_binance]).unwrap(), 1);

        // Both exist independently
        let stats = store.stats(None, Some("BTCUSD")).unwrap();
        assert_eq!(stats.len(), 2); // one for ctrader, one for binance

        cleanup(&dir);
    }

    #[test]
    fn parquet_insert_bars_and_dedup() {
        let (store, dir) = temp_store("insert_bars_dedup");

        let ts1 = ndt(2026, 2, 20, 22, 45, 0);
        let ts2 = ndt(2026, 2, 20, 22, 46, 0);

        let bars = vec![
            make_bar("ctrader", "BTCUSD", Timeframe::M1, ts1),
            make_bar("ctrader", "BTCUSD", Timeframe::M1, ts2),
        ];
        assert_eq!(store.insert_bars(&bars).unwrap(), 2);

        // Dedup: same exchange+symbol+timeframe+ts
        assert_eq!(store.insert_bars(&bars).unwrap(), 0);

        // Same symbol+ts, different timeframe → not a dup
        let bar_h1 = make_bar("ctrader", "BTCUSD", Timeframe::H1, ts1);
        assert_eq!(store.insert_bars(&[bar_h1]).unwrap(), 1);

        cleanup(&dir);
    }

    #[test]
    fn parquet_insert_bars_same_symbol_diff_exchange() {
        let (store, dir) = temp_store("bars_diff_exchange");

        let ts = ndt(2026, 2, 20, 22, 45, 0);

        let bar_ct = make_bar("ctrader", "BTCUSD", Timeframe::M1, ts);
        let bar_bn = make_bar("binance", "BTCUSD", Timeframe::M1, ts);

        assert_eq!(store.insert_bars(&[bar_ct]).unwrap(), 1);
        assert_eq!(store.insert_bars(&[bar_bn]).unwrap(), 1);

        let stats = store.stats(None, Some("BTCUSD")).unwrap();
        assert_eq!(stats.len(), 2);

        cleanup(&dir);
    }

    // ── Delete ──

    #[test]
    fn parquet_delete_ticks_and_bars() {
        let (store, dir) = temp_store("delete_ticks_bars");

        let ts1 = ndt(2026, 2, 16, 17, 0, 0);
        let ts2 = ndt(2026, 2, 17, 10, 0, 0);
        let ts3 = ndt(2026, 2, 18, 12, 0, 0);

        let ticks = vec![
            make_tick("ctrader", "BTCUSD", ts1, 100.0, 101.0),
            make_tick("ctrader", "BTCUSD", ts2, 102.0, 103.0),
            make_tick("ctrader", "BTCUSD", ts3, 104.0, 105.0),
        ];
        store.insert_ticks(&ticks).unwrap();

        // Delete with date range: only ts2 falls in [2026-02-17 00:00, 2026-02-17 23:59:59]
        let from = ndt(2026, 2, 17, 0, 0, 0);
        let to = ndt(2026, 2, 17, 23, 59, 59);
        let deleted = store
            .delete_ticks("ctrader", "BTCUSD", Some(from), Some(to))
            .unwrap();
        assert_eq!(deleted, 1);

        // Delete all remaining
        let deleted = store.delete_ticks("ctrader", "BTCUSD", None, None).unwrap();
        assert_eq!(deleted, 2);

        // Bar delete with timeframe filter
        let bars = vec![
            make_bar("ctrader", "BTCUSD", Timeframe::M1, ts1),
            make_bar("ctrader", "BTCUSD", Timeframe::H1, ts1),
        ];
        store.insert_bars(&bars).unwrap();

        let deleted = store
            .delete_bars("ctrader", "BTCUSD", "1m", None, None)
            .unwrap();
        assert_eq!(deleted, 1);
        let deleted = store
            .delete_bars("ctrader", "BTCUSD", "1h", None, None)
            .unwrap();
        assert_eq!(deleted, 1);

        cleanup(&dir);
    }

    #[test]
    fn parquet_delete_symbol_and_exchange() {
        let (store, dir) = temp_store("delete_sym_ex");

        let ts = ndt(2026, 2, 16, 17, 0, 0);

        // Setup: 2 exchanges, 2 symbols each
        store
            .insert_ticks(&[make_tick("ctrader", "BTCUSD", ts, 1.0, 2.0)])
            .unwrap();
        store
            .insert_ticks(&[make_tick("ctrader", "EURUSD", ts, 1.1, 1.2)])
            .unwrap();
        store
            .insert_bars(&[make_bar("ctrader", "BTCUSD", Timeframe::M1, ts)])
            .unwrap();
        store
            .insert_ticks(&[make_tick("binance", "BTCUSD", ts, 1.0, 2.0)])
            .unwrap();
        store
            .insert_bars(&[make_bar("binance", "BTCUSD", Timeframe::M1, ts)])
            .unwrap();

        // Delete symbol: ctrader/BTCUSD (ticks + bars)
        let (t, b) = store.delete_symbol("ctrader", "BTCUSD").unwrap();
        assert_eq!(t, 1);
        assert_eq!(b, 1);

        // ctrader/EURUSD still exists
        let stats = store.stats(Some("ctrader"), None).unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].symbol, "EURUSD");

        // binance data untouched
        let stats = store.stats(Some("binance"), None).unwrap();
        assert_eq!(stats.len(), 2); // tick + bar

        // Delete entire exchange: binance
        let (t, b) = store.delete_exchange("binance").unwrap();
        assert_eq!(t, 1);
        assert_eq!(b, 1);

        // Only ctrader/EURUSD remains
        let all = store.stats(None, None).unwrap();
        assert_eq!(all.len(), 1);

        cleanup(&dir);
    }

    // ── Stats ──

    #[test]
    fn parquet_stats_filters() {
        let (store, dir) = temp_store("stats_filters");

        let ts = ndt(2026, 2, 16, 17, 0, 0);

        store
            .insert_ticks(&[make_tick("ctrader", "BTCUSD", ts, 1.0, 2.0)])
            .unwrap();
        store
            .insert_ticks(&[make_tick("ctrader", "EURUSD", ts, 1.1, 1.2)])
            .unwrap();
        store
            .insert_ticks(&[make_tick("binance", "BTCUSD", ts, 1.0, 2.0)])
            .unwrap();
        store
            .insert_bars(&[make_bar("ctrader", "BTCUSD", Timeframe::M1, ts)])
            .unwrap();

        // No filter → 4 rows
        let all = store.stats(None, None).unwrap();
        assert_eq!(all.len(), 4);

        // Filter by exchange
        let ct = store.stats(Some("ctrader"), None).unwrap();
        assert_eq!(ct.len(), 3); // BTCUSD tick, EURUSD tick, BTCUSD bar(1m)

        // Filter by symbol
        let btc = store.stats(None, Some("BTCUSD")).unwrap();
        assert_eq!(btc.len(), 3); // ctrader tick, binance tick, ctrader bar(1m)

        // Filter by both
        let ct_btc = store.stats(Some("ctrader"), Some("BTCUSD")).unwrap();
        assert_eq!(ct_btc.len(), 2); // tick + bar(1m)

        cleanup(&dir);
    }

    // ── Query ──

    #[test]
    fn parquet_query_ticks_and_bars() {
        let (store, dir) = temp_store("query_ticks_bars");

        let ts1 = ndt(2026, 2, 16, 10, 0, 0);
        let ts2 = ndt(2026, 2, 16, 11, 0, 0);
        let ts3 = ndt(2026, 2, 16, 12, 0, 0);
        let ts4 = ndt(2026, 2, 16, 13, 0, 0);
        let ts5 = ndt(2026, 2, 16, 14, 0, 0);

        let ticks: Vec<Tick> = vec![ts1, ts2, ts3, ts4, ts5]
            .into_iter()
            .enumerate()
            .map(|(i, ts)| make_tick("ctrader", "BTCUSD", ts, 100.0 + i as f64, 101.0 + i as f64))
            .collect();
        store.insert_ticks(&ticks).unwrap();

        // Basic query: limit 3
        let (rows, total) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: None,
                to: None,
                limit: 3,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 5);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].ts, ts1); // ascending, first 3

        // Tail: last 2
        let (rows, _) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: None,
                to: None,
                limit: 2,
                tail: true,
                descending: false,
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ts, ts4);
        assert_eq!(rows[1].ts, ts5);

        // Date range filter
        let (rows, total) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: Some(ts2),
                to: Some(ts4),
                limit: 50,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 3); // ts2, ts3, ts4
        assert_eq!(rows.len(), 3);

        // Bar query
        let bars: Vec<Bar> = vec![ts1, ts2, ts3]
            .into_iter()
            .map(|ts| make_bar("ctrader", "BTCUSD", Timeframe::M1, ts))
            .collect();
        store.insert_bars(&bars).unwrap();

        let (rows, total) = store
            .query_bars(&BarQueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                timeframe: "1m".into(),
                from: None,
                to: None,
                limit: 2,
                tail: false,
                descending: true,
            })
            .unwrap();
        assert_eq!(total, 3);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ts, ts3); // descending

        // Tail + descending for bars
        let (rows, _) = store
            .query_bars(&BarQueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                timeframe: "1m".into(),
                from: None,
                to: None,
                limit: 2,
                tail: true,
                descending: true,
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].ts, ts3); // tail grabs last 2, then sort desc
        assert_eq!(rows[1].ts, ts2);

        cleanup(&dir);
    }

    // ── Empty insert ──

    #[test]
    fn parquet_empty_insert() {
        let (store, dir) = temp_store("empty_insert");

        assert_eq!(store.insert_ticks(&[]).unwrap(), 0);
        assert_eq!(store.insert_bars(&[]).unwrap(), 0);

        cleanup(&dir);
    }

    // ── Mixed symbols in one import ──

    #[test]
    fn parquet_mixed_symbols_one_import() {
        let (store, dir) = temp_store("mixed_symbols");

        let ts = ndt(2026, 3, 1, 10, 0, 0);
        let ticks = vec![
            make_tick("ctrader", "BTCUSD", ts, 67000.0, 67010.0),
            make_tick("ctrader", "EURUSD", ts, 1.08, 1.09),
            make_tick("ctrader", "XAUUSD", ts, 2350.0, 2351.0),
        ];

        // All three should be inserted into separate partition files
        assert_eq!(store.insert_ticks(&ticks).unwrap(), 3);

        // Each symbol queryable independently
        let (rows, _) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: None,
                to: None,
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(rows.len(), 1);

        let (rows, _) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "EURUSD".into(),
                from: None,
                to: None,
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(rows.len(), 1);

        let (rows, _) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "XAUUSD".into(),
                from: None,
                to: None,
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(rows.len(), 1);

        // Stats should show 3 entries (one per symbol)
        let stats = store.stats(Some("ctrader"), None).unwrap();
        assert_eq!(stats.len(), 3);

        cleanup(&dir);
    }

    // ── Partial date delete ──

    #[test]
    fn parquet_partial_date_delete() {
        let (store, dir) = temp_store("partial_delete");

        // Insert 4 ticks on the same date
        let ts1 = ndt(2026, 3, 2, 10, 0, 0);
        let ts2 = ndt(2026, 3, 2, 12, 0, 0);
        let ts3 = ndt(2026, 3, 2, 14, 0, 0);
        let ts4 = ndt(2026, 3, 2, 16, 0, 0);

        let ticks = vec![
            make_tick("ctrader", "EURUSD", ts1, 1.08, 1.09),
            make_tick("ctrader", "EURUSD", ts2, 1.085, 1.095),
            make_tick("ctrader", "EURUSD", ts3, 1.09, 1.10),
            make_tick("ctrader", "EURUSD", ts4, 1.095, 1.105),
        ];
        store.insert_ticks(&ticks).unwrap();

        // Delete mid-day range (12:00 to 14:00) — should remove ts2 and ts3
        let from = ndt(2026, 3, 2, 12, 0, 0);
        let to = ndt(2026, 3, 2, 14, 0, 0);
        let deleted = store
            .delete_ticks("ctrader", "EURUSD", Some(from), Some(to))
            .unwrap();
        assert_eq!(deleted, 2);

        // ts1 and ts4 should remain
        let (remaining, total) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "EURUSD".into(),
                from: None,
                to: None,
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 2);
        assert_eq!(remaining[0].ts, ts1);
        assert_eq!(remaining[1].ts, ts4);

        cleanup(&dir);
    }

    // ── Directory structure verification ──

    #[test]
    fn parquet_directory_structure() {
        let (store, dir) = temp_store("dir_structure");

        let ts = ndt(2026, 3, 2, 10, 0, 0);
        store
            .insert_ticks(&[make_tick("ctrader", "EURUSD", ts, 1.08, 1.09)])
            .unwrap();
        store
            .insert_bars(&[make_bar("ctrader", "EURUSD", Timeframe::H1, ts)])
            .unwrap();

        // Verify tick directory layout
        let tick_file = dir.join("ticks/exchange=ctrader/symbol=EURUSD/2026-03-02.parquet");
        assert!(
            tick_file.exists(),
            "tick parquet file should exist at: {:?}",
            tick_file
        );

        // Verify bar directory layout
        let bar_file =
            dir.join("bars/exchange=ctrader/symbol=EURUSD/timeframe=1h/2026-03-02.parquet");
        assert!(
            bar_file.exists(),
            "bar parquet file should exist at: {:?}",
            bar_file
        );

        cleanup(&dir);
    }

    // ── Sort order ──

    #[test]
    fn parquet_sort_order() {
        let (store, dir) = temp_store("sort_order");

        // Insert ticks out of order
        let ts3 = ndt(2026, 3, 2, 14, 0, 0);
        let ts1 = ndt(2026, 3, 2, 10, 0, 0);
        let ts2 = ndt(2026, 3, 2, 12, 0, 0);

        let ticks = vec![
            make_tick("ctrader", "BTCUSD", ts3, 104.0, 105.0),
            make_tick("ctrader", "BTCUSD", ts1, 100.0, 101.0),
            make_tick("ctrader", "BTCUSD", ts2, 102.0, 103.0),
        ];
        store.insert_ticks(&ticks).unwrap();

        // Query should return sorted by ts ascending
        let (rows, _) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: None,
                to: None,
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].ts, ts1);
        assert_eq!(rows[1].ts, ts2);
        assert_eq!(rows[2].ts, ts3);

        cleanup(&dir);
    }

    // ── Overwrite dedup (re-import overlapping data) ──

    #[test]
    fn parquet_overwrite_dedup() {
        let (store, dir) = temp_store("overwrite_dedup");

        let ts1 = ndt(2026, 3, 2, 10, 0, 0);
        let ts2 = ndt(2026, 3, 2, 11, 0, 0);
        let ts3 = ndt(2026, 3, 2, 12, 0, 0);

        // First import: ts1, ts2
        let batch1 = vec![
            make_tick("ctrader", "BTCUSD", ts1, 100.0, 101.0),
            make_tick("ctrader", "BTCUSD", ts2, 102.0, 103.0),
        ];
        assert_eq!(store.insert_ticks(&batch1).unwrap(), 2);

        // Second import: ts2 (dup), ts3 (new)
        let batch2 = vec![
            make_tick("ctrader", "BTCUSD", ts2, 102.0, 103.0),
            make_tick("ctrader", "BTCUSD", ts3, 104.0, 105.0),
        ];
        assert_eq!(store.insert_ticks(&batch2).unwrap(), 1);

        // Should have exactly 3 unique ticks
        let (rows, total) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: None,
                to: None,
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 3);
        assert_eq!(rows.len(), 3);

        cleanup(&dir);
    }

    // ── Multi-day query across files ──

    #[test]
    fn parquet_multi_day_query() {
        let (store, dir) = temp_store("multi_day");

        let ts_day1 = ndt(2026, 3, 1, 10, 0, 0);
        let ts_day2 = ndt(2026, 3, 2, 10, 0, 0);
        let ts_day3 = ndt(2026, 3, 3, 10, 0, 0);

        let ticks = vec![
            make_tick("ctrader", "EURUSD", ts_day1, 1.08, 1.09),
            make_tick("ctrader", "EURUSD", ts_day2, 1.085, 1.095),
            make_tick("ctrader", "EURUSD", ts_day3, 1.09, 1.10),
        ];
        store.insert_ticks(&ticks).unwrap();

        // Query across all days
        let (rows, total) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "EURUSD".into(),
                from: None,
                to: None,
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 3);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].ts, ts_day1);
        assert_eq!(rows[1].ts, ts_day2);
        assert_eq!(rows[2].ts, ts_day3);

        // Query just day 2
        let (rows, total) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "EURUSD".into(),
                from: Some(ndt(2026, 3, 2, 0, 0, 0)),
                to: Some(ndt(2026, 3, 2, 23, 59, 59)),
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts, ts_day2);

        cleanup(&dir);
    }

    // ── Query nonexistent symbol returns empty ──

    #[test]
    fn parquet_query_nonexistent() {
        let (store, dir) = temp_store("nonexistent");

        let (rows, total) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "DOESNOTEXIST".into(),
                from: None,
                to: None,
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 0);
        assert_eq!(rows.len(), 0);

        let (rows, total) = store
            .query_bars(&BarQueryOpts {
                exchange: "ctrader".into(),
                symbol: "DOESNOTEXIST".into(),
                timeframe: "1m".into(),
                from: None,
                to: None,
                limit: 100,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(total, 0);
        assert_eq!(rows.len(), 0);

        cleanup(&dir);
    }

    // ── Total size ──

    #[test]
    fn parquet_total_size() {
        let (store, dir) = temp_store("total_size");

        // Empty store
        assert!(store.total_size().is_none());

        let ts = ndt(2026, 3, 2, 10, 0, 0);
        store
            .insert_ticks(&[make_tick("ctrader", "BTCUSD", ts, 100.0, 101.0)])
            .unwrap();

        let size = store.total_size();
        assert!(size.is_some());
        assert!(size.unwrap() > 0);

        cleanup(&dir);
    }

    // ── Tick field roundtrip (all optional fields) ──

    #[test]
    fn parquet_tick_fields_roundtrip() {
        let (store, dir) = temp_store("tick_roundtrip");

        let ts = ndt(2026, 3, 2, 10, 0, 0);
        let tick = Tick {
            exchange: "ctrader".into(),
            symbol: "BTCUSD".into(),
            ts,
            bid: Some(67849.69),
            ask: Some(67861.69),
            last: Some(67855.0),
            volume: Some(42.5),
            flags: Some(6),
        };
        store.insert_ticks(&[tick]).unwrap();

        let (rows, _) = store
            .query_ticks(&QueryOpts {
                exchange: "ctrader".into(),
                symbol: "BTCUSD".into(),
                from: None,
                to: None,
                limit: 10,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        let t = &rows[0];
        assert_eq!(t.exchange, "ctrader");
        assert_eq!(t.symbol, "BTCUSD");
        assert_eq!(t.ts, ts);
        assert!((t.bid.unwrap() - 67849.69).abs() < 0.001);
        assert!((t.ask.unwrap() - 67861.69).abs() < 0.001);
        assert!((t.last.unwrap() - 67855.0).abs() < 0.001);
        assert!((t.volume.unwrap() - 42.5).abs() < 0.001);
        assert_eq!(t.flags, Some(6));

        cleanup(&dir);
    }

    // ── Bar field roundtrip ──

    #[test]
    fn parquet_bar_fields_roundtrip() {
        let (store, dir) = temp_store("bar_roundtrip");

        let ts = ndt(2026, 3, 2, 10, 0, 0);
        let bar = Bar {
            exchange: "ctrader".into(),
            symbol: "EURUSD".into(),
            timeframe: Timeframe::H4,
            ts,
            open: 1.0850,
            high: 1.0900,
            low: 1.0830,
            close: 1.0875,
            tick_vol: 12345,
            volume: 67890,
            spread: 15,
        };
        store.insert_bars(&[bar]).unwrap();

        let (rows, _) = store
            .query_bars(&BarQueryOpts {
                exchange: "ctrader".into(),
                symbol: "EURUSD".into(),
                timeframe: "4h".into(),
                from: None,
                to: None,
                limit: 10,
                tail: false,
                descending: false,
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        let b = &rows[0];
        assert_eq!(b.exchange, "ctrader");
        assert_eq!(b.symbol, "EURUSD");
        assert_eq!(b.timeframe, Timeframe::H4);
        assert_eq!(b.ts, ts);
        assert!((b.open - 1.0850).abs() < 0.0001);
        assert!((b.high - 1.0900).abs() < 0.0001);
        assert!((b.low - 1.0830).abs() < 0.0001);
        assert!((b.close - 1.0875).abs() < 0.0001);
        assert_eq!(b.tick_vol, 12345);
        assert_eq!(b.volume, 67890);
        assert_eq!(b.spread, 15);

        cleanup(&dir);
    }
}
