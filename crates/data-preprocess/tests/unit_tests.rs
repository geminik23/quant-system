use chrono::{FixedOffset, NaiveDate, NaiveDateTime};
use std::io::Write;

use data_preprocess::db::{BarQueryOpts, Database, QueryOpts};
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

// ── DB: tick insert, dedup, cross-exchange isolation ──

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

// ── DB: bar insert, dedup, cross-exchange isolation ──

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

// ── DB: delete operations ──

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

// ── DB: stats filtering ──

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

// ── DB: query ticks and bars ──

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

// ── DB: empty insert returns 0 ──

#[test]
fn db_empty_insert() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(db.insert_ticks(&[]).unwrap(), 0);
    assert_eq!(db.insert_bars(&[]).unwrap(), 0);
}
