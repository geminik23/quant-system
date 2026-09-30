//! A complete parameter search over data the example produces itself.
//!
//! It writes a deterministic synthetic tick series into a temporary Parquet store, reads it back through the same loader a service uses, runs an EMA-cross grid over an in-sample and an out-of-sample window, and writes the resulting table to `target/research/ema_grid.csv`.
//!
//! Going through the store and the loader is deliberate: it is the path a real search takes with real ticks, so the example exercises the loading boundary rather than hand-feeding events to the engine. The series is synthetic and means nothing economically; it exists to make the loop reproducible without shipping market data.
//!
//! Run it with `cargo run -p qs-research --example ema_grid`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{Duration, NaiveDate, NaiveDateTime};
use data_preprocess::{ParquetStore, Tick};
use qs_backtest::evaluation::{BreakdownDimension, EvaluationOptions};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_core::{CommissionModel, InstrumentCosts};
use qs_research::{
    DataWindow, DeclaredSpace, ResearchPlan, WindowPlan, load_symbol_ticks, run_batch,
};
use qs_symbols::SymbolSpec;

const EXCHANGE: &str = "synthetic";
const SYMBOL: &str = "EURUSD";
const POINT_SIZE: f64 = 1.0e-5;
const MINUTES: i64 = 3_600;

fn base() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 5)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn at(minute: i64) -> NaiveDateTime {
    base() + Duration::minutes(minute)
}

/// A fixed-seed tick series: a slow cycle with bounded noise, so crossovers occur and the output is reproducible.
fn synthetic_ticks() -> Vec<Tick> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    (0..MINUTES)
        .map(|minute| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let noise = ((state >> 33) % 21) as i64 - 10;
            let phase = (minute % 240) as f64 / 240.0 * std::f64::consts::TAU;
            let drift = (phase.sin() * 120.0) as i64;
            let bid = 1.10000 + (drift + noise) as f64 * POINT_SIZE;
            Tick {
                exchange: EXCHANGE.into(),
                symbol: SYMBOL.into(),
                ts: at(minute),
                bid: Some(bid),
                ask: Some(bid + 2.0 * POINT_SIZE),
                last: None,
                volume: None,
                flags: None,
            }
        })
        .collect()
}

fn symbol_spec() -> SymbolSpec {
    SymbolSpec {
        canonical: SYMBOL.to_ascii_lowercase(),
        pip_position: 4,
        digits: 5,
        category: "forex".into(),
        lot_base_units: 100_000,
        lot_step_units: 1_000,
        lot_min_steps: 1,
        lot_max_steps: 0,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir =
        std::env::temp_dir().join(format!("qs-research-ema-grid-{}", std::process::id()));
    std::fs::create_dir_all(&data_dir)?;

    let store = ParquetStore::open(&data_dir)?;
    let ticks = synthetic_ticks();
    store.insert_ticks(&ticks)?;
    println!(
        "wrote {} synthetic ticks to {}",
        ticks.len(),
        data_dir.display()
    );

    let events = load_symbol_ticks(
        data_dir.to_str().expect("a temporary path is valid UTF-8"),
        EXCHANGE,
        SYMBOL,
        Some(at(0)),
        Some(at(MINUTES)),
    )?;
    println!("loaded {} events through the market loader", events.len());

    let family = DeclaredSpace::load(
        concat!(env!("CARGO_MANIFEST_DIR"), "/examples/ema_strategy.toml"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/examples/ema_space.toml"),
    )?;

    let mut config = BacktestConfig {
        initial_balance: 10_000.0,
        sizing: Some(SizingPolicy::FixedLot { lots: 0.1 }),
        symbol_specs: [(SYMBOL.to_owned(), symbol_spec())].into_iter().collect(),
        contract_sizes: [(SYMBOL.to_owned(), 100_000.0)].into_iter().collect(),
        ..BacktestConfig::default()
    };
    // A search without costs ranks configurations that a real account could not have traded.
    config.costs.insert(
        SYMBOL.to_owned(),
        InstrumentCosts {
            commission: Some(CommissionModel::PerLotPerSide {
                amount: 3.5,
                currency: "USD".into(),
            }),
            swap: None,
        },
    );
    config.run_tags.insert("dataset".into(), "synthetic".into());

    let plan = ResearchPlan::new(
        vec![SYMBOL.to_owned()],
        WindowPlan::Fixed {
            in_sample: DataWindow::new("is", at(300), at(2_400))?,
            out_of_sample: DataWindow::new("oos", at(2_400), at(MINUTES))?,
        },
        config,
    )
    .with_workers(4);

    let batch = run_batch(
        &plan,
        &family,
        &BTreeMap::from([(SYMBOL.to_owned(), events)]),
    )?;

    let out_dir = PathBuf::from("target/research");
    std::fs::create_dir_all(&out_dir)?;
    let out_file = out_dir.join("ema_grid.csv");
    let table = batch.table();
    std::fs::write(&out_file, table.to_csv())?;

    let pairs = table.paired("is", "oos");
    println!(
        "searched {} configurations, wrote {} rows to {}",
        table.rows().first().map_or(0, |row| row.points_total),
        table.len(),
        out_file.display()
    );
    println!("in-sample and out-of-sample pairs: {}", pairs.len());
    let pooled = batch.evaluate(EvaluationOptions {
        breakdowns: vec![
            BreakdownDimension::Tag("entry".into()),
            BreakdownDimension::Tag("window".into()),
        ],
        minimum_breakdown_bucket_count: 1,
        ..EvaluationOptions::default()
    });
    for breakdown in pooled.breakdowns.unwrap_or_default() {
        println!(
            "pooled {:?}: {} buckets",
            breakdown.dimension,
            breakdown.buckets.len()
        );
    }
    println!();
    println!("The table is ordered by parameter, not by result. It reports what each");
    println!("configuration did; choosing among them is the reader's job, and the");
    println!("searched-space size above is why that choice deserves suspicion.");

    std::fs::remove_dir_all(&data_dir)?;
    Ok(())
}
