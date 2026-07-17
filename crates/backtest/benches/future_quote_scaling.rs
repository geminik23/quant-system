use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use qs_backtest::data_feed::{EventMetadata, FeedEvent, SeriesRoles};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    BacktestRunner, ConversionLeg, ConversionRoute, FutureQuoteConfig, FxPair, FxPairDirection,
    MarketEvent, MtmOutputPolicy, RawSignal, RunCurrencyPlan, VecFeed,
};
use qs_core::types::{OrderType, Side};
use qs_symbols::SymbolSpec;

const PRIMARY: &str = "ACTIVE";
const SIZES: [usize; 5] = [128, 512, 2_048, 10_000, 100_000];

#[derive(Clone, Copy)]
enum CurrencyCase {
    Identity,
    OneLeg,
    TwoLeg,
}

struct ReplayFixture {
    feed: VecFeed,
    signals: Vec<RawSignal>,
    config: BacktestConfig,
    future: FutureQuoteConfig,
}

fn ts(milliseconds: usize) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + ChronoDuration::milliseconds(i64::try_from(milliseconds).unwrap())
}

fn tick(symbol: &str, milliseconds: usize, price: f64) -> MarketEvent {
    MarketEvent::Tick {
        symbol: symbol.to_owned(),
        ts: ts(milliseconds),
        bid: price,
        ask: price,
    }
}

fn pair(symbol: &str, base_currency: &str, quote_currency: &str) -> FxPair {
    FxPair {
        symbol: symbol.to_owned(),
        base_currency: base_currency.to_owned(),
        quote_currency: quote_currency.to_owned(),
    }
}

fn plan(case: CurrencyCase) -> RunCurrencyPlan {
    let primary_symbols = BTreeSet::from([PRIMARY.to_owned()]);
    match case {
        CurrencyCase::Identity => RunCurrencyPlan::new(
            "USD",
            primary_symbols,
            BTreeSet::new(),
            BTreeMap::from([(PRIMARY.to_owned(), "USD".to_owned())]),
            BTreeMap::from([(
                "USD".to_owned(),
                ConversionRoute::Identity {
                    currency: "USD".to_owned(),
                },
            )]),
            Vec::new(),
        ),
        CurrencyCase::OneLeg => RunCurrencyPlan::new(
            "USD",
            primary_symbols,
            BTreeSet::from(["EURUSD".to_owned()]),
            BTreeMap::from([(PRIMARY.to_owned(), "EUR".to_owned())]),
            BTreeMap::from([(
                "EUR".to_owned(),
                ConversionRoute::Direct {
                    pair: pair("EURUSD", "EUR", "USD"),
                },
            )]),
            Vec::new(),
        ),
        CurrencyCase::TwoLeg => RunCurrencyPlan::new(
            "USD",
            primary_symbols,
            BTreeSet::from(["EURGBP".to_owned(), "GBPUSD".to_owned()]),
            BTreeMap::from([(PRIMARY.to_owned(), "EUR".to_owned())]),
            BTreeMap::from([(
                "EUR".to_owned(),
                ConversionRoute::TwoLeg {
                    pivot_currency: "GBP".to_owned(),
                    first: ConversionLeg {
                        pair: pair("EURGBP", "EUR", "GBP"),
                        direction: FxPairDirection::Direct,
                    },
                    second: ConversionLeg {
                        pair: pair("GBPUSD", "GBP", "USD"),
                        direction: FxPairDirection::Direct,
                    },
                },
            )]),
            Vec::new(),
        ),
    }
    .unwrap()
}

fn fixture(primary_events: usize, case: CurrencyCase) -> ReplayFixture {
    let mut events = Vec::new();
    for index in 0..primary_events {
        match case {
            CurrencyCase::Identity => {}
            CurrencyCase::OneLeg => events.push(FeedEvent::new(
                tick("EURUSD", index, 1.0),
                EventMetadata::new(SeriesRoles::CONVERSION, 0, index as u64),
            )),
            CurrencyCase::TwoLeg => {
                events.push(FeedEvent::new(
                    tick("EURGBP", index, 2.0),
                    EventMetadata::new(SeriesRoles::CONVERSION, 0, index as u64),
                ));
                events.push(FeedEvent::new(
                    tick("GBPUSD", index, 0.5),
                    EventMetadata::new(SeriesRoles::CONVERSION, 1, index as u64),
                ));
            }
        }
        events.push(FeedEvent::new(
            tick(PRIMARY, index, 100.0 + index as f64 * 0.01),
            EventMetadata::new(
                SeriesRoles::PRIMARY,
                match case {
                    CurrencyCase::Identity => 0,
                    CurrencyCase::OneLeg => 1,
                    CurrencyCase::TwoLeg => 2,
                },
                index as u64,
            ),
        ));
    }

    let config = BacktestConfig {
        close_on_finish: false,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: HashMap::from([(
            PRIMARY.to_owned(),
            SymbolSpec {
                canonical: PRIMARY.to_ascii_lowercase(),
                pip_position: 2,
                digits: 2,
                category: "index".to_owned(),
                lot_base_units: 1,
                lot_step_units: 1,
                lot_min_steps: 1,
                lot_max_steps: 0,
            },
        )]),
        ..BacktestConfig::default()
    };
    let future = FutureQuoteConfig {
        currency_plan: Some(plan(case)),
        mtm_output: MtmOutputPolicy::Bounded { max_points: 64 },
        ..FutureQuoteConfig::default()
    };
    let signals = vec![RawSignal::Entry {
        ts: ts(0),
        symbol: PRIMARY.to_owned(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        risk_multiplier: 1.0,
        stoploss: Some(90.0),
        targets: Vec::new(),
        group: None,
        trade_id: Some("benchmark-active".to_owned()),
    }];

    ReplayFixture {
        feed: VecFeed::from_feed_events(events),
        signals,
        config,
        future,
    }
}

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
}

fn bench_active_identity(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("future_quote_active_identity");
    configure_group(&mut group);

    for primary_events in SIZES {
        let fixture = fixture(primary_events, CurrencyCase::Identity);
        group.throughput(Throughput::Elements(primary_events as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(primary_events),
            &primary_events,
            |benchmark, _| {
                benchmark.iter_batched(
                    || (fixture.feed.clone(), fixture.signals.clone()),
                    |(mut feed, signals)| {
                        black_box(
                            BacktestRunner::new_future(
                                fixture.config.clone(),
                                fixture.future.clone(),
                            )
                            .run_raw_signals_future(&mut feed, signals, None),
                        )
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

fn bench_active_fx(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("future_quote_active_fx");
    configure_group(&mut group);

    for (name, case) in [
        ("one_leg", CurrencyCase::OneLeg),
        ("two_leg", CurrencyCase::TwoLeg),
    ] {
        for primary_events in SIZES {
            let fixture = fixture(primary_events, case);
            group.throughput(Throughput::Elements(primary_events as u64));
            group.bench_with_input(
                BenchmarkId::new(name, primary_events),
                &primary_events,
                |benchmark, _| {
                    benchmark.iter_batched(
                        || (fixture.feed.clone(), fixture.signals.clone()),
                        |(mut feed, signals)| {
                            black_box(
                                BacktestRunner::new_future(
                                    fixture.config.clone(),
                                    fixture.future.clone(),
                                )
                                .run_raw_signals_future(&mut feed, signals, None),
                            )
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_active_identity, bench_active_fx);
criterion_main!(benches);
