use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::data_feed::{
    EventMetadata, FallibleBatchFeed, FeedEvent, SeriesRoles, TimestampBatch,
};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    BacktestResult, BacktestRunner, ConversionLeg, ConversionRoute, FutureQuoteConfig, FxPair,
    FxPairDirection, MarketEvent, MtmOutputPolicy, PositionRef, RawSignal, RunCurrencyPlan,
    VecFeed,
};
use qs_core::types::{OrderType, Side};
use qs_symbols::SymbolSpec;

const PRIMARY: &str = "ACTIVE";
const TRADE_ID: &str = "active-scaling";
const BOUNDED_POINTS: usize = 8;

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
        + Duration::milliseconds(i64::try_from(milliseconds).unwrap())
}

fn tick(symbol: &str, milliseconds: usize, bid: f64, ask: f64) -> MarketEvent {
    MarketEvent::Tick {
        symbol: symbol.to_owned(),
        ts: ts(milliseconds),
        bid,
        ask,
    }
}

fn pair(symbol: &str, base_currency: &str, quote_currency: &str) -> FxPair {
    FxPair {
        symbol: symbol.to_owned(),
        base_currency: base_currency.to_owned(),
        quote_currency: quote_currency.to_owned(),
    }
}

fn currency_plan(case: CurrencyCase) -> RunCurrencyPlan {
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

fn config() -> BacktestConfig {
    BacktestConfig {
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
    }
}

fn active_signal() -> RawSignal {
    RawSignal::Entry {
        ts: ts(0),
        symbol: PRIMARY.to_owned(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        risk_multiplier: 1.0,
        stoploss: Some(90.0),
        targets: Vec::new(),
        group: None,
        trade_id: Some(TRADE_ID.to_owned()),
    }
}

fn fixture(primary_events: usize, case: CurrencyCase, policy: MtmOutputPolicy) -> ReplayFixture {
    assert!(primary_events > 1);
    let mut events = Vec::with_capacity(
        primary_events
            * match case {
                CurrencyCase::Identity => 1,
                CurrencyCase::OneLeg => 2,
                CurrencyCase::TwoLeg => 3,
            },
    );

    for index in 0..primary_events {
        match case {
            CurrencyCase::Identity => {}
            CurrencyCase::OneLeg => events.push(FeedEvent::new(
                tick("EURUSD", index, 1.0, 1.0),
                EventMetadata::new(SeriesRoles::CONVERSION, 0, index as u64),
            )),
            CurrencyCase::TwoLeg => {
                events.push(FeedEvent::new(
                    tick("EURGBP", index, 2.0, 2.0),
                    EventMetadata::new(SeriesRoles::CONVERSION, 0, index as u64),
                ));
                events.push(FeedEvent::new(
                    tick("GBPUSD", index, 0.5, 0.5),
                    EventMetadata::new(SeriesRoles::CONVERSION, 1, index as u64),
                ));
            }
        }
        events.push(FeedEvent::new(
            tick(
                PRIMARY,
                index,
                100.0 + index as f64 * 0.01,
                100.0 + index as f64 * 0.01,
            ),
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

    ReplayFixture {
        feed: VecFeed::from_feed_events(events),
        signals: vec![active_signal()],
        config: config(),
        future: FutureQuoteConfig {
            currency_plan: Some(currency_plan(case)),
            mtm_output: policy,
            ..FutureQuoteConfig::default()
        },
    }
}

fn run_fixture(
    primary_events: usize,
    case: CurrencyCase,
    policy: MtmOutputPolicy,
) -> BacktestResult {
    let fixture = fixture(primary_events, case, policy);
    let mut feed = fixture.feed;
    let result = BacktestRunner::new_future(fixture.config, fixture.future).run_raw_signals_future(
        &mut feed,
        fixture.signals,
        None,
    );
    assert_eq!(feed.remaining(), 0);
    assert_eq!(result.open_position_snapshots.len(), 1);
    assert_eq!(
        result.open_position_snapshots[0].quote_ts,
        Some(ts(primary_events - 1)),
        "the active position must force replay through the final primary event"
    );
    result
}

fn assert_output_counts(result: &BacktestResult, primary_events: usize, retained: usize) {
    let observed = u64::try_from(primary_events + 2).unwrap();
    assert_eq!(result.mtm_output_summary.observed_points, observed);
    assert_eq!(result.mtm_output_summary.retained_points, retained as u64);
    assert_eq!(
        result.mtm_output_summary.omitted_points,
        observed - retained as u64
    );
    assert_eq!(result.mtm_equity_curve.len(), retained);
    assert!(
        result
            .mtm_equity_curve
            .windows(2)
            .all(|points| points[0].observation_sequence < points[1].observation_sequence)
    );
}

#[test]
fn active_output_policies_have_deterministic_cardinality_and_bytes() {
    const PRIMARY_EVENTS: usize = 64;
    let policies = [
        (MtmOutputPolicy::None, 0),
        (
            MtmOutputPolicy::Bounded {
                max_points: BOUNDED_POINTS,
            },
            BOUNDED_POINTS,
        ),
        (MtmOutputPolicy::Full, PRIMARY_EVENTS + 2),
    ];

    for (policy, retained) in policies {
        let first = run_fixture(PRIMARY_EVENTS, CurrencyCase::Identity, policy);
        let second = run_fixture(PRIMARY_EVENTS, CurrencyCase::Identity, policy);

        assert_output_counts(&first, PRIMARY_EVENTS, retained);
        assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap(),
            "repeated replay output must be byte deterministic for {policy:?}"
        );
        if policy == MtmOutputPolicy::Full {
            assert_eq!(
                first
                    .mtm_equity_curve
                    .iter()
                    .map(|point| point.observation_sequence.unwrap())
                    .collect::<Vec<_>>(),
                (0..u64::try_from(PRIMARY_EVENTS + 2).unwrap()).collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn active_identity_one_leg_and_two_leg_replays_preserve_account_trace() {
    const PRIMARY_EVENTS: usize = 32;
    let identity = run_fixture(
        PRIMARY_EVENTS,
        CurrencyCase::Identity,
        MtmOutputPolicy::Full,
    );
    let one_leg = run_fixture(PRIMARY_EVENTS, CurrencyCase::OneLeg, MtmOutputPolicy::Full);
    let two_leg = run_fixture(PRIMARY_EVENTS, CurrencyCase::TwoLeg, MtmOutputPolicy::Full);

    for result in [&identity, &one_leg, &two_leg] {
        assert_output_counts(result, PRIMARY_EVENTS, PRIMARY_EVENTS + 2);
        assert_eq!(result.recorded_fills.len(), 1);
        assert_eq!(result.total_pnl, 0.0);
        assert_eq!(result.final_balance, result.initial_balance);
    }

    assert_eq!(identity.mtm_equity_curve, one_leg.mtm_equity_curve);
    assert_eq!(identity.mtm_equity_curve, two_leg.mtm_equity_curve);

    let snapshots = [
        &identity.open_position_snapshots[0],
        &one_leg.open_position_snapshots[0],
        &two_leg.open_position_snapshots[0],
    ];
    for field in [
        |snapshot: &qs_backtest::OpenPositionSnapshot| snapshot.unrealized_pnl,
        |snapshot: &qs_backtest::OpenPositionSnapshot| snapshot.gross_exposure,
        |snapshot: &qs_backtest::OpenPositionSnapshot| snapshot.open_risk,
    ] {
        assert_eq!(field(snapshots[0]), field(snapshots[1]));
        assert_eq!(field(snapshots[0]), field(snapshots[2]));
    }

    assert_eq!(
        snapshots[0]
            .unrealized_pnl_conversion
            .as_ref()
            .unwrap()
            .legs
            .len(),
        0
    );
    assert_eq!(
        snapshots[1]
            .unrealized_pnl_conversion
            .as_ref()
            .unwrap()
            .legs
            .len(),
        1
    );
    assert_eq!(
        snapshots[2]
            .unrealized_pnl_conversion
            .as_ref()
            .unwrap()
            .legs
            .len(),
        2
    );
}

struct ScriptedFeed {
    batches: VecDeque<Result<Option<TimestampBatch>, &'static str>>,
}

impl FallibleBatchFeed for ScriptedFeed {
    type Error = &'static str;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        self.batches.pop_front().unwrap_or(Ok(None))
    }
}

fn primary_batch(milliseconds: usize, price: f64) -> TimestampBatch {
    TimestampBatch {
        ts: ts(milliseconds),
        events: vec![FeedEvent::new(
            tick(PRIMARY, milliseconds, price, price),
            EventMetadata::new(SeriesRoles::PRIMARY, 0, milliseconds as u64),
        )],
    }
}

#[test]
fn streaming_quiescence_leaves_an_unread_tail_without_polling_its_error() {
    let mut feed = ScriptedFeed {
        batches: VecDeque::from([
            Ok(Some(primary_batch(0, 100.0))),
            Ok(Some(primary_batch(1, 101.0))),
            Ok(Some(primary_batch(2, 102.0))),
            Err("tail must stay unread"),
        ]),
    };
    let signals = vec![
        active_signal(),
        RawSignal::Close {
            ts: ts(1),
            position: PositionRef::ByTradeId {
                trade_id: TRADE_ID.to_owned(),
            },
        },
    ];

    let result = BacktestRunner::new_future(
        config(),
        FutureQuoteConfig {
            currency_plan: Some(currency_plan(CurrencyCase::Identity)),
            mtm_output: MtmOutputPolicy::Full,
            ..FutureQuoteConfig::default()
        },
    )
    .run_raw_signals_future_streaming_controlled(
        &mut feed,
        Some(ts(2)),
        signals,
        None,
        || false,
        |_| {},
    )
    .unwrap();

    assert_eq!(feed.batches.len(), 2);
    assert!(result.open_position_snapshots.is_empty());
    assert_eq!(result.recorded_fills.len(), 2);
    assert_eq!(result.mtm_equity_curve.last().unwrap().ts, ts(1));
}
