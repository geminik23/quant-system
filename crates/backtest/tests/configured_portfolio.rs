//! Several configured strategy instances replayed against one account, with and without a portfolio supervisor.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{Duration, NaiveDateTime, NaiveTime};
use qs_backtest::data_feed::{EventMetadata, FeedEvent, SeriesRoles};
use qs_backtest::ledger::ActionDispositionStatus;
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    BacktestConfiguredStrategyAdapter, BacktestResult, BacktestRunner, BarSeriesSpec,
    ConfiguredHistoricalBindings, ConfiguredInstance, ConfiguredSourceBinding, ConversionRoute,
    FutureQuoteConfig, HistoricalVolumeProjection, MarketEvent, MissingIntervalPolicy,
    PortfolioBacktestResult, PortfolioReplayError, PriceBasis, RunCurrencyPlan, SeriesId,
    SeriesRequirement, StrategyDescriptor, StrategyId, StrategyRetentionLimits, Timeframe, VecFeed,
    WarmupRequirement,
};
use qs_risk::{CorrelationGroup, HaltAction, LossLimit, PortfolioSupervisor, RiskPolicy, Verdict};
use qs_strategy::{MaterialLibrary, SourceId, StrategyConfig};
use qs_symbols::SymbolSpec;
use serde_json::json;
use support::configured::{analysis, crossover_adapter, crossover_events, runner_config, ts};

const SPREAD: f64 = 0.0002;
const HALF: f64 = SPREAD / 2.0;
const SYMBOLS: [&str; 3] = ["EURUSD", "GBPUSD", "AUDUSD"];

/// Enter long once the last completed close is above `enter_above`, protect at `stop`, and close once a completed close is below `exit_below` or the position is gone; a rejected entry returns to flat and tries again.
fn threshold_config(
    strategy_id: &str,
    enter_above: f64,
    stop: f64,
    exit_below: f64,
) -> StrategyConfig {
    let literal_price =
        |value: f64| json!({ "op": "literal", "value": { "type": "price", "value": value } });
    serde_json::from_value(json!({
        "strategy_id": strategy_id,
        "title": "threshold",
        "initial_state": "flat",
        "sources": ["primary"],
        "trade_slots": ["primary"],
        "variables": [],
        "materials": [
            { "id": "close", "key": "completed_bar_field", "inputs": [], "params": {
                "field": { "type": "bar_field", "value": "close" },
                "source": { "type": "source", "value": "primary" } } },
            { "id": "open", "key": "position_open", "inputs": [], "params": {
                "slot": { "type": "slot", "value": "primary" } } },
            { "id": "rejected", "key": "entry_rejected", "inputs": [], "params": {
                "action": { "type": "action_kind", "value": "entry" },
                "slot": { "type": "slot", "value": "primary" } } }
        ],
        "states": [
            { "id": "flat", "transitions": [{
                "priority": 1, "target": "entering",
                "when": { "op": "gt", "left": { "op": "material", "id": "close" }, "right": literal_price(enter_above) },
                "decision": { "kind": "entry", "reason": "close above level", "trade_slot": "primary", "values": [] },
                "actions": [{
                    "action": "entry", "slot": "primary", "order_type": "Market", "targets": [],
                    "side": { "op": "literal", "value": { "type": "side", "value": "Buy" } },
                    "price": { "op": "literal", "value": { "type": "missing", "value": "price" } },
                    "risk": { "op": "literal", "value": { "type": "number", "value": 1.0 } },
                    "stoploss": literal_price(stop)
                }]
            }]},
            { "id": "entering", "transitions": [
                { "priority": 1, "target": "long", "when": { "op": "material", "id": "open" } },
                { "priority": 2, "target": "flat", "when": { "op": "material", "id": "rejected" } }
            ]},
            { "id": "long", "transitions": [
                // A position closed from outside, by its stop or by the supervisor, still needs the strategy's own close to release the slot.
                { "priority": 1, "target": "closing",
                  "when": { "op": "any", "items": [
                      { "op": "not", "value": { "op": "material", "id": "open" } },
                      { "op": "lt", "left": { "op": "material", "id": "close" }, "right": literal_price(exit_below) }
                  ] },
                  "decision": { "kind": "exit", "reason": "close below level or position gone", "trade_slot": "primary", "values": [] },
                  "actions": [{ "action": "close", "slot": "primary" }] }
            ]},
            { "id": "closing", "transitions": [
                { "priority": 1, "target": "flat", "when": { "op": "not", "value": { "op": "material", "id": "open" } } }
            ]}
        ]
    }))
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn threshold_adapter(
    strategy_id: &str,
    instance: &str,
    symbol: &str,
    timeframe_minutes: u32,
    enter_above: f64,
    stop: f64,
    exit_below: f64,
) -> BacktestConfiguredStrategyAdapter {
    threshold_adapter_with_latency(
        strategy_id,
        instance,
        symbol,
        timeframe_minutes,
        enter_above,
        stop,
        exit_below,
        0,
    )
}

#[allow(clippy::too_many_arguments)]
fn threshold_adapter_with_latency(
    strategy_id: &str,
    instance: &str,
    symbol: &str,
    timeframe_minutes: u32,
    enter_above: f64,
    stop: f64,
    exit_below: f64,
    latency_ms: u64,
) -> BacktestConfiguredStrategyAdapter {
    let strategy = qs_strategy::ConfiguredStrategy::compile(
        threshold_config(strategy_id, enter_above, stop, exit_below),
        &MaterialLibrary::builtins(),
        instance,
        symbol,
    )
    .unwrap();
    let requirement = SeriesRequirement::new(
        SeriesId::new(format!("{instance}_bars")).unwrap(),
        symbol,
        Timeframe::minutes(timeframe_minutes).unwrap(),
        PriceBasis::Bid,
        WarmupRequirement::bars(1).unwrap(),
    )
    .unwrap();
    let series = BarSeriesSpec::new(requirement, 64, 0, MissingIntervalPolicy::Skip).unwrap();
    BacktestConfiguredStrategyAdapter::new(
        strategy,
        StrategyDescriptor::new(StrategyId::new(strategy_id).unwrap(), "r1", strategy_id).unwrap(),
        ConfiguredHistoricalBindings::new(
            vec![ConfiguredSourceBinding::new(
                SourceId::new("primary").unwrap(),
                series,
            )],
            vec![],
            HistoricalVolumeProjection::TickCountExact,
        ),
        latency_ms,
    )
    .unwrap()
}

fn instance(adapter: BacktestConfiguredStrategyAdapter) -> ConfiguredInstance {
    ConfiguredInstance::new(adapter, analysis())
}

fn symbol_spec(symbol: &str) -> SymbolSpec {
    SymbolSpec {
        canonical: symbol.to_ascii_lowercase(),
        pip_position: 4,
        digits: 5,
        category: "forex".into(),
        lot_base_units: 100_000,
        lot_step_units: 1_000,
        lot_min_steps: 1,
        lot_max_steps: 0,
    }
}

fn config(sizing: SizingPolicy) -> BacktestConfig {
    BacktestConfig {
        close_on_finish: false,
        initial_balance: 10_000.0,
        sizing: Some(sizing),
        symbol_specs: SYMBOLS
            .iter()
            .map(|symbol| ((*symbol).to_owned(), symbol_spec(symbol)))
            .collect(),
        contract_sizes: SYMBOLS
            .iter()
            .map(|symbol| ((*symbol).to_owned(), 100_000.0))
            .collect(),
        ..BacktestConfig::default()
    }
}

fn future() -> FutureQuoteConfig {
    FutureQuoteConfig {
        currency_plan: Some(
            RunCurrencyPlan::new(
                "USD",
                SYMBOLS.iter().map(|symbol| (*symbol).to_owned()).collect(),
                BTreeSet::new(),
                SYMBOLS
                    .iter()
                    .map(|symbol| ((*symbol).to_owned(), "USD".to_owned()))
                    .collect(),
                BTreeMap::from([(
                    "USD".to_owned(),
                    ConversionRoute::Identity {
                        currency: "USD".to_owned(),
                    },
                )]),
                vec![],
            )
            .unwrap(),
        ),
        ..FutureQuoteConfig::default()
    }
}

fn tick(symbol: &str, minute: i64, mid: f64) -> MarketEvent {
    MarketEvent::Tick {
        symbol: symbol.into(),
        ts: ts(minute),
        bid: mid - HALF,
        ask: mid + HALF,
    }
}

fn ticks(symbol: &str, mids: &[f64]) -> Vec<MarketEvent> {
    mids.iter()
        .enumerate()
        .map(|(minute, mid)| tick(symbol, minute as i64, *mid))
        .collect()
}

fn run(
    events: Vec<MarketEvent>,
    instances: Vec<ConfiguredInstance>,
    supervisor: Option<PortfolioSupervisor>,
    sizing: SizingPolicy,
) -> PortfolioBacktestResult {
    BacktestRunner::new_future(config(sizing), future())
        .run_portfolio_future(
            &mut VecFeed::new(events),
            instances,
            supervisor,
            StrategyRetentionLimits::default(),
        )
        .unwrap()
}

fn instance_tag(result: &BacktestResult, position_id: &str) -> Option<String> {
    result
        .provider_positions
        .iter()
        .find(|position| position.id == position_id)
        .and_then(|position| position.dimensions.tags.get("instance").cloned())
}

fn rejections<'a>(result: &'a BacktestResult, prefix: &str) -> Vec<&'a str> {
    result
        .action_dispositions
        .iter()
        .filter(|disposition| disposition.status == ActionDispositionStatus::Rejected)
        .filter_map(|disposition| disposition.reason.as_deref())
        .filter(|reason| reason.starts_with(prefix))
        .collect()
}

fn economic_view(result: &BacktestResult) -> serde_json::Value {
    json!({
        "fills": result.recorded_fills,
        "closes": result.close_events,
        "completed": result.completed_positions,
        "dispositions": result.action_dispositions,
        "equity": result.mtm_equity_curve,
        "total_pnl": result.total_pnl,
        "open": result.open_position_snapshots,
    })
}

/// One stored one-minute bar per tick of the crossover fixture, stamped at its bucket open.
fn crossover_bar_events() -> Vec<FeedEvent> {
    crossover_events()
        .into_iter()
        .enumerate()
        .map(|(index, event)| {
            let MarketEvent::Tick {
                symbol, ts, bid, ..
            } = event
            else {
                unreachable!("the crossover fixture is tick-only");
            };
            FeedEvent::new(
                MarketEvent::Bar {
                    symbol,
                    ts,
                    open: bid,
                    high: bid,
                    low: bid,
                    close: bid,
                    volume: 0,
                    spread: Some(SPREAD),
                    timeframe_seconds: Some(60),
                    tick_count: Some(1),
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 0, index as u64),
            )
        })
        .collect()
}

#[test]
fn one_instance_without_a_supervisor_equals_the_single_instance_run() {
    for feed_events in [
        crossover_events()
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                FeedEvent::new(
                    event,
                    EventMetadata::new(SeriesRoles::PRIMARY, 0, index as u64),
                )
            })
            .collect::<Vec<_>>(),
        crossover_bar_events(),
    ] {
        let mut adapter = crossover_adapter();
        let single = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
            .run_configured_strategy_future(
                &mut VecFeed::from_feed_events(feed_events.clone()),
                &mut adapter,
                analysis(),
                StrategyRetentionLimits::default(),
                None,
            )
            .unwrap();
        let portfolio = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
            .run_portfolio_future(
                &mut VecFeed::from_feed_events(feed_events),
                vec![instance(crossover_adapter())],
                None,
                StrategyRetentionLimits::default(),
            )
            .unwrap();
        assert!(
            !single.replay.recorded_fills.is_empty(),
            "the fixture trades"
        );
        assert_eq!(
            economic_view(&portfolio.replay),
            economic_view(&single.replay)
        );
        assert_eq!(
            serde_json::to_value(&portfolio.instances[0].decisions).unwrap(),
            serde_json::to_value(&single.decisions).unwrap()
        );
        assert!(portfolio.supervisor.is_none());
    }
}

#[test]
fn instances_on_different_symbols_share_one_balance_and_label_their_positions() {
    let mut events = ticks(
        "EURUSD",
        &[1.0990, 1.1010, 1.1020, 1.1030, 1.0990, 1.0980, 1.0980],
    );
    events.extend(ticks(
        "GBPUSD",
        &[
            1.2990, 1.3010, 1.3040, 1.3060, 1.3070, 1.2980, 1.2990, 1.2990,
        ],
    ));
    let result = run(
        events,
        vec![
            instance(threshold_adapter(
                "threshold",
                "eur",
                "EURUSD",
                1,
                1.1000,
                1.0500,
                1.1000,
            )),
            instance(threshold_adapter(
                "threshold",
                "gbp",
                "GBPUSD",
                1,
                1.3000,
                1.2500,
                1.3000,
            )),
        ],
        None,
        SizingPolicy::FixedLot { lots: 1.0 },
    );
    let replay = &result.replay;
    assert_eq!(replay.completed_positions.len(), 2);
    let tags = replay
        .completed_positions
        .iter()
        .map(|position| instance_tag(replay, &position.position_id))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        tags,
        BTreeSet::from([Some("eur".to_owned()), Some("gbp".to_owned())])
    );
    let summed = replay
        .completed_positions
        .iter()
        .map(|position| position.net_pnl)
        .sum::<f64>();
    assert!((replay.total_pnl - summed).abs() < 1e-6);
    assert!(!replay.mtm_equity_curve.is_empty());
    assert_eq!(result.instances.len(), 2);
    assert_eq!(result.instances[0].instance_id, "eur");
    assert_eq!(result.instances[1].instance_id, "gbp");
}

#[test]
fn instances_reading_different_stored_bar_lengths_of_one_symbol_execute_on_the_shorter() {
    let mut events = Vec::new();
    for minute in 0..15 {
        let mid = 1.1000 + 0.0005 * minute as f64;
        events.push(FeedEvent::new(
            MarketEvent::Bar {
                symbol: "EURUSD".into(),
                ts: ts(minute),
                open: mid,
                high: mid + 0.0002,
                low: mid - 0.0002,
                close: mid + 0.0001,
                volume: 0,
                spread: Some(SPREAD),
                timeframe_seconds: Some(60),
                tick_count: Some(5),
            },
            EventMetadata::new(SeriesRoles::PRIMARY, 0, minute as u64),
        ));
        if minute % 5 == 0 {
            events.push(FeedEvent::new(
                MarketEvent::Bar {
                    symbol: "EURUSD".into(),
                    ts: ts(minute),
                    open: mid,
                    high: mid + 0.0030,
                    low: mid - 0.0030,
                    close: mid + 0.0020,
                    volume: 0,
                    spread: Some(SPREAD),
                    timeframe_seconds: Some(300),
                    tick_count: Some(25),
                },
                EventMetadata::new(SeriesRoles::PRIMARY, 1, minute as u64),
            ));
        }
    }
    let result = BacktestRunner::new_future(config(SizingPolicy::FixedLot { lots: 1.0 }), future())
        .run_portfolio_future(
            &mut VecFeed::from_feed_events(events),
            vec![
                instance(threshold_adapter(
                    "threshold",
                    "m1",
                    "EURUSD",
                    1,
                    1.1010,
                    1.0500,
                    1.0000,
                )),
                instance(threshold_adapter(
                    "threshold",
                    "m5",
                    "EURUSD",
                    5,
                    1.1010,
                    1.0500,
                    1.0000,
                )),
            ],
            None,
            StrategyRetentionLimits::default(),
        )
        .unwrap();
    let fills = &result.replay.recorded_fills;
    assert_eq!(fills.len(), 2);
    for fill in fills {
        // Every fill happens at the open of a one-minute bar: the five-minute bars only feed the five-minute series.
        let minute = (fill.execution_ts.unwrap() - ts(0)).num_minutes();
        let open = 1.1000 + 0.0005 * minute as f64;
        assert!((fill.fill.price - (open + HALF)).abs() < 1e-9);
    }
    // The one-minute instance decides first; the five-minute instance can only decide on a five-minute boundary.
    let m5_decision = result.instances[1]
        .decisions
        .records
        .iter()
        .find(|record| !record.emitted_signals().is_empty())
        .expect("the five-minute instance enters");
    assert_eq!(
        (m5_decision.observed_through() - ts(0)).num_minutes() % 5,
        0
    );
}

#[test]
fn a_repeated_instance_identifier_is_rejected_even_across_strategies() {
    let error = BacktestRunner::new_future(config(SizingPolicy::FixedLot { lots: 1.0 }), future())
        .run_portfolio_future(
            &mut VecFeed::new(vec![]),
            vec![
                instance(threshold_adapter(
                    "threshold",
                    "same",
                    "EURUSD",
                    1,
                    1.1,
                    1.0,
                    1.0,
                )),
                instance(threshold_adapter(
                    "other", "same", "GBPUSD", 1, 1.3, 1.2, 1.2,
                )),
            ],
            None,
            StrategyRetentionLimits::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        PortfolioReplayError::DuplicateInstanceIdentity { .. }
    ));
}

fn two_symbol_entries() -> Vec<MarketEvent> {
    let mut events = ticks("EURUSD", &[1.0990, 1.1010, 1.1020, 1.1030, 1.1040]);
    events.extend(ticks("GBPUSD", &[1.2990, 1.3010, 1.3020, 1.3030, 1.3040]));
    events
}

fn two_symbol_instances() -> Vec<ConfiguredInstance> {
    vec![
        instance(threshold_adapter(
            "threshold",
            "eur",
            "EURUSD",
            1,
            1.1000,
            1.0900,
            1.0000,
        )),
        instance(threshold_adapter(
            "threshold",
            "gbp",
            "GBPUSD",
            1,
            1.3000,
            1.2900,
            1.0000,
        )),
    ]
}

#[test]
fn max_open_positions_rejects_the_later_instance_in_the_same_boundary() {
    let supervisor =
        PortfolioSupervisor::new(vec![RiskPolicy::MaxOpenPositions { limit: 1 }], vec![]).unwrap();
    let result = run(
        two_symbol_entries(),
        two_symbol_instances(),
        Some(supervisor),
        SizingPolicy::FixedLot { lots: 1.0 },
    );
    assert_eq!(result.replay.recorded_fills.len(), 1);
    assert_eq!(result.replay.recorded_fills[0].symbol, "EURUSD");
    assert!(!rejections(&result.replay, "max_open_positions").is_empty());
    let supervisor = result.supervisor.unwrap();
    let first = &supervisor.events[0];
    let second = &supervisor.events[1];
    assert_eq!(
        first.ts, second.ts,
        "both entries are reviewed in one boundary"
    );
    assert_eq!(first.instance_id, "eur");
    assert!(first.verdict.is_approved());
    assert_eq!(second.instance_id, "gbp");
    assert!(
        matches!(&second.verdict, Verdict::Reject { policy, .. } if policy == "max_open_positions")
    );
    assert!(supervisor.rejected_entries() >= 1);
}

fn usd_group(cap: f64) -> PortfolioSupervisor {
    PortfolioSupervisor::new(
        vec![RiskPolicy::GroupRiskCap {
            group: "usd".into(),
            max_group_risk: cap,
        }],
        vec![CorrelationGroup {
            id: "usd".into(),
            symbols: ["EURUSD".to_owned(), "GBPUSD".to_owned()].into(),
        }],
    )
    .unwrap()
}

#[test]
fn a_group_risk_cap_rejects_the_entry_that_would_exceed_it() {
    let mut events = two_symbol_entries();
    events.extend(ticks("AUDUSD", &[0.6990, 0.7010, 0.7020, 0.7030, 0.7040]));
    let mut instances = two_symbol_instances();
    instances.push(instance(threshold_adapter(
        "threshold",
        "aud",
        "AUDUSD",
        1,
        0.7000,
        0.6900,
        0.0100,
    )));
    let result = run(
        events,
        instances,
        Some(usd_group(150.0)),
        SizingPolicy::FixedRiskAmount { amount: 100.0 },
    );
    let symbols = result
        .replay
        .recorded_fills
        .iter()
        .map(|fill| fill.symbol.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(symbols, BTreeSet::from(["AUDUSD", "EURUSD"]));
    assert!(!rejections(&result.replay, "group_risk_cap").is_empty());
    let event = result
        .supervisor
        .as_ref()
        .unwrap()
        .events
        .iter()
        .find(|event| event.instance_id == "gbp")
        .unwrap();
    assert_eq!(event.requested_risk, Some(100.0));
}

#[test]
fn a_group_risk_cap_with_fixed_lots_is_rejected_before_the_feed_is_read() {
    let error = BacktestRunner::new_future(config(SizingPolicy::FixedLot { lots: 1.0 }), future())
        .run_portfolio_future(
            &mut VecFeed::new(two_symbol_entries()),
            two_symbol_instances(),
            Some(usd_group(150.0)),
            StrategyRetentionLimits::default(),
        )
        .unwrap_err();
    assert!(matches!(error, PortfolioReplayError::Supervisor(_)));
}

#[test]
fn a_daily_loss_halt_blocks_entries_until_the_reset() {
    // EURUSD enters at minute 3 and stops out at minute 4 for a loss near the 100 it risked; GBPUSD wants in from minute 7.
    let mut eur = vec![1.0990, 1.1010, 1.1020, 1.1030, 1.0880];
    eur.extend(std::iter::repeat_n(1.0870, 13));
    let mut gbp = vec![1.2990; 6];
    gbp.extend(std::iter::repeat_n(1.3010, 12));
    let mut events = ticks("EURUSD", &eur);
    events.extend(ticks("GBPUSD", &gbp));
    let supervisor = PortfolioSupervisor::new(
        vec![RiskPolicy::DailyLossHalt {
            max_loss: LossLimit::Amount(50.0),
            reset_at_utc: NaiveTime::from_hms_opt(12, 10, 0).unwrap(),
        }],
        vec![],
    )
    .unwrap();
    let result = run(
        events,
        vec![
            instance(threshold_adapter(
                "threshold",
                "eur",
                "EURUSD",
                1,
                1.1000,
                1.0900,
                1.0000,
            )),
            instance(threshold_adapter(
                "threshold",
                "gbp",
                "GBPUSD",
                1,
                1.3000,
                1.2900,
                1.0000,
            )),
        ],
        Some(supervisor),
        SizingPolicy::FixedRiskAmount { amount: 100.0 },
    );
    assert!(!rejections(&result.replay, "daily_loss_halt").is_empty());
    let gbp_fill = result
        .replay
        .recorded_fills
        .iter()
        .find(|fill| fill.symbol == "GBPUSD")
        .expect("the entry is admitted after the reset");
    assert!(gbp_fill.execution_ts.unwrap() > ts(10));
    let supervisor = result.supervisor.unwrap();
    assert_eq!(supervisor.halts.len(), 1);
    assert_eq!(supervisor.halts[0].to, Some(ts(10)));
    let halted_from = supervisor.halts[0].from;
    assert!(halted_from >= ts(4) && halted_from < ts(10));
    assert!(supervisor.halt_minutes(ts(20)) > 0);
}

fn drawdown_events() -> Vec<MarketEvent> {
    // Both instances are long by minute 3; EURUSD then falls far enough to draw the account down by more than one percent, and GBPUSD later closes below its exit.
    let mut events = ticks(
        "EURUSD",
        &[
            1.0990, 1.1010, 1.1020, 1.1030, 1.1000, 1.0880, 1.0880, 1.0880, 1.1030, 1.1030, 1.1030,
        ],
    );
    events.extend(ticks(
        "GBPUSD",
        &[
            1.2990, 1.3010, 1.3020, 1.3030, 1.3030, 1.3030, 1.3030, 1.2950, 1.2950, 1.2950, 1.2950,
        ],
    ));
    events
}

fn drawdown_instances() -> Vec<ConfiguredInstance> {
    vec![
        instance(threshold_adapter(
            "threshold",
            "eur",
            "EURUSD",
            1,
            1.1000,
            1.0500,
            1.0950,
        )),
        instance(threshold_adapter(
            "threshold",
            "gbp",
            "GBPUSD",
            1,
            1.3000,
            1.2500,
            1.3000,
        )),
    ]
}

#[test]
fn a_kill_switch_halt_rejects_new_entries_but_never_blocks_a_close() {
    let supervisor = PortfolioSupervisor::new(
        vec![RiskPolicy::KillSwitch {
            max_drawdown_percent: 1.0,
            action: HaltAction::Halt,
        }],
        vec![],
    )
    .unwrap();
    let result = run(
        drawdown_events(),
        drawdown_instances(),
        Some(supervisor),
        SizingPolicy::FixedLot { lots: 1.0 },
    );
    let supervisor = result.supervisor.unwrap();
    assert_eq!(supervisor.halts.len(), 1);
    assert_eq!(supervisor.halts[0].to, None);
    // Both strategies close on their own exits while halted; the EURUSD instance then wants back in and is refused.
    let closes = result
        .replay
        .close_events
        .iter()
        .map(|close| close.symbol.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(closes, BTreeSet::from(["EURUSD", "GBPUSD"]));
    assert!(
        result
            .replay
            .close_events
            .iter()
            .all(|close| close.ts > supervisor.halts[0].from)
    );
    assert!(!rejections(&result.replay, "kill_switch").is_empty());
}

#[test]
fn a_kill_switch_that_closes_everything_does_so_at_the_next_quote() {
    let supervisor = PortfolioSupervisor::new(
        vec![RiskPolicy::KillSwitch {
            max_drawdown_percent: 1.0,
            action: HaltAction::HaltAndCloseAll,
        }],
        vec![],
    )
    .unwrap();
    let result = run(
        drawdown_events(),
        drawdown_instances(),
        Some(supervisor),
        SizingPolicy::FixedLot { lots: 1.0 },
    );
    let supervisor = result.supervisor.unwrap();
    let tripped = supervisor.halts[0].from;
    assert_eq!(supervisor.halt_actions.len(), 2);
    let next_quote: NaiveDateTime = tripped + Duration::minutes(1);
    let supervisor_closes = result
        .replay
        .close_events
        .iter()
        .filter(|close| {
            close
                .action_id
                .as_deref()
                .is_some_and(|action| action.starts_with("supervisor:"))
        })
        .collect::<Vec<_>>();
    assert_eq!(supervisor_closes.len(), 2);
    assert!(supervisor_closes.iter().all(|close| close.ts == next_quote));
    assert!(result.replay.open_position_snapshots.is_empty());
}

#[test]
fn an_instance_reads_nothing_before_its_own_feed_start() {
    // Both instances would enter on the first completed bar above the level; the second may only read from minute 6, as if its own run had loaded data from its warmup start.
    let events = ticks("EURUSD", &[1.1010; 12]);
    let result = run(
        events,
        vec![
            instance(threshold_adapter(
                "threshold",
                "early",
                "EURUSD",
                1,
                1.1000,
                1.0500,
                1.0000,
            )),
            instance(threshold_adapter(
                "threshold",
                "late",
                "EURUSD",
                1,
                1.1000,
                1.0500,
                1.0000,
            ))
            .with_feed_from(ts(6)),
        ],
        None,
        SizingPolicy::FixedLot { lots: 1.0 },
    );
    let first_decision = |index: usize| {
        result.instances[index]
            .decisions
            .records
            .iter()
            .find(|record| !record.emitted_signals().is_empty())
            .map(|record| record.observed_through())
            .expect("the instance enters")
    };
    assert_eq!(first_decision(0), ts(1));
    assert_eq!(first_decision(1), ts(7));
}

#[test]
fn a_halt_rejects_an_approved_entry_that_has_not_reached_the_market() {
    let supervisor = PortfolioSupervisor::new(
        vec![RiskPolicy::KillSwitch {
            max_drawdown_percent: 1.0,
            action: HaltAction::HaltAndCloseAll,
        }],
        vec![],
    )
    .unwrap();
    // The GBPUSD instance decides at minute 2 with a four-minute decision latency, so its approved Entry is still waiting when the EURUSD drop trips the kill switch at minute 5.
    let result = run(
        drawdown_events(),
        vec![
            instance(threshold_adapter(
                "threshold",
                "eur",
                "EURUSD",
                1,
                1.1000,
                1.0500,
                1.0950,
            )),
            instance(threshold_adapter_with_latency(
                "threshold",
                "gbp",
                "GBPUSD",
                1,
                1.3000,
                1.2500,
                1.0000,
                240_000,
            )),
        ],
        Some(supervisor),
        SizingPolicy::FixedLot { lots: 1.0 },
    );
    let supervisor = result.supervisor.unwrap();
    let tripped = supervisor.halts[0].from;
    assert!(tripped < ts(6), "the halt precedes the delayed entry");
    assert!(
        result
            .replay
            .recorded_fills
            .iter()
            .all(|fill| fill.symbol != "GBPUSD"),
        "the delayed GBPUSD entry never fills"
    );
    assert!(
        !rejections(
            &result.replay,
            "kill_switch: new exposure is halted and the request had not reached the market"
        )
        .is_empty()
    );
    assert!(result.replay.open_position_snapshots.is_empty());
    assert!(result.replay.pending_order_snapshots.is_empty());
}

#[test]
fn a_feed_mixing_ticks_and_stored_bars_is_rejected() {
    let mut events = vec![FeedEvent::new(
        tick("EURUSD", 0, 1.1000),
        EventMetadata::new(SeriesRoles::PRIMARY, 0, 0),
    )];
    events.push(FeedEvent::new(
        MarketEvent::Bar {
            symbol: "GBPUSD".into(),
            ts: ts(1),
            open: 1.3,
            high: 1.3,
            low: 1.3,
            close: 1.3,
            volume: 0,
            spread: Some(SPREAD),
            timeframe_seconds: Some(60),
            tick_count: Some(1),
        },
        EventMetadata::new(SeriesRoles::PRIMARY, 1, 0),
    ));
    let error = BacktestRunner::new_future(config(SizingPolicy::FixedLot { lots: 1.0 }), future())
        .run_portfolio_future(
            &mut VecFeed::from_feed_events(events),
            vec![
                instance(threshold_adapter(
                    "threshold",
                    "eur",
                    "EURUSD",
                    1,
                    1.1,
                    1.0,
                    1.0,
                )),
                instance(threshold_adapter(
                    "threshold",
                    "gbp",
                    "GBPUSD",
                    1,
                    1.3,
                    1.2,
                    1.2,
                )),
            ],
            None,
            StrategyRetentionLimits::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        PortfolioReplayError::MixedPrimaryInput { timestamp } if timestamp == ts(1)
    ));
}

#[test]
fn an_instance_on_a_symbol_the_account_cannot_size_is_rejected_before_the_feed_is_read() {
    let error = BacktestRunner::new_future(config(SizingPolicy::FixedLot { lots: 1.0 }), future())
        .run_portfolio_future(
            &mut VecFeed::new(vec![]),
            vec![instance(threshold_adapter(
                "threshold",
                "jpy",
                "USDJPY",
                1,
                150.0,
                140.0,
                140.0,
            ))],
            None,
            StrategyRetentionLimits::default(),
        )
        .unwrap_err();
    assert!(
        matches!(&error, PortfolioReplayError::Instance { instance_id, .. } if instance_id == "jpy"),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains("missing instrument or symbol spec for USDJPY")
    );
}
