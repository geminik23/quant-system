mod support;

use std::cell::Cell;
use std::collections::VecDeque;
use std::convert::Infallible;

use qs_backtest::artifacts::{EntryProfileResolutionAudit, EntryProfileSelectionSource};
use qs_backtest::data_feed::{
    EventMetadata, FallibleBatchFeed, FeedEvent, SeriesRoles, TimestampBatch,
};
use qs_backtest::{
    BacktestResult, BacktestRunner, ConfiguredEntryProfileError, EntryGeometryPolicy,
    FutureQuoteConfig, ManagementProfile, MarketEvent, PreparedEntryProfiles, RawSignal,
    StoplossMode, StrategyBacktestResult, StrategyReplayError, StrategyReplayInputError,
    StrategyRetentionLimits, TargetSource, VecFeed,
};
use qs_core::{CloseReason, RuleConfigDef};
use support::configured::{
    SYMBOL, analysis, crossover_adapter_with_class, lifecycle_adapter, runner_config,
    scenario_feed, ts,
};

const TRAILING_DISTANCE: f64 = 0.5;
/// Decision latency given to the configured adapter and signal latency given to the direct run, so both schedule the same signal at the same instant.
const DECISION_LATENCY_MS: u64 = 60_000;

fn profile(
    name: &str,
    stoploss_mode: StoplossMode,
    rules: Vec<RuleConfigDef>,
) -> ManagementProfile {
    ManagementProfile {
        name: name.into(),
        target_selection: None,
        use_targets: vec![],
        close_ratios: vec![],
        target_source: TargetSource::FromSignal,
        stoploss_mode,
        rules,
        group_override: None,
        let_remainder_run: false,
        entry_geometry: EntryGeometryPolicy::Strict,
    }
}

fn trailing_profile() -> ManagementProfile {
    profile(
        "trail",
        StoplossMode::FromSignal,
        vec![RuleConfigDef::TrailingStop {
            distance: TRAILING_DISTANCE,
        }],
    )
}

fn routed(entry_class: &str, profile: ManagementProfile) -> PreparedEntryProfiles {
    PreparedEntryProfiles::try_new(None, [(entry_class.to_owned(), profile)]).unwrap()
}

/// Rises long enough for the fast EMA to cross above and for the entry to fill near the top, then falls through the trailing distance.
fn profiled_events() -> Vec<MarketEvent> {
    [2.0, 1.0, 2.0, 2.0, 2.2, 2.4, 2.6, 1.8, 1.6, 1.4, 1.2, 1.0]
        .into_iter()
        .enumerate()
        .map(|(minute, bid)| MarketEvent::Tick {
            symbol: SYMBOL.into(),
            ts: ts(minute as i64),
            bid,
            ask: bid + 0.0002,
        })
        .collect()
}

fn profiled_feed() -> VecFeed {
    VecFeed::new(profiled_events())
}

fn profiled_batches() -> Vec<TimestampBatch> {
    profiled_events()
        .into_iter()
        .enumerate()
        .map(|(index, event)| TimestampBatch {
            ts: event.ts(),
            events: vec![FeedEvent::new(
                event,
                EventMetadata::new(SeriesRoles::PRIMARY, 0, index as u64),
            )],
        })
        .collect()
}

struct BatchFeed {
    batches: VecDeque<TimestampBatch>,
}

impl FallibleBatchFeed for BatchFeed {
    type Error = Infallible;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        Ok(self.batches.pop_front())
    }
}

fn generated_signals(result: &StrategyBacktestResult) -> Vec<RawSignal> {
    result
        .decisions
        .records
        .iter()
        .flat_map(|record| record.emitted_signals().iter().cloned())
        .collect()
}

fn resolutions(result: &BacktestResult) -> &[EntryProfileResolutionAudit] {
    &result
        .execution_metadata
        .as_ref()
        .expect("FutureQuote runs record execution metadata")
        .entry_profile_resolutions
}

fn json<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}

fn without_action_ids(mut value: serde_json::Value) -> serde_json::Value {
    fn strip(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(items) => items.iter_mut().for_each(strip),
            serde_json::Value::Object(fields) => {
                fields.remove("action_id");
                fields.values_mut().for_each(strip);
            }
            _ => {}
        }
    }
    strip(&mut value);
    value
}

fn direct_runner() -> BacktestRunner {
    BacktestRunner::new_future(
        runner_config(),
        FutureQuoteConfig {
            signal_latency_ms: DECISION_LATENCY_MS as i64,
            ..FutureQuoteConfig::default()
        },
    )
}

fn assert_economic_parity(configured: &BacktestResult, direct: &BacktestResult) {
    let fills = |result: &BacktestResult| {
        result
            .recorded_fills
            .iter()
            .map(|fill| (fill.effective_ts, fill.execution_ts, fill.size, fill.fill))
            .collect::<Vec<_>>()
    };
    assert_eq!(fills(configured), fills(direct));
    let closes = |result: &BacktestResult| {
        result
            .close_events
            .iter()
            .map(|event| (event.ts, event.size, event.price, event.pnl, event.reason))
            .collect::<Vec<_>>()
    };
    assert_eq!(closes(configured), closes(direct));
    let dispositions = |result: &BacktestResult| {
        result
            .action_dispositions
            .iter()
            .map(|item| (item.action_kind.clone(), item.effective_ts, item.status))
            .collect::<Vec<_>>()
    };
    assert_eq!(dispositions(configured), dispositions(direct));
    assert_eq!(configured.final_balance, direct.final_balance);
    assert_eq!(configured.total_pnl, direct.total_pnl);
    assert_eq!(json(&configured.trade_log), json(&direct.trade_log));
    assert_eq!(
        without_action_ids(json(&configured.completed_positions)),
        without_action_ids(json(&direct.completed_positions))
    );
    let metadata = |result: &BacktestResult| {
        let value = json(&result.execution_metadata);
        without_action_ids(serde_json::json!({
            "default": value["entry_profile_default"],
            "routes": value["entry_profile_routes"],
            "resolutions": value["entry_profile_resolutions"],
        }))
    };
    assert_eq!(metadata(configured), metadata(direct));
}

#[test]
fn routed_entry_class_matches_direct_signals_with_the_same_class() {
    let mut adapter = crossover_adapter_with_class(Some("trend"), DECISION_LATENCY_MS);
    let configured = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .with_entry_profiles(routed("trend", trailing_profile()))
        .run_configured_strategy_future(
            &mut profiled_feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();
    let signals = generated_signals(&configured);
    assert!(matches!(
        signals.first(),
        Some(RawSignal::Entry { entry_class: Some(class), .. }) if class == "trend"
    ));

    let direct = direct_runner()
        .with_entry_profiles(routed("trend", trailing_profile()))
        .run_raw_signals_future(&mut profiled_feed(), signals, None);
    assert_economic_parity(&configured.replay, &direct);

    let resolution = &resolutions(&configured.replay)[0];
    assert_eq!(resolution.entry_class.as_deref(), Some("trend"));
    assert_eq!(
        resolution.selection_source,
        EntryProfileSelectionSource::Mapped
    );
    assert_eq!(resolution.selected_profile_name.as_deref(), Some("trail"));
    assert_eq!(
        configured.replay.close_events[0].reason,
        CloseReason::TrailingStop
    );
}

#[test]
fn run_default_profile_applies_to_unclassified_configured_entries() {
    let default = trailing_profile();
    let mut adapter = crossover_adapter_with_class(None, DECISION_LATENCY_MS);
    let configured = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut profiled_feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            Some(&default),
        )
        .unwrap();
    let direct = direct_runner().run_raw_signals_future(
        &mut profiled_feed(),
        generated_signals(&configured),
        Some(&default),
    );
    assert_economic_parity(&configured.replay, &direct);

    let resolution = &resolutions(&configured.replay)[0];
    assert_eq!(resolution.entry_class, None);
    assert_eq!(
        resolution.selection_source,
        EntryProfileSelectionSource::RunDefault
    );
    assert_eq!(resolution.selected_profile_name.as_deref(), Some("trail"));
}

#[test]
fn profiled_streaming_run_matches_the_materialized_run() {
    let mut materialized_adapter = crossover_adapter_with_class(Some("trend"), DECISION_LATENCY_MS);
    let materialized = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .with_entry_profiles(routed("trend", trailing_profile()))
        .run_configured_strategy_future(
            &mut profiled_feed(),
            &mut materialized_adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    let mut streaming_adapter = crossover_adapter_with_class(Some("trend"), DECISION_LATENCY_MS);
    let batches = profiled_batches();
    let primary_eod = batches.last().map(|batch| batch.ts);
    let mut feed = BatchFeed {
        batches: VecDeque::from(batches),
    };
    let streaming = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .with_entry_profiles(routed("trend", trailing_profile()))
        .run_configured_strategy_future_streaming(
            &mut feed,
            primary_eod,
            &mut streaming_adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    assert_eq!(
        json(&generated_signals(&materialized)),
        json(&generated_signals(&streaming))
    );
    assert_economic_parity(&materialized.replay, &streaming.replay);
}

struct PollTrackingFeed {
    inner: VecFeed,
    polls: Cell<usize>,
}

impl qs_backtest::DataFeed for PollTrackingFeed {
    fn next_event(&mut self) -> Option<MarketEvent> {
        self.polls.set(self.polls.get() + 1);
        self.inner.next_event()
    }

    fn peek(&self) -> Option<&MarketEvent> {
        self.inner.peek()
    }
}

fn preflight_error(
    runner: BacktestRunner,
    adapter: &mut qs_backtest::BacktestConfiguredStrategyAdapter,
    profile: Option<&ManagementProfile>,
) -> StrategyReplayInputError {
    let mut feed = PollTrackingFeed {
        inner: profiled_feed(),
        polls: Cell::new(0),
    };
    let error = runner
        .run_configured_strategy_future(
            &mut feed,
            adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            profile,
        )
        .unwrap_err();
    assert_eq!(
        feed.polls.get(),
        0,
        "preflight must fail before the feed is read"
    );
    match error {
        StrategyReplayError::Input(error) => error,
        other => panic!("expected an input error, got {other:?}"),
    }
}

fn runner() -> BacktestRunner {
    BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
}

#[test]
fn unrouted_entry_class_fails_before_feed_polling() {
    let unrouted = StrategyReplayInputError::ConfiguredEntryProfile(
        ConfiguredEntryProfileError::UnroutedEntryClass {
            entry_class: "trend".into(),
            slot: "primary".into(),
        },
    );

    let error = preflight_error(
        runner(),
        &mut crossover_adapter_with_class(Some("trend"), DECISION_LATENCY_MS),
        None,
    );
    assert_eq!(error, unrouted);

    let default = trailing_profile();
    let error = preflight_error(
        runner(),
        &mut crossover_adapter_with_class(Some("trend"), DECISION_LATENCY_MS),
        Some(&default),
    );
    assert_eq!(
        error, unrouted,
        "a run default profile does not route a classified entry"
    );

    let error = preflight_error(
        runner().with_entry_profiles(routed("range", trailing_profile())),
        &mut crossover_adapter_with_class(Some("trend"), DECISION_LATENCY_MS),
        None,
    );
    assert_eq!(error, unrouted);
}

#[test]
fn invalid_run_default_profile_fails_before_feed_polling() {
    let mut invalid = trailing_profile();
    invalid.target_source = TargetSource::StopDistanceMultiples { multiples: vec![] };
    let error = preflight_error(
        runner(),
        &mut crossover_adapter_with_class(None, DECISION_LATENCY_MS),
        Some(&invalid),
    );
    assert!(matches!(
        error,
        StrategyReplayInputError::ManagementProfile(_)
    ));
}

#[test]
fn stop_owning_profile_conflicts_with_a_strategy_that_moves_the_stop() {
    let owners = [
        trailing_profile(),
        profile(
            "fixed_distance",
            StoplossMode::FixedDistance { distance: 0.2 },
            vec![],
        ),
        profile(
            "breakeven",
            StoplossMode::FromSignal,
            vec![RuleConfigDef::BreakevenAfterTargets { after_n: 1 }],
        ),
    ];
    for owner in owners {
        let name = owner.name.clone();
        let error = preflight_error(runner(), &mut lifecycle_adapter(), Some(&owner));
        assert_eq!(
            error,
            StrategyReplayInputError::ConfiguredEntryProfile(
                ConfiguredEntryProfileError::StoplossOwnerConflict {
                    profile: name,
                    slot: "primary".into(),
                    entry_class: None,
                }
            )
        );
    }
}

#[test]
fn profile_that_leaves_the_stop_alone_composes_with_strategy_stop_moves() {
    let time_exit = profile(
        "time_exit",
        StoplossMode::FromSignal,
        vec![RuleConfigDef::TimeExit { max_seconds: 3_600 }],
    );
    let mut adapter = lifecycle_adapter();
    let result = runner()
        .run_configured_strategy_future(
            &mut scenario_feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            Some(&time_exit),
        )
        .unwrap();
    let resolution = &resolutions(&result.replay)[0];
    assert_eq!(
        resolution.selected_profile_name.as_deref(),
        Some("time_exit")
    );
    assert!(
        result
            .replay
            .close_events
            .iter()
            .all(|event| event.reason == CloseReason::Manual),
        "the strategy's own close still ends the position"
    );
}

#[test]
fn profile_take_profit_reduces_a_configured_position_like_direct_signals() {
    let scaled_out = profile(
        "scale_out",
        StoplossMode::FromSignal,
        vec![
            RuleConfigDef::TakeProfit {
                price: 2.5,
                close_ratio: 0.5,
            },
            RuleConfigDef::TrailingStop {
                distance: TRAILING_DISTANCE,
            },
        ],
    );
    let mut adapter = crossover_adapter_with_class(Some("trend"), DECISION_LATENCY_MS);
    let configured = runner()
        .with_entry_profiles(routed("trend", scaled_out.clone()))
        .run_configured_strategy_future(
            &mut profiled_feed(),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();
    let direct = direct_runner()
        .with_entry_profiles(routed("trend", scaled_out))
        .run_raw_signals_future(&mut profiled_feed(), generated_signals(&configured), None);
    assert_economic_parity(&configured.replay, &direct);

    let reasons = configured
        .replay
        .close_events
        .iter()
        .map(|event| event.reason)
        .collect::<Vec<_>>();
    assert_eq!(
        reasons,
        vec![CloseReason::Target, CloseReason::TrailingStop]
    );
}
