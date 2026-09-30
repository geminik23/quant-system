mod support;

use std::collections::VecDeque;
use std::convert::Infallible;

use qs_backtest::data_feed::{FallibleBatchFeed, TimestampBatch};
use qs_backtest::{BacktestRunner, FutureQuoteConfig, StrategyRetentionLimits};
use support::configured::{
    analysis, lifecycle_adapter, runner_config, scenario_batches, scenario_feed, ts,
};

struct BatchFeed {
    batches: VecDeque<TimestampBatch>,
}

impl FallibleBatchFeed for BatchFeed {
    type Error = Infallible;

    fn next_batch(&mut self) -> Result<Option<TimestampBatch>, Self::Error> {
        Ok(self.batches.pop_front())
    }
}

#[test]
fn materialized_and_streaming_configured_replay_match() {
    let mut materialized_adapter = lifecycle_adapter();
    let materialized = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .run_configured_strategy_future(
            &mut scenario_feed(),
            &mut materialized_adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    let mut streaming_adapter = lifecycle_adapter();
    let mut stream = BatchFeed {
        batches: VecDeque::from(scenario_batches()),
    };
    let streaming = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .run_configured_strategy_future_streaming(
            &mut stream,
            Some(ts(9)),
            &mut streaming_adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
        )
        .unwrap();

    assert_eq!(
        serde_json::to_value(&materialized.decisions).unwrap(),
        serde_json::to_value(&streaming.decisions).unwrap()
    );
    let materialized_signals = materialized
        .decisions
        .records
        .iter()
        .flat_map(|record| record.emitted_signals())
        .collect::<Vec<_>>();
    let streaming_signals = streaming
        .decisions
        .records
        .iter()
        .flat_map(|record| record.emitted_signals())
        .collect::<Vec<_>>();
    assert_eq!(
        serde_json::to_value(materialized_signals).unwrap(),
        serde_json::to_value(streaming_signals).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&materialized.research.journal).unwrap(),
        serde_json::to_value(&streaming.research.journal).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&materialized.replay).unwrap(),
        serde_json::to_value(&streaming.replay).unwrap()
    );
    assert_eq!(
        materialized_adapter.configured_strategy().state_id(),
        streaming_adapter.configured_strategy().state_id()
    );
}

#[test]
fn controlled_configured_replay_reports_progress_and_stops_when_cancelled() {
    use std::cell::Cell;

    let completed = {
        let mut adapter = lifecycle_adapter();
        let mut stream = BatchFeed {
            batches: VecDeque::from(scenario_batches()),
        };
        let progress = Cell::new(0usize);
        BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
            .run_configured_strategy_future_streaming_controlled(
                &mut stream,
                Some(ts(9)),
                &mut adapter,
                analysis(),
                StrategyRetentionLimits::default(),
                None,
                || false,
                |_| progress.set(progress.get() + 1),
            )
            .unwrap();
        progress.get()
    };
    assert!(completed > 0, "an uncancelled run reports progress");

    let mut adapter = lifecycle_adapter();
    let mut stream = BatchFeed {
        batches: VecDeque::from(scenario_batches()),
    };
    let polls = Cell::new(0usize);
    let error = BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
        .run_configured_strategy_future_streaming_controlled(
            &mut stream,
            Some(ts(9)),
            &mut adapter,
            analysis(),
            StrategyRetentionLimits::default(),
            None,
            || {
                polls.set(polls.get() + 1);
                polls.get() > 3
            },
            |_| {},
        )
        .err()
        .unwrap();
    assert!(matches!(error, qs_backtest::StrategyReplayError::Cancelled));
}
