//! Caller-supplied run tags must reach evaluation, because that is the only thing they are for.
//!
//! A batch of runs distinguishes one run from another by tagging it. If the tags stop at the artifacts, evaluation cannot group positions by parameter or window and the whole point of recording them is lost.

mod support;

use std::collections::BTreeMap;

use qs_backtest::evaluation::{BreakdownDimension, BreakdownValue, EvaluationOptions};
use qs_backtest::{BacktestRunner, FutureQuoteConfig};
use support::configured as shared;

fn tags() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("ema_fast".to_owned(), "12".to_owned()),
        ("window".to_owned(), "is".to_owned()),
    ])
}

fn run_with_tags(tags: BTreeMap<String, String>) -> qs_backtest::StrategyBacktestResult {
    let mut config = shared::runner_config();
    config.close_on_finish = true;
    config.run_tags = tags;
    let mut adapter = shared::adapter();
    let mut feed = shared::feed();
    let evaluation = EvaluationOptions {
        breakdowns: vec![
            BreakdownDimension::Tag("ema_fast".to_owned()),
            BreakdownDimension::Tag("window".to_owned()),
        ],
        minimum_breakdown_bucket_count: 1,
        include_position_rows: true,
        ..EvaluationOptions::default()
    };
    BacktestRunner::new_future(config, FutureQuoteConfig::default())
        .with_evaluation_options(evaluation)
        .run_configured_strategy_future(
            &mut feed,
            &mut adapter,
            shared::analysis(),
            Default::default(),
            None,
        )
        .expect("the neutral fixture replays")
}

#[test]
fn run_tags_reach_the_artifacts_and_every_position() {
    let result = run_with_tags(tags());
    let metadata = result
        .replay
        .execution_metadata
        .as_ref()
        .expect("a future run records execution metadata");

    assert_eq!(metadata.run_tags, tags());
    // Engine diagnostics stay in their own map, so neither can overwrite the other.
    assert!(metadata.tags.contains_key("termination_reason"));
    assert!(!metadata.tags.contains_key("ema_fast"));

    let evaluation = result
        .replay
        .provider_evaluation
        .as_ref()
        .expect("provider evaluation is produced");
    let rows = evaluation
        .position_rows
        .as_ref()
        .expect("position rows are requested by default");
    assert!(!rows.rows.is_empty());
    for row in &rows.rows {
        assert_eq!(row.dimensions.tags, tags());
    }
}

#[test]
fn run_tags_become_breakdown_dimensions() {
    let result = run_with_tags(tags());
    let evaluation = result.replay.provider_evaluation.unwrap();
    let breakdowns = evaluation.breakdowns.expect("breakdowns were requested");

    let by_fast = breakdowns
        .iter()
        .find(|breakdown| breakdown.dimension == BreakdownDimension::Tag("ema_fast".to_owned()))
        .expect("the requested tag dimension is present");
    assert_eq!(by_fast.buckets.len(), 1);
    assert_eq!(
        by_fast.buckets[0].value,
        BreakdownValue::Text("12".to_owned())
    );
    assert!(by_fast.buckets[0].performance.position_count > 0);
}

#[test]
fn a_run_without_tags_is_unchanged() {
    let tagged = run_with_tags(BTreeMap::new());
    let metadata = tagged.replay.execution_metadata.as_ref().unwrap();
    assert!(metadata.run_tags.is_empty());

    let evaluation = tagged.replay.provider_evaluation.as_ref().unwrap();
    for row in &evaluation.position_rows.as_ref().unwrap().rows {
        assert!(row.dimensions.tags.is_empty());
    }
}

#[test]
fn invalid_run_tags_are_rejected_before_replay_starts() {
    let cases: Vec<BTreeMap<String, String>> = vec![
        BTreeMap::from([("bad key".to_owned(), "1".to_owned())]),
        BTreeMap::from([(String::new(), "1".to_owned())]),
        BTreeMap::from([("k".to_owned(), "a".repeat(65))]),
        BTreeMap::from([("k".to_owned(), "line\nbreak".to_owned())]),
        (0..33)
            .map(|index| (format!("k{index}"), "v".to_owned()))
            .collect(),
    ];

    for tags in cases {
        let mut config = shared::runner_config();
        config.run_tags = tags.clone();
        let mut adapter = shared::adapter();
        let mut feed = shared::feed();
        let error = BacktestRunner::new_future(config, FutureQuoteConfig::default())
            .run_configured_strategy_future(
                &mut feed,
                &mut adapter,
                shared::analysis(),
                Default::default(),
                None,
            )
            .expect_err("invalid run tags must be rejected");
        assert!(
            error.to_string().contains("run tag"),
            "unexpected error for {tags:?}: {error}"
        );
    }
}
