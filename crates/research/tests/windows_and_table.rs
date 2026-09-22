mod support;

use std::collections::BTreeMap;

use chrono::Duration;
use qs_backtest::Timeframe;
use qs_research::families::EmaCrossFamily;
use qs_research::{DataWindow, ResearchRow, ResearchTable, RunStatus, StrategyFamily, WindowPlan};
use support::at;

fn row(window: &str, fast: &str, slow: &str) -> ResearchRow {
    ResearchRow {
        family_id: "ema_cross".into(),
        symbol: "EURUSD".into(),
        params: BTreeMap::from([
            ("ema_fast".to_owned(), fast.to_owned()),
            ("ema_slow".to_owned(), slow.to_owned()),
        ]),
        window: window.into(),
        status: RunStatus::Completed,
        data_mode: "ticks".into(),
        positions: 3,
        win_rate: Some(0.5),
        net_pnl: 12.5,
        gross_pnl: None,
        commission: 0.0,
        swap: 0.0,
        expectancy_r: Some(0.2),
        r_p05: Some(-1.0),
        r_p50: Some(0.1),
        r_p95: Some(1.4),
        avg_favorable_r: Some(0.8),
        avg_adverse_r: Some(-0.4),
        max_drawdown_pct: 1.25,
        forced_closes: 1,
        entries_before_window: 0,
        points_total: 2,
    }
}

#[test]
fn a_fixed_plan_produces_one_pair() {
    let plan = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(0), at(100)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(100), at(200)).unwrap(),
    };
    let pairs = plan.pairs().unwrap();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].in_sample.label(), "is");
    assert_eq!(pairs[0].out_of_sample.label(), "oos");
}

#[test]
fn fixed_windows_require_distinct_labels() {
    let plan = WindowPlan::Fixed {
        in_sample: DataWindow::new("same", at(0), at(100)).unwrap(),
        out_of_sample: DataWindow::new("same", at(100), at(200)).unwrap(),
    };
    assert!(plan.pairs().is_err());
}

#[test]
fn an_out_of_sample_window_may_not_start_inside_the_in_sample_one() {
    let plan = WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(0), at(100)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(50), at(200)).unwrap(),
    };
    assert!(plan.pairs().is_err());
}

#[test]
fn walk_forward_rolls_forward_and_only_emits_complete_splits() {
    let plan = WindowPlan::RollingWalkForward {
        start: at(0),
        end: at(500),
        train: Duration::minutes(200),
        test: Duration::minutes(100),
        step: Duration::minutes(100),
    };
    let pairs = plan.pairs().unwrap();

    // Splits start at 0, 100, 200; the one starting at 300 would need data past the end.
    assert_eq!(pairs.len(), 3);
    for (index, pair) in pairs.iter().enumerate() {
        assert_eq!(pair.in_sample.label(), format!("is{index}"));
        assert_eq!(pair.out_of_sample.label(), format!("oos{index}"));
        // Test always follows train immediately, so nothing is evaluated twice or skipped.
        assert_eq!(pair.in_sample.to(), pair.out_of_sample.from());
        assert_eq!(
            pair.out_of_sample.to() - pair.out_of_sample.from(),
            Duration::minutes(100)
        );
    }
    assert_eq!(pairs[0].in_sample.from(), at(0));
    assert_eq!(pairs[2].in_sample.from(), at(200));
}

#[test]
fn a_range_too_short_for_one_split_is_rejected() {
    let plan = WindowPlan::RollingWalkForward {
        start: at(0),
        end: at(100),
        train: Duration::minutes(200),
        test: Duration::minutes(100),
        step: Duration::minutes(100),
    };
    assert!(plan.pairs().is_err());
}

#[test]
fn a_window_must_end_after_it_starts_and_be_labelled() {
    assert!(DataWindow::new("w", at(100), at(100)).is_err());
    assert!(DataWindow::new("  ", at(0), at(100)).is_err());
}

#[test]
fn a_table_orders_by_key_and_never_by_a_metric() {
    let mut high = row("is", "9", "20");
    high.net_pnl = 9_999.0;
    let table = ResearchTable::new(vec![row("oos", "3", "8"), high, row("is", "3", "8")]);

    let order: Vec<(&str, &str)> = table
        .rows()
        .iter()
        .map(|row| (row.params["ema_fast"].as_str(), row.window.as_str()))
        .collect();
    // The best net result sorts last because "9" follows "3", not because of its value.
    assert_eq!(order, vec![("3", "is"), ("3", "oos"), ("9", "is")]);
}

#[test]
fn csv_has_one_column_per_parameter_and_no_ranking_column() {
    let table = ResearchTable::new(vec![row("is", "3", "8"), row("oos", "3", "8")]);
    let csv = table.to_csv();
    let header = csv.lines().next().unwrap();

    assert!(header.starts_with("family_id,symbol,param_ema_fast,param_ema_slow,window,status"));
    for forbidden in ["score", "rank", "rating", "best"] {
        assert!(
            !header.contains(forbidden),
            "the table must not rank configurations, found '{forbidden}'"
        );
    }
    assert_eq!(csv.lines().count(), 3);
    assert!(csv.lines().nth(1).unwrap().contains(",completed,,ticks,"));
}

#[test]
fn a_failed_row_keeps_its_reason_in_the_csv() {
    let mut failed = row("is", "3", "8");
    failed.status = RunStatus::Failed {
        kind: "compile".into(),
        message: "period must be positive, got 0".into(),
    };
    let csv = ResearchTable::new(vec![failed]).to_csv();
    assert!(csv.contains("failed"));
    assert!(csv.contains("compile: period must be positive"));
}

#[test]
fn a_pair_is_dropped_when_only_one_of_its_windows_produced_a_row() {
    let table = ResearchTable::new(vec![row("is", "3", "8"), row("is", "9", "20")]);
    assert!(table.paired("is", "oos").is_empty());
}

#[test]
fn a_family_generates_the_same_points_and_documents_every_time() {
    let family = EmaCrossFamily::new(3..=5, 8..=10, vec![10, 15], Timeframe::minutes(1).unwrap());
    let first = family.points();
    let second = family.points();
    assert_eq!(first, second);
    assert!(!first.is_empty());

    // A cross needs two different lengths, so equal or inverted pairs never appear.
    assert!(first.iter().all(|point| point.fast < point.slow));

    for point in &first {
        assert_eq!(family.config(point), family.config(point));
        assert_eq!(
            family.parameter_binding(point),
            family.parameter_binding(point)
        );
    }

    // Each point yields a distinct document, otherwise the search would evaluate duplicates.
    let mut ids: Vec<String> = first
        .iter()
        .map(|point| family.config(point).strategy_id)
        .collect();
    ids.sort();
    let total = ids.len();
    ids.dedup();
    assert_eq!(ids.len(), total);
}
