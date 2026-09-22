mod support;

use std::collections::BTreeMap;

use qs_backtest::Timeframe;
use qs_research::families::EmaCrossFamily;
use qs_research::{DataWindow, ResearchPlan, RunStatus, SymbolEvents, WindowPlan, run_batch};
use support::{SYMBOL, at, config, synthetic_ticks};

fn family() -> EmaCrossFamily {
    EmaCrossFamily::new(3..=4, 8..=9, vec![10, 20], Timeframe::minutes(1).unwrap())
        .with_atr_period(5)
}

fn windows() -> WindowPlan {
    WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(600)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(600), at(900)).unwrap(),
    }
}

fn events() -> BTreeMap<String, SymbolEvents> {
    BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(960))])
}

fn plan(workers: usize) -> ResearchPlan {
    ResearchPlan::new(vec![SYMBOL.to_owned()], windows(), config()).with_workers(workers)
}

#[test]
fn a_batch_evaluates_every_point_over_every_window() {
    let table = run_batch(&plan(1), &family(), &events()).unwrap();

    // Every point is evaluated once per window, and both windows of the pair are run.
    let points = qs_research::StrategyFamily::points(&family()).len();
    assert_eq!(points, 8);
    assert_eq!(table.len(), points * 2);
    for row in table.rows() {
        assert_eq!(row.points_total, points);
        assert_eq!(row.family_id, "ema_cross");
        assert_eq!(row.symbol, SYMBOL);
        assert_eq!(row.data_mode, "ticks");
        assert!(
            row.status.is_completed(),
            "run failed: {:?} for {:?}",
            row.status,
            row.params
        );
    }
}

#[test]
fn a_batch_produces_the_same_table_sequentially_and_in_parallel() {
    let events = events();
    let sequential = run_batch(&plan(1), &family(), &events).unwrap();
    let parallel = run_batch(&plan(4), &family(), &events).unwrap();
    assert_eq!(sequential, parallel);
    assert_eq!(sequential.to_csv(), parallel.to_csv());
}

#[test]
fn a_run_never_opens_a_position_before_its_window() {
    let table = run_batch(&plan(1), &family(), &events()).unwrap();
    for row in table.rows() {
        assert_eq!(
            row.entries_before_window, 0,
            "warmup should make an entry before the window impossible, {:?}",
            row.params
        );
    }
}

#[test]
fn a_missing_symbol_fails_its_rows_without_aborting_the_batch() {
    let plan = ResearchPlan::new(
        vec![SYMBOL.to_owned(), "GBPUSD".to_owned()],
        windows(),
        config(),
    );
    let table = run_batch(&plan, &family(), &events()).unwrap();

    let completed = table
        .rows()
        .iter()
        .filter(|row| row.status.is_completed())
        .count();
    let failed: Vec<&qs_research::ResearchRow> = table
        .rows()
        .iter()
        .filter(|row| !row.status.is_completed())
        .collect();

    assert!(completed > 0);
    assert_eq!(failed.len(), completed);
    for row in failed {
        assert_eq!(row.symbol, "GBPUSD");
        assert!(matches!(
            &row.status,
            RunStatus::Failed { kind, message }
                if kind == "replay" && message.contains("GBPUSD")
        ));
    }
}

#[test]
fn every_row_carries_its_parameters_and_window() {
    let table = run_batch(&plan(1), &family(), &events()).unwrap();
    for row in table.rows() {
        assert!(row.params.contains_key("ema_fast"));
        assert!(row.params.contains_key("ema_slow"));
        assert!(row.params.contains_key("atr_stop"));
        assert!(row.params.contains_key("entry"));
        assert!(row.window == "is" || row.window == "oos");
    }

    let pairs = table.paired("is", "oos");
    assert_eq!(
        pairs.len(),
        qs_research::StrategyFamily::points(&family()).len()
    );
    for pair in pairs {
        assert_eq!(pair.in_sample.params, pair.out_of_sample.params);
        assert_eq!(pair.in_sample.window, "is");
        assert_eq!(pair.out_of_sample.window, "oos");
    }
}

#[test]
fn a_family_whose_document_does_not_compile_fails_only_its_own_rows() {
    // A zero-length average is rejected by the strategy compiler, which is what a caller's broken
    // generator looks like from the batch's side.
    let broken = EmaCrossFamily::new(0..=0, 8..=8, vec![10], Timeframe::minutes(1).unwrap());
    let table = run_batch(&plan(1), &broken, &events()).unwrap();

    assert!(!table.is_empty());
    for row in table.rows() {
        assert!(matches!(
            &row.status,
            RunStatus::Failed { kind, .. } if kind == "compile"
        ));
        assert_eq!(row.positions, 0);
    }
}

#[test]
fn an_empty_plan_or_family_is_rejected_before_any_run() {
    let empty_symbols = ResearchPlan::new(Vec::new(), windows(), config());
    assert!(run_batch(&empty_symbols, &family(), &events()).is_err());

    let duplicate = ResearchPlan::new(
        vec![SYMBOL.to_owned(), SYMBOL.to_owned()],
        windows(),
        config(),
    );
    assert!(run_batch(&duplicate, &family(), &events()).is_err());

    let no_points = EmaCrossFamily::new(9..=9, 3..=3, vec![10], Timeframe::minutes(1).unwrap());
    assert!(run_batch(&plan(1), &no_points, &events()).is_err());
}

#[test]
fn a_walk_forward_plan_runs_every_split() {
    let plan = ResearchPlan::new(
        vec![SYMBOL.to_owned()],
        WindowPlan::RollingWalkForward {
            start: at(100),
            end: at(900),
            train: chrono::Duration::minutes(300),
            test: chrono::Duration::minutes(200),
            step: chrono::Duration::minutes(200),
        },
        config(),
    );
    let table = run_batch(&plan, &family(), &events()).unwrap();

    let windows: std::collections::BTreeSet<&str> =
        table.rows().iter().map(|row| row.window.as_str()).collect();
    assert_eq!(
        windows,
        ["is0", "is1", "oos0", "oos1"].into_iter().collect()
    );
    assert_eq!(table.paired("is0", "oos0").len(), 8);
    assert!(table.rows().iter().all(|row| row.status.is_completed()));
}

#[test]
fn a_family_that_labels_two_points_alike_is_rejected() {
    // Rows are ordered by their labels, so indistinguishable points would make the output order
    // depend on which worker finished first.
    struct Ambiguous(EmaCrossFamily);

    impl qs_research::StrategyFamily for Ambiguous {
        type Params = <EmaCrossFamily as qs_research::StrategyFamily>::Params;

        fn family_id(&self) -> &str {
            self.0.family_id()
        }
        fn points(&self) -> Vec<Self::Params> {
            self.0.points()
        }
        fn config(&self, point: &Self::Params) -> qs_strategy::StrategyConfig {
            self.0.config(point)
        }
        fn parameter_binding(&self, _point: &Self::Params) -> qs_strategy::ParameterBinding {
            qs_strategy::ParameterBinding::new([(
                "fixed",
                qs_strategy::ParameterValue::Choice("same".into()),
            )])
        }
        fn geometry(&self, symbol: &str, point: &Self::Params) -> Vec<qs_research::SeriesGeometry> {
            self.0.geometry(symbol, point)
        }
    }

    let error = run_batch(&plan(1), &Ambiguous(family()), &events()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("binds two parameter points identically")
    );
}

#[test]
fn unordered_events_are_rejected_before_any_run() {
    let mut events = events();
    let reversed: Vec<_> = events[SYMBOL].iter().rev().cloned().collect();
    events.insert(SYMBOL.to_owned(), reversed.into());

    let error = run_batch(&plan(1), &family(), &events).unwrap_err();
    assert!(error.to_string().contains("ascending timestamp order"));
}
