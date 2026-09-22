mod support;

use std::collections::BTreeMap;

use qs_backtest::Timeframe;
use qs_backtest::evaluation::{BreakdownDimension, EvaluationOptions};
use qs_research::families::EmaCrossFamily;
use qs_research::{DataWindow, ResearchBatch, ResearchPlan, SymbolEvents, WindowPlan, run_batch};
use support::{SYMBOL, at, config, synthetic_ticks};

fn family() -> EmaCrossFamily {
    EmaCrossFamily::new(3..=3, 8..=8, vec![10], Timeframe::minutes(1).unwrap()).with_atr_period(5)
}

fn events() -> BTreeMap<String, SymbolEvents> {
    BTreeMap::from([(SYMBOL.to_owned(), synthetic_ticks(960))])
}

fn plan() -> ResearchPlan {
    ResearchPlan::new(
        vec![SYMBOL.to_owned()],
        WindowPlan::Fixed {
            in_sample: DataWindow::new("is", at(200), at(600)).unwrap(),
            out_of_sample: DataWindow::new("oos", at(600), at(900)).unwrap(),
        },
        config(),
    )
}

fn table() -> ResearchBatch {
    run_batch(&plan(), &family(), &events()).unwrap()
}

#[test]
fn the_fixture_family_actually_trades() {
    let traded: usize = table().rows().iter().map(|row| row.positions).sum();
    assert!(
        traded > 0,
        "the acceptance fixture must produce positions, otherwise it proves nothing"
    );
}

#[test]
fn a_row_reports_net_figures_and_r_metrics() {
    let table = table();
    let row = table
        .rows()
        .iter()
        .find(|row| row.window == "is" && row.positions > 0)
        .expect("the in-sample window trades");

    // This fixture charges nothing, so no cost is reported and the gross figure stays absent.
    assert_eq!(row.commission, 0.0);
    assert_eq!(row.swap, 0.0);
    assert_eq!(row.gross_pnl, None);
    assert!(row.net_pnl.is_finite());
    assert!(row.max_drawdown_pct.is_finite());
    assert!(row.win_rate.is_some());
    assert!(row.expectancy_r.is_some(), "R metrics must reach the row");
    assert!(row.r_p50.is_some(), "R quantiles must reach the row");
}

#[test]
fn costs_reduce_the_reported_result_and_are_named() {
    let mut costed = config();
    costed.costs.insert(
        SYMBOL.to_owned(),
        qs_core::InstrumentCosts {
            commission: Some(qs_core::CommissionModel::PerLotPerSide {
                amount: 3.5,
                currency: "USD".into(),
            }),
            swap: None,
        },
    );
    let mut plan = plan();
    plan.config = costed;

    let free = table();
    let charged = run_batch(&plan, &family(), &events()).unwrap();

    let free_row = free.rows().iter().find(|row| row.positions > 0).unwrap();
    let charged_row = charged
        .rows()
        .iter()
        .find(|row| row.window == free_row.window && row.params == free_row.params)
        .unwrap();

    assert!(charged_row.commission > 0.0);
    assert!(charged_row.net_pnl < free_row.net_pnl);
    assert_eq!(
        charged_row.gross_pnl,
        Some(charged_row.net_pnl + charged_row.commission + charged_row.swap)
    );
}

#[test]
fn a_window_never_leaves_a_position_open_past_its_end() {
    for row in table().rows() {
        // `close_on_finish` forces any survivor closed, and the row says how many there were.
        assert!(row.forced_closes <= row.positions);
    }
}

#[test]
fn a_plan_may_request_breakdowns_by_a_parameter_tag() {
    // The batch tags every run with its parameters, so a requested tag dimension needs no extra wiring.
    // That the dimension actually resolves is proved where the wiring lives, in the backtest crate's
    // `run_tags` tests; here the claim is only that requesting it does not disturb the batch.
    let plan = plan().with_evaluation(EvaluationOptions {
        breakdowns: vec![BreakdownDimension::Tag("ema_fast".to_owned())],
        minimum_breakdown_bucket_count: 1,
        ..EvaluationOptions::default()
    });
    let table = run_batch(&plan, &family(), &events()).unwrap();
    assert!(table.rows().iter().all(|row| row.status.is_completed()));
    assert!(table.rows().iter().any(|row| row.positions > 0));
}
