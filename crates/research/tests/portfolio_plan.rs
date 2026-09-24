//! A portfolio plan replays every symbol together against one account in each run and reports what its supervisor rejected.

mod support;

use std::collections::BTreeMap;

use qs_backtest::Timeframe;
use qs_backtest::data_feed::{FeedEvent, MarketEvent};
use qs_research::families::EmaCrossFamily;
use qs_research::{
    DataWindow, PortfolioPlan, ResearchError, ResearchPlan, StrategyFamily, SymbolEvents,
    WindowPlan, run_batch, validate_batch,
};
use qs_risk::{CorrelationGroup, RiskPolicy};
use support::{SYMBOL, at, config, symbol_spec, synthetic_ticks};

const SECOND: &str = "GBPUSD";

fn family() -> EmaCrossFamily {
    EmaCrossFamily::new(3..=4, 8..=8, vec![10], Timeframe::minutes(1).unwrap()).with_atr_period(5)
}

fn windows() -> WindowPlan {
    WindowPlan::Fixed {
        in_sample: DataWindow::new("is", at(200), at(600)).unwrap(),
        out_of_sample: DataWindow::new("oos", at(600), at(900)).unwrap(),
    }
}

/// The synthetic series for the first symbol and a copy shifted in price for the second, so both instances want to trade at the same moments.
fn events() -> BTreeMap<String, SymbolEvents> {
    let first = synthetic_ticks(960);
    let second: SymbolEvents = first
        .iter()
        .map(|event| {
            let MarketEvent::Tick { ts, bid, ask, .. } = event.event else {
                unreachable!("synthetic ticks are ticks");
            };
            FeedEvent::new(
                MarketEvent::Tick {
                    symbol: SECOND.into(),
                    ts,
                    bid: bid + 0.2,
                    ask: ask + 0.2,
                },
                event.metadata,
            )
        })
        .collect::<Vec<_>>()
        .into();
    BTreeMap::from([(SYMBOL.to_owned(), first), (SECOND.to_owned(), second)])
}

fn plan(portfolio: PortfolioPlan, workers: usize) -> ResearchPlan {
    let mut config = config();
    let mut spec = symbol_spec();
    spec.canonical = SECOND.to_ascii_lowercase();
    config.symbol_specs.insert(SECOND.to_owned(), spec);
    config.contract_sizes.insert(SECOND.to_owned(), 100_000.0);
    ResearchPlan::new(
        vec![SYMBOL.to_owned(), SECOND.to_owned()],
        windows(),
        config,
    )
    .with_workers(workers)
    .with_portfolio(portfolio)
}

fn one_position() -> PortfolioPlan {
    PortfolioPlan {
        policies: vec![RiskPolicy::MaxOpenPositions { limit: 1 }],
        groups: vec![],
    }
}

#[test]
fn a_portfolio_plan_runs_each_point_once_per_window_over_all_symbols() {
    let plan = plan(one_position(), 1);
    let points = family().points().len();
    assert_eq!(validate_batch(&plan, &family()).unwrap(), points * 2);
    let batch = run_batch(&plan, &family(), &events()).unwrap();
    assert_eq!(batch.len(), points * 2);
    for row in batch.rows() {
        assert!(row.status.is_completed(), "{:?}", row.status);
        assert_eq!(row.symbol, "EURUSD+GBPUSD");
        assert!(row.rejected_entries.is_some());
        assert!(row.halt_minutes.is_some());
    }
    assert!(
        batch
            .rows()
            .iter()
            .any(|row| row.rejected_entries.unwrap() > 0),
        "the one-position limit must reject an entry to be meaningful"
    );
    let header = batch.to_csv().lines().next().unwrap().to_owned();
    assert!(
        header.ends_with(",rejected_entries,halt_minutes"),
        "{header}"
    );

    let instances = batch
        .position_outcomes()
        .iter()
        .filter_map(|position| position.dimensions.tags.get("instance").cloned())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(instances.contains(SYMBOL), "{instances:?}");
    assert!(
        batch
            .position_outcomes()
            .iter()
            .all(|position| position.dimensions.tags["symbol"] == "EURUSD+GBPUSD")
    );
}

#[test]
fn a_portfolio_batch_produces_the_same_table_and_verdicts_sequentially_and_in_parallel() {
    let events = events();
    let sequential = run_batch(&plan(one_position(), 1), &family(), &events).unwrap();
    let parallel = run_batch(&plan(one_position(), 4), &family(), &events).unwrap();
    assert_eq!(sequential, parallel);
    assert_eq!(sequential.to_csv(), parallel.to_csv());
}

#[test]
fn an_unsupervised_portfolio_reports_no_supervisor_columns() {
    let batch = run_batch(&plan(PortfolioPlan::default(), 1), &family(), &events()).unwrap();
    assert!(
        batch
            .rows()
            .iter()
            .all(|row| row.rejected_entries.is_none())
    );
    let header = batch.to_csv().lines().next().unwrap().to_owned();
    assert!(header.ends_with(",points_total"), "{header}");
}

#[test]
fn a_portfolio_plan_reserves_the_instance_tag_and_needs_monetary_sizing_for_a_cap() {
    let mut reserved = plan(one_position(), 1);
    reserved
        .config
        .run_tags
        .insert("instance".into(), "manual".into());
    assert!(matches!(
        run_batch(&reserved, &family(), &events()),
        Err(ResearchError::InvalidPlan(message)) if message.contains("reserved")
    ));

    let capped = plan(
        PortfolioPlan {
            policies: vec![RiskPolicy::GroupRiskCap {
                group: "usd".into(),
                max_group_risk: 100.0,
            }],
            groups: vec![CorrelationGroup {
                id: "usd".into(),
                symbols: [SYMBOL.to_owned(), SECOND.to_owned()].into(),
            }],
        },
        1,
    );
    assert!(matches!(
        validate_batch(&capped, &family()),
        Err(ResearchError::InvalidPlan(message)) if message.contains("monetary sizing")
    ));
}
