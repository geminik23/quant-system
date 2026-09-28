mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::NaiveTime;
use qs_backtest::{PriceBasis, Timeframe};
use qs_market_loader::{QuoteStatisticKind, StoredTick};
use qs_research::*;
use qs_strategy::{
    Expr, ParameterBinding, ScalarType, SourceId, StateConfig, StrategyConfig, TransitionConfig,
    ValueType,
};
use support::{SYMBOL, at, config, synthetic_ticks};

#[derive(Clone)]
struct NamedInputFamily;

impl StrategyFamily for NamedInputFamily {
    type Params = ();

    fn family_id(&self) -> &str {
        "named_input_projectors"
    }

    fn points(&self) -> Vec<Self::Params> {
        vec![()]
    }

    fn parameter_binding(&self, _: &Self::Params) -> ParameterBinding {
        ParameterBinding::new(std::iter::empty::<(String, qs_strategy::ParameterValue)>())
    }

    fn config(&self, _: &Self::Params) -> StrategyConfig {
        let present = |field: &str, scalar| Expr::IsPresent {
            value: Box::new(Expr::Input {
                field: field.into(),
                value_type: ValueType::optional(scalar),
            }),
        };
        StrategyConfig {
            strategy_id: "named_input_projectors".into(),
            title: "Named input projector fixture".into(),
            parameters: vec![],
            initial_state: "idle".into(),
            sources: vec![SourceId::new("primary").unwrap()],
            trade_slots: vec![],
            materials: vec![],
            variables: vec![],
            states: vec![
                StateConfig {
                    id: "idle".into(),
                    transitions: vec![TransitionConfig {
                        priority: 1,
                        target: "active".into(),
                        when: Expr::All {
                            items: vec![
                                present("previous_session_high", ScalarType::Price),
                                present("spread_p90", ScalarType::Number),
                            ],
                        },
                        assignments: vec![],
                        decision: None,
                        actions: vec![],
                        notes: vec![],
                    }],
                },
                StateConfig {
                    id: "active".into(),
                    transitions: vec![],
                },
            ],
        }
    }

    fn geometry(&self, symbol: &str, _: &Self::Params) -> Vec<SeriesGeometry> {
        vec![SeriesGeometry {
            source: SourceId::new("primary").unwrap(),
            symbol: symbol.into(),
            timeframe: Timeframe::minutes(1).unwrap(),
            price_basis: PriceBasis::Bid,
            alignment_offset_seconds: 0,
        }]
    }
}

#[test]
fn calendar_and_quote_projectors_are_fresh_consumed_and_recorded_in_recipes() {
    let events = BTreeMap::from([(SYMBOL.into(), synthetic_ticks(500))]);
    let stored = events[SYMBOL]
        .iter()
        .enumerate()
        .map(|(ordinal, event)| {
            let qs_backtest::data_feed::MarketEvent::Tick { ts, bid, ask, .. } = &event.event
            else {
                unreachable!()
            };
            StoredTick {
                tick: data_preprocess::Tick {
                    exchange: "demo".into(),
                    symbol: SYMBOL.into(),
                    ts: *ts,
                    bid: Some(*bid),
                    ask: Some(*ask),
                    last: None,
                    volume: None,
                    flags: None,
                },
                source_ordinal: ordinal as u64,
                source_identity: None,
                provider_sequence: None,
            }
        })
        .collect::<Vec<_>>();
    let projected = ProjectedStrategyFamily::new(
        NamedInputFamily,
        vec![
            NamedProjectorSelection::Calendar(CalendarProjectorSelection {
                name: "previous_session_high".into(),
                source: "primary".into(),
                timezone: "UTC".into(),
                session_open: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                session_close: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                holidays: BTreeSet::new(),
                early_closes: BTreeMap::new(),
                kind: qs_backtest::CalendarFeatureKind::PreviousSessionHigh,
                opening_range_minutes: 5,
                child_seconds: 60,
                alignment_offset_seconds: 0,
                maximum_history: 16,
            }),
            NamedProjectorSelection::Quote(QuoteProjectorSelection {
                name: "spread_p90".into(),
                source: "primary".into(),
                dataset_reference: "ordered-tick-fixture".into(),
                ticks: Arc::<[StoredTick]>::from(stored),
                kind: QuoteStatisticKind::SpreadP90,
                captured_level: Some(1.1),
                bins: None,
                maximum_rows_per_query: 64,
            }),
        ],
    )
    .unwrap();
    let plan = ResearchPlan::new(
        vec![SYMBOL.into()],
        WindowPlan::Fixed {
            in_sample: DataWindow::new("is", at(120), at(300)).unwrap(),
            out_of_sample: DataWindow::new("oos", at(300), at(480)).unwrap(),
        },
        config(),
    );
    let batch = run_batch(&plan, &projected, &events).unwrap();
    assert_eq!(batch.rows().len(), 2);
    assert!(
        batch.rows().iter().all(|row| row.status.is_completed()),
        "{:?}",
        batch.rows()
    );
    let recipe = &batch.candidate_recipes()[0];
    assert_eq!(recipe.input_projectors.len(), 2);
    assert!(
        recipe
            .input_projectors
            .iter()
            .any(|projector| projector.kind == "calendar")
    );
    assert!(
        recipe
            .input_projectors
            .iter()
            .any(|projector| projector.kind == "quote_statistics")
    );
}
