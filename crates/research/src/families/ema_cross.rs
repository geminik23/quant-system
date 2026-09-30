use std::ops::RangeInclusive;

use qs_backtest::{PriceBasis, Timeframe};
use qs_core::{OrderType, Side};
use qs_strategy::{
    ActionTemplate, BarField, ConfiguredActionKind, DecisionKind, DecisionTemplate, Expr, Literal,
    MATERIAL_ATR, MATERIAL_BAR_FIELD, MATERIAL_CROSS_ABOVE, MATERIAL_CROSS_BELOW, MATERIAL_EMA,
    MATERIAL_ENTRY_REJECTED, MATERIAL_POSITION_OPEN, MATERIAL_RSI, MaterialConfig, MaterialParams,
    ParameterBinding, ParameterValue, ScalarType, SourceId, StateConfig, StrategyConfig,
    TransitionConfig,
};

use crate::family::StrategyFamily;
use crate::geometry::SeriesGeometry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EmaEntryCondition {
    Cross,
    PriceAboveFast,
    RsiAboveFifty,
}

impl EmaEntryCondition {
    pub fn label(self) -> &'static str {
        match self {
            Self::Cross => "cross",
            Self::PriceAboveFast => "price_above_fast",
            Self::RsiAboveFifty => "rsi_above_fifty",
        }
    }
}

/// One configuration of the EMA research family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EmaCrossPoint {
    pub fast: u16,
    pub slow: u16,
    pub atr_stop_tenths: u16,
    pub entry: EmaEntryCondition,
}

impl EmaCrossPoint {
    pub fn atr_multiple(self) -> f64 {
        f64::from(self.atr_stop_tenths) / 10.0
    }
}

/// A neutral long-only EMA crossover family, used as the crate's public example and acceptance fixture.
///
/// It enters long when the fast average crosses above the slow one, places a protective stop an ATR multiple below the crossing bar's close, and exits on the opposite cross. It is deliberately ordinary: its purpose is to exercise the search loop over a parameter space that has a real shape, not to be a strategy worth trading.
#[derive(Debug, Clone)]
pub struct EmaCrossFamily {
    fast: RangeInclusive<u16>,
    slow: RangeInclusive<u16>,
    atr_stop_tenths: Vec<u16>,
    timeframe: Timeframe,
    atr_period: u16,
    entry_conditions: Vec<EmaEntryCondition>,
}

impl EmaCrossFamily {
    pub fn new(
        fast: RangeInclusive<u16>,
        slow: RangeInclusive<u16>,
        atr_stop_tenths: Vec<u16>,
        timeframe: Timeframe,
    ) -> Self {
        Self {
            fast,
            slow,
            atr_stop_tenths,
            timeframe,
            atr_period: 14,
            entry_conditions: vec![EmaEntryCondition::Cross],
        }
    }

    pub fn with_atr_period(mut self, period: u16) -> Self {
        self.atr_period = period;
        self
    }

    pub fn with_entry_conditions(mut self, conditions: Vec<EmaEntryCondition>) -> Self {
        self.entry_conditions = conditions;
        self
    }

    fn source() -> SourceId {
        SourceId::new("primary").expect("a constant source identifier is valid")
    }
}

impl StrategyFamily for EmaCrossFamily {
    type Params = EmaCrossPoint;

    fn family_id(&self) -> &str {
        "ema_cross"
    }

    fn points(&self) -> Vec<Self::Params> {
        let mut points = Vec::new();
        for fast in self.fast.clone() {
            for slow in self.slow.clone() {
                // A cross is only meaningful when the averages differ in length.
                if fast >= slow {
                    continue;
                }
                for atr_stop_tenths in &self.atr_stop_tenths {
                    for entry in &self.entry_conditions {
                        points.push(EmaCrossPoint {
                            fast,
                            slow,
                            atr_stop_tenths: *atr_stop_tenths,
                            entry: *entry,
                        });
                    }
                }
            }
        }
        points
    }

    fn config(&self, point: &Self::Params) -> StrategyConfig {
        let source = Self::source();
        let material =
            |id: &str, key: &str, inputs: Vec<Expr>, params: MaterialParams| MaterialConfig {
                id: id.into(),
                key: key.into(),
                inputs,
                params: params.into(),
            };
        let close = || Expr::Material { id: "close".into() };

        let stoploss = Expr::Sub {
            left: Box::new(Expr::Bar {
                source: source.clone(),
                field: BarField::Close,
            }),
            right: Box::new(Expr::Mul {
                left: Box::new(Expr::Material { id: "atr".into() }),
                right: Box::new(Expr::Literal {
                    value: Literal::Number(point.atr_multiple()),
                }),
            }),
        };

        let entry_when = match point.entry {
            EmaEntryCondition::Cross => Expr::Material {
                id: "cross_up".into(),
            },
            EmaEntryCondition::PriceAboveFast => Expr::Gt {
                left: Box::new(close()),
                right: Box::new(Expr::Material { id: "fast".into() }),
            },
            EmaEntryCondition::RsiAboveFifty => Expr::Gt {
                left: Box::new(Expr::Material { id: "rsi".into() }),
                right: Box::new(Expr::Literal {
                    value: Literal::Number(50.0),
                }),
            },
        };
        let mut materials = vec![
            material(
                "close",
                MATERIAL_BAR_FIELD,
                vec![],
                MaterialParams::BarField {
                    source: source.clone(),
                    field: BarField::Close,
                },
            ),
            material(
                "fast",
                MATERIAL_EMA,
                vec![close()],
                MaterialParams::Ema { period: point.fast },
            ),
            material(
                "slow",
                MATERIAL_EMA,
                vec![close()],
                MaterialParams::Ema { period: point.slow },
            ),
            material(
                "atr",
                MATERIAL_ATR,
                vec![],
                MaterialParams::Atr {
                    source: source.clone(),
                    period: self.atr_period,
                },
            ),
            material(
                "cross_up",
                MATERIAL_CROSS_ABOVE,
                vec![
                    Expr::Material { id: "fast".into() },
                    Expr::Material { id: "slow".into() },
                ],
                MaterialParams::None,
            ),
            material(
                "position_open",
                MATERIAL_POSITION_OPEN,
                vec![],
                MaterialParams::Position {
                    slot: "primary".into(),
                },
            ),
            material(
                "entry_rejected",
                MATERIAL_ENTRY_REJECTED,
                vec![],
                MaterialParams::Feedback {
                    slot: "primary".into(),
                    action: ConfiguredActionKind::Entry,
                },
            ),
            material(
                "cross_down",
                MATERIAL_CROSS_BELOW,
                vec![
                    Expr::Material { id: "fast".into() },
                    Expr::Material { id: "slow".into() },
                ],
                MaterialParams::None,
            ),
        ];
        if point.entry == EmaEntryCondition::RsiAboveFifty {
            materials.push(material(
                "rsi",
                MATERIAL_RSI,
                vec![close()],
                MaterialParams::Ema {
                    period: self.atr_period,
                },
            ));
        }

        StrategyConfig {
            strategy_id: format!(
                "ema_cross_{}_{}_{}_{}",
                point.fast,
                point.slow,
                point.atr_stop_tenths,
                point.entry.label()
            ),
            title: "EMA cross".into(),
            parameters: vec![],
            initial_state: "flat".into(),
            sources: vec![source.clone()],
            trade_slots: vec!["primary".into()],
            materials,
            variables: vec![],
            states: vec![
                // The family follows its own commands rather than assuming they took effect. A trade slot is reserved from the moment an entry is issued until a close is committed, so every state below exists to keep the strategy from acting on a slot whose command is still in flight.
                StateConfig {
                    id: "flat".into(),
                    transitions: vec![TransitionConfig {
                        priority: 1,
                        target: "entering".into(),
                        when: entry_when,
                        assignments: vec![],
                        decision: Some(DecisionTemplate {
                            kind: DecisionKind::Entry,
                            reason: "fast average crossed above slow".into(),
                            trade_slot: Some("primary".into()),
                            values: vec![],
                        }),
                        actions: vec![ActionTemplate::Entry {
                            slot: "primary".into(),
                            side: Expr::Literal {
                                value: Literal::Side(Side::Buy),
                            },
                            order_type: OrderType::Market,
                            price: Expr::Literal {
                                value: Literal::Missing(ScalarType::Price),
                            },
                            risk: Expr::Literal {
                                value: Literal::Number(1.0),
                            },
                            stoploss,
                            targets: vec![],
                            entry_class: None,
                        }],
                        notes: vec![],
                    }],
                },
                StateConfig {
                    id: "entering".into(),
                    transitions: vec![
                        TransitionConfig {
                            priority: 1,
                            target: "long".into(),
                            when: Expr::Material {
                                id: "position_open".into(),
                            },
                            assignments: vec![],
                            decision: None,
                            actions: vec![],
                            notes: vec![],
                        },
                        // A rejected entry releases the slot, so the family returns to waiting for the next cross instead of trying to close a position it never opened.
                        TransitionConfig {
                            priority: 2,
                            target: "flat".into(),
                            when: Expr::Material {
                                id: "entry_rejected".into(),
                            },
                            assignments: vec![],
                            decision: None,
                            actions: vec![],
                            notes: vec![],
                        },
                    ],
                },
                StateConfig {
                    id: "long".into(),
                    // The close is issued on the opposite cross whether or not the protective stop already closed the position. If it did, the close resolves as a no-op, and issuing it anyway is what releases the trade slot for the next entry.
                    transitions: vec![TransitionConfig {
                        priority: 1,
                        target: "closing".into(),
                        when: Expr::Material {
                            id: "cross_down".into(),
                        },
                        assignments: vec![],
                        decision: Some(DecisionTemplate {
                            kind: DecisionKind::Exit,
                            reason: "fast average crossed below slow".into(),
                            trade_slot: Some("primary".into()),
                            values: vec![],
                        }),
                        actions: vec![ActionTemplate::Close {
                            slot: "primary".into(),
                        }],
                        notes: vec![],
                    }],
                },
                StateConfig {
                    // The close may be applied or may find nothing left to close after a protective stop fired. Either way the slot is free once the position is no longer open, which is the condition that works for both.
                    id: "closing".into(),
                    transitions: vec![TransitionConfig {
                        priority: 1,
                        target: "flat".into(),
                        when: Expr::Not {
                            value: Box::new(Expr::Material {
                                id: "position_open".into(),
                            }),
                        },
                        assignments: vec![],
                        decision: None,
                        actions: vec![],
                        notes: vec![],
                    }],
                },
            ],
        }
    }

    fn parameter_binding(&self, point: &Self::Params) -> ParameterBinding {
        ParameterBinding::new([
            ("ema_fast", ParameterValue::Integer(i64::from(point.fast))),
            ("ema_slow", ParameterValue::Integer(i64::from(point.slow))),
            ("atr_stop", ParameterValue::Number(point.atr_multiple())),
            ("entry", ParameterValue::Choice(point.entry.label().into())),
        ])
    }

    fn geometry(&self, symbol: &str, _point: &Self::Params) -> Vec<SeriesGeometry> {
        vec![SeriesGeometry::new(
            Self::source(),
            symbol,
            self.timeframe,
            PriceBasis::Mid,
            0,
        )]
    }
}
