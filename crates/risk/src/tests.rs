use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use qs_core::types::Side;

use super::*;

fn at(day: u32, hour: u32, minute: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, day)
        .unwrap()
        .and_hms_opt(hour, minute, 0)
        .unwrap()
}

fn fact(symbol: &str, risk: Option<f64>) -> ExposureFact {
    ExposureFact {
        symbol: symbol.into(),
        side: Side::Buy,
        risk,
    }
}

fn entry(symbol: &str, risk: Option<f64>) -> ExposureIntent<'_> {
    ExposureIntent {
        symbol,
        side: Side::Buy,
        kind: IntentKind::Entry,
        requested_risk: risk,
    }
}

fn facts<'a>(
    now: NaiveDateTime,
    balance: f64,
    open: &'a [ExposureFact],
    pending: &'a [ExposureFact],
    reserved: &'a [ExposureFact],
) -> PortfolioFacts<'a> {
    PortfolioFacts {
        now,
        balance,
        drawdown_fraction: None,
        day_realized_r: 0.0,
        open,
        pending,
        reserved,
    }
}

fn usd_group() -> CorrelationGroup {
    CorrelationGroup {
        id: "usd".into(),
        symbols: ["EURUSD".to_owned(), "GBPUSD".to_owned()].into(),
    }
}

fn rejected_policy(verdict: &Verdict) -> &str {
    match verdict {
        Verdict::Reject { policy, .. } => policy,
        Verdict::Approve => panic!("expected a rejection"),
    }
}

#[test]
fn max_open_positions_counts_open_pending_and_approved_requests() {
    let supervisor =
        PortfolioSupervisor::new(vec![RiskPolicy::MaxOpenPositions { limit: 3 }], vec![]).unwrap();
    let open = [fact("EURUSD", Some(100.0))];
    let pending = [fact("GBPUSD", Some(100.0))];
    let reserved = [fact("USDJPY", Some(100.0))];

    let under = facts(at(5, 10, 0), 10_000.0, &open, &pending, &[]);
    assert!(
        supervisor
            .review(&under, &entry("AUDUSD", Some(100.0)))
            .is_approved()
    );

    let full = facts(at(5, 10, 0), 10_000.0, &open, &pending, &reserved);
    let verdict = supervisor.review(&full, &entry("AUDUSD", Some(100.0)));
    assert_eq!(rejected_policy(&verdict), "max_open_positions");
}

#[test]
fn max_open_per_symbol_only_counts_the_requested_symbol() {
    let supervisor =
        PortfolioSupervisor::new(vec![RiskPolicy::MaxOpenPerSymbol { limit: 1 }], vec![]).unwrap();
    let open = [fact("EURUSD", Some(100.0))];
    let facts = facts(at(5, 10, 0), 10_000.0, &open, &[], &[]);
    assert!(
        supervisor
            .review(&facts, &entry("GBPUSD", Some(100.0)))
            .is_approved()
    );
    let verdict = supervisor.review(&facts, &entry("EURUSD", Some(100.0)));
    assert_eq!(rejected_policy(&verdict), "max_open_per_symbol");
}

#[test]
fn a_group_cap_rejects_the_entry_that_would_exceed_it() {
    let supervisor = PortfolioSupervisor::new(
        vec![RiskPolicy::GroupRiskCap {
            group: "usd".into(),
            max_group_risk: 250.0,
        }],
        vec![usd_group()],
    )
    .unwrap();
    let open = [fact("EURUSD", Some(100.0))];
    let reserved = [fact("GBPUSD", Some(100.0))];

    let one = facts(at(5, 10, 0), 10_000.0, &open, &[], &[]);
    assert!(
        supervisor
            .review(&one, &entry("GBPUSD", Some(100.0)))
            .is_approved()
    );

    // Two entries approved in one boundary: the reservation of the first counts against the second.
    let two = facts(at(5, 10, 0), 10_000.0, &open, &[], &reserved);
    let verdict = supervisor.review(&two, &entry("EURUSD", Some(100.0)));
    assert_eq!(rejected_policy(&verdict), "group_risk_cap");

    // A symbol outside the group is not capped by it.
    assert!(
        supervisor
            .review(&two, &entry("USDJPY", Some(100.0)))
            .is_approved()
    );

    // Exactly reaching the cap is allowed.
    assert!(
        supervisor
            .review(&two, &entry("EURUSD", Some(50.0)))
            .is_approved()
    );
}

#[test]
fn an_unmeasurable_request_or_position_is_rejected_while_a_cap_is_active() {
    let capped = PortfolioSupervisor::new(
        vec![RiskPolicy::GroupRiskCap {
            group: "usd".into(),
            max_group_risk: 250.0,
        }],
        vec![usd_group()],
    )
    .unwrap();
    let empty = facts(at(5, 10, 0), 10_000.0, &[], &[], &[]);
    let Verdict::Reject { reason, .. } = capped.review(&empty, &entry("EURUSD", None)) else {
        panic!("an unmeasurable request must be rejected under a cap");
    };
    assert!(reason.starts_with("risk_unmeasurable"));

    let open = [fact("GBPUSD", None)];
    let unknown = facts(at(5, 10, 0), 10_000.0, &open, &[], &[]);
    let Verdict::Reject { reason, .. } = capped.review(&unknown, &entry("EURUSD", Some(50.0)))
    else {
        panic!("an unknown carried risk must be rejected under a cap");
    };
    assert!(reason.starts_with("group_risk_unmeasurable"));

    let uncapped =
        PortfolioSupervisor::new(vec![RiskPolicy::MaxOpenPositions { limit: 5 }], vec![]).unwrap();
    assert!(
        uncapped
            .review(&empty, &entry("EURUSD", None))
            .is_approved()
    );
}

#[test]
fn a_scale_in_is_not_counted_as_a_new_position_but_is_capped() {
    let supervisor = PortfolioSupervisor::new(
        vec![
            RiskPolicy::MaxOpenPositions { limit: 1 },
            RiskPolicy::GroupRiskCap {
                group: "usd".into(),
                max_group_risk: 250.0,
            },
        ],
        vec![usd_group()],
    )
    .unwrap();
    let open = [fact("EURUSD", Some(100.0))];
    let facts = facts(at(5, 10, 0), 10_000.0, &open, &[], &[]);
    let scale_in = ExposureIntent {
        symbol: "EURUSD",
        side: Side::Buy,
        kind: IntentKind::ScaleIn,
        requested_risk: None,
    };
    let verdict = supervisor.review(&facts, &scale_in);
    assert_eq!(rejected_policy(&verdict), "group_risk_cap");

    let uncapped =
        PortfolioSupervisor::new(vec![RiskPolicy::MaxOpenPositions { limit: 1 }], vec![]).unwrap();
    assert!(uncapped.review(&facts, &scale_in).is_approved());
}

#[test]
fn a_daily_loss_halt_blocks_until_the_next_reset() {
    let reset = NaiveTime::from_hms_opt(22, 0, 0).unwrap();
    let mut supervisor = PortfolioSupervisor::new(
        vec![RiskPolicy::DailyLossHalt {
            max_loss: LossLimit::AccountPercent(2.0),
            reset_at_utc: reset,
        }],
        vec![],
    )
    .unwrap();
    supervisor.begin(at(5, 10, 0), 10_000.0);
    assert_eq!(supervisor.day_start(), Some(at(4, 22, 0)));

    let small = facts(at(5, 11, 0), 9_850.0, &[], &[], &[]);
    assert!(supervisor.on_boundary(&small).is_empty());
    assert!(
        supervisor
            .review(&small, &entry("EURUSD", Some(100.0)))
            .is_approved()
    );

    let breach = facts(at(5, 12, 0), 9_800.0, &[], &[], &[]);
    assert_eq!(
        supervisor.on_boundary(&breach),
        vec![HaltCommand::CancelAllPending]
    );
    let verdict = supervisor.review(&breach, &entry("EURUSD", Some(100.0)));
    assert_eq!(rejected_policy(&verdict), "daily_loss_halt");
    assert!(supervisor.halted());

    let next_day = facts(at(5, 22, 0), 9_800.0, &[], &[], &[]);
    assert!(supervisor.on_boundary(&next_day).is_empty());
    assert!(!supervisor.halted());
    assert!(
        supervisor
            .review(&next_day, &entry("EURUSD", Some(100.0)))
            .is_approved()
    );
    assert_eq!(
        supervisor.intervals(),
        &[HaltInterval {
            policy: "daily_loss_halt".into(),
            from: at(5, 12, 0),
            to: Some(at(5, 22, 0)),
        }]
    );
}

#[test]
fn a_daily_loss_limit_in_r_uses_realized_r_since_the_reset() {
    let mut supervisor = PortfolioSupervisor::new(
        vec![RiskPolicy::DailyLossHalt {
            max_loss: LossLimit::RiskMultiples(3.0),
            reset_at_utc: NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        }],
        vec![],
    )
    .unwrap();
    let mut facts = facts(at(5, 10, 0), 10_000.0, &[], &[], &[]);
    facts.day_realized_r = -2.5;
    assert!(supervisor.on_boundary(&facts).is_empty());
    facts.day_realized_r = -3.0;
    assert_eq!(
        supervisor.on_boundary(&facts),
        vec![HaltCommand::CancelAllPending]
    );
}

#[test]
fn a_kill_switch_trips_once_and_closes_everything_once() {
    let mut supervisor = PortfolioSupervisor::new(
        vec![RiskPolicy::KillSwitch {
            max_drawdown_percent: 10.0,
            action: HaltAction::HaltAndCloseAll,
        }],
        vec![],
    )
    .unwrap();
    let mut facts = facts(at(5, 10, 0), 10_000.0, &[], &[], &[]);
    assert!(
        supervisor.on_boundary(&facts).is_empty(),
        "no mark, no halt"
    );
    facts.drawdown_fraction = Some(0.05);
    assert!(supervisor.on_boundary(&facts).is_empty());
    facts.drawdown_fraction = Some(0.10);
    assert_eq!(
        supervisor.on_boundary(&facts),
        vec![HaltCommand::CancelAllPending, HaltCommand::CloseAll]
    );
    facts.drawdown_fraction = Some(0.0);
    assert!(supervisor.on_boundary(&facts).is_empty());
    let verdict = supervisor.review(&facts, &entry("EURUSD", Some(100.0)));
    assert_eq!(rejected_policy(&verdict), "kill_switch");
    let intervals = supervisor.finish();
    assert_eq!(intervals.len(), 1);
    assert_eq!(
        intervals[0].to, None,
        "a kill switch lasts until the run ends"
    );
}

#[test]
fn invalid_configurations_are_rejected() {
    let undeclared = PortfolioSupervisor::new(
        vec![RiskPolicy::GroupRiskCap {
            group: "none".into(),
            max_group_risk: 100.0,
        }],
        vec![],
    );
    assert!(matches!(
        undeclared,
        Err(RiskConfigError::UnknownGroup { .. })
    ));
    assert!(matches!(
        PortfolioSupervisor::new(vec![RiskPolicy::MaxOpenPositions { limit: 0 }], vec![]),
        Err(RiskConfigError::InvalidValue { .. })
    ));
    assert!(matches!(
        PortfolioSupervisor::new(vec![], vec![usd_group(), usd_group()]),
        Err(RiskConfigError::DuplicateGroup { .. })
    ));
    assert!(matches!(
        PortfolioSupervisor::new(
            vec![RiskPolicy::KillSwitch {
                max_drawdown_percent: 150.0,
                action: HaltAction::Halt,
            }],
            vec![],
        ),
        Err(RiskConfigError::InvalidValue { .. })
    ));
    assert!(matches!(
        PortfolioSupervisor::new(
            vec![
                RiskPolicy::DailyLossHalt {
                    max_loss: LossLimit::Amount(100.0),
                    reset_at_utc: NaiveTime::from_hms_opt(21, 0, 0).unwrap(),
                },
                RiskPolicy::DailyLossHalt {
                    max_loss: LossLimit::Amount(200.0),
                    reset_at_utc: NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
                },
            ],
            vec![],
        ),
        Err(RiskConfigError::InvalidValue { .. })
    ));
}

#[test]
fn policies_decode_strictly_from_their_wire_form() {
    let policies: Vec<RiskPolicy> = serde_json::from_value(serde_json::json!([
        { "type": "max_open_positions", "limit": 3 },
        { "type": "group_risk_cap", "group": "usd", "max_group_risk": 250.0 },
        { "type": "daily_loss_halt", "max_loss": { "account_percent": 2.0 }, "reset_at_utc": "22:00:00" },
        { "type": "kill_switch", "max_drawdown_percent": 15.0, "action": "halt_and_close_all" }
    ]))
    .unwrap();
    assert_eq!(policies.len(), 4);
    assert!(
        serde_json::from_value::<RiskPolicy>(serde_json::json!({
            "type": "max_open_positions", "limit": 3, "extra": true
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<CorrelationGroup>(serde_json::json!({
            "id": "usd", "symbols": ["EURUSD"], "weight": 1
        }))
        .is_err()
    );
}

#[test]
fn kill_switch_tiers_trip_separately_and_the_deeper_one_closes_everything() {
    let mut supervisor = PortfolioSupervisor::new(
        vec![
            RiskPolicy::KillSwitch {
                max_drawdown_percent: 5.0,
                action: HaltAction::Halt,
            },
            RiskPolicy::KillSwitch {
                max_drawdown_percent: 10.0,
                action: HaltAction::HaltAndCloseAll,
            },
        ],
        vec![],
    )
    .unwrap();
    let mut facts = facts(at(5, 10, 0), 10_000.0, &[], &[], &[]);
    facts.drawdown_fraction = Some(0.06);
    assert_eq!(
        supervisor.on_boundary(&facts),
        vec![HaltCommand::CancelAllPending]
    );
    facts.drawdown_fraction = Some(0.11);
    assert_eq!(supervisor.on_boundary(&facts), vec![HaltCommand::CloseAll]);
    facts.drawdown_fraction = Some(0.20);
    assert!(supervisor.on_boundary(&facts).is_empty());
    assert_eq!(supervisor.finish().len(), 2);
}
