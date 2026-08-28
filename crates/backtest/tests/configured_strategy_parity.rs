mod support;

use qs_backtest::{BacktestRunner, FutureQuoteConfig, StrategyRetentionLimits};
use support::configured::{
    analysis, direct_lifecycle_signals, lifecycle_adapter, runner_config, scenario_feed,
};

fn json<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}

fn json_without_termination_reason<T: serde::Serialize>(value: &T) -> serde_json::Value {
    let mut value = json(value);
    value
        .pointer_mut("/tags")
        .and_then(serde_json::Value::as_object_mut)
        .map(|tags| tags.remove("termination_reason"));
    value
}

fn active_mtm_economics<T: serde::Serialize>(value: &T) -> serde_json::Value {
    let mut value = json(value);
    let points = value.as_array_mut().unwrap();
    points.retain(|point| {
        point
            .get("gross_exposure")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|exposure| exposure > 0.0)
    });
    for point in points {
        let fields = point.as_object_mut().unwrap();
        fields.remove("observation_kind");
        fields.remove("observation_sequence");
    }
    value
}

fn json_without_action_ids<T: serde::Serialize>(value: &T) -> serde_json::Value {
    fn remove_action_ids(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(items) => {
                for item in items {
                    remove_action_ids(item);
                }
            }
            serde_json::Value::Object(fields) => {
                fields.remove("action_id");
                for value in fields.values_mut() {
                    remove_action_ids(value);
                }
            }
            _ => {}
        }
    }

    let mut value = json(value);
    remove_action_ids(&mut value);
    value
}

#[test]
fn configured_and_direct_lifecycle_runs_have_full_economic_parity() {
    let mut configured_adapter = lifecycle_adapter();
    let configured_result =
        BacktestRunner::new_future(runner_config(), FutureQuoteConfig::default())
            .run_configured_strategy_future(
                &mut scenario_feed(),
                &mut configured_adapter,
                analysis(),
                StrategyRetentionLimits::default(),
                None,
            )
            .unwrap();

    let direct_future = FutureQuoteConfig {
        signal_latency_ms: 60_000,
        ..FutureQuoteConfig::default()
    };
    let expected_signals = direct_lifecycle_signals();
    let direct = BacktestRunner::new_future(runner_config(), direct_future).run_raw_signals_future(
        &mut scenario_feed(),
        expected_signals.clone(),
        None,
    );

    let generated_signals = configured_result
        .decisions
        .records
        .iter()
        .flat_map(|record| record.emitted_signals().iter().cloned())
        .collect::<Vec<_>>();
    assert_eq!(json(&generated_signals), json(&expected_signals));
    let configured = configured_result.replay;

    let configured_fills = configured
        .recorded_fills
        .iter()
        .map(|fill| {
            (
                fill.signal_ts,
                fill.effective_ts,
                fill.execution_ts,
                fill.quote_ts,
                fill.quote_age_millis,
                fill.size,
                fill.bid,
                fill.ask,
                fill.fill,
            )
        })
        .collect::<Vec<_>>();
    let direct_fills = direct
        .recorded_fills
        .iter()
        .map(|fill| {
            (
                fill.signal_ts,
                fill.effective_ts,
                fill.execution_ts,
                fill.quote_ts,
                fill.quote_age_millis,
                fill.size,
                fill.bid,
                fill.ask,
                fill.fill,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(configured_fills, direct_fills);

    let configured_dispositions = configured
        .action_dispositions
        .iter()
        .map(|item| {
            (
                item.action_kind.as_deref(),
                item.signal_ts,
                item.effective_ts,
                item.status,
                item.reason.as_deref(),
                item.position_ids.as_slice(),
            )
        })
        .collect::<Vec<_>>();
    let direct_dispositions = direct
        .action_dispositions
        .iter()
        .map(|item| {
            (
                item.action_kind.as_deref(),
                item.signal_ts,
                item.effective_ts,
                item.status,
                item.reason.as_deref(),
                item.position_ids.as_slice(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(configured_dispositions, direct_dispositions);

    let configured_closes = configured
        .close_events
        .iter()
        .map(|event| {
            (
                event.position_id.as_str(),
                event.symbol.as_str(),
                event.side,
                event.ts,
                event.size,
                event.price,
                event.entry_price,
                event.pnl,
                event.reason,
                event.remaining_size,
            )
        })
        .collect::<Vec<_>>();
    let direct_closes = direct
        .close_events
        .iter()
        .map(|event| {
            (
                event.position_id.as_str(),
                event.symbol.as_str(),
                event.side,
                event.ts,
                event.size,
                event.price,
                event.entry_price,
                event.pnl,
                event.reason,
                event.remaining_size,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(configured_closes, direct_closes);

    assert_eq!(
        configured.pending_order_lifecycle,
        direct.pending_order_lifecycle
    );
    assert_eq!(
        configured.pending_order_snapshots,
        direct.pending_order_snapshots
    );
    assert_eq!(
        configured.open_position_snapshots,
        direct.open_position_snapshots
    );
    assert_eq!(
        json_without_action_ids(&configured.completed_positions),
        json_without_action_ids(&direct.completed_positions)
    );

    assert_eq!(configured.initial_balance, direct.initial_balance);
    assert_eq!(configured.final_balance, direct.final_balance);
    assert_eq!(configured.total_pnl, direct.total_pnl);
    assert_eq!(configured.total_trades, direct.total_trades);
    assert_eq!(configured.winning_trades, direct.winning_trades);
    assert_eq!(configured.losing_trades, direct.losing_trades);
    assert_eq!(configured.win_rate, direct.win_rate);
    assert_eq!(configured.max_drawdown, direct.max_drawdown);
    assert_eq!(configured.max_drawdown_pct, direct.max_drawdown_pct);
    assert_eq!(configured.profit_factor, direct.profit_factor);
    assert_eq!(configured.equity_curve, direct.equity_curve);
    assert_eq!(json(&configured.trade_log), json(&direct.trade_log));

    assert_eq!(
        configured.future_format_version,
        direct.future_format_version
    );
    assert_eq!(
        json_without_termination_reason(&configured.execution_metadata),
        json_without_termination_reason(&direct.execution_metadata)
    );
    assert_eq!(
        active_mtm_economics(&configured.mtm_equity_curve),
        active_mtm_economics(&direct.mtm_equity_curve)
    );
    assert_eq!(
        configured.mtm_output_summary.policy,
        direct.mtm_output_summary.policy
    );
    assert_eq!(configured.mtm_max_drawdown, direct.mtm_max_drawdown);
    assert_eq!(configured.mtm_max_drawdown_pct, direct.mtm_max_drawdown_pct);
    assert!(
        configured
            .mtm_equity_curve
            .iter()
            .any(|point| { point.gross_exposure.is_some() && point.open_risk.is_some() })
    );

    assert_eq!(configured.total_positions, direct.total_positions);
    assert_eq!(configured.winning_positions, direct.winning_positions);
    assert_eq!(configured.losing_positions, direct.losing_positions);
    assert_eq!(configured.position_win_rate, direct.position_win_rate);

    assert_eq!(json(&configured.summary), json(&direct.summary));
    assert_eq!(json(&configured.per_symbol), json(&direct.per_symbol));
    assert_eq!(json(&configured.per_group), json(&direct.per_group));
    assert_eq!(json(&configured.long_stats), json(&direct.long_stats));
    assert_eq!(json(&configured.short_stats), json(&direct.short_stats));
    assert_eq!(
        json(&configured.per_close_reason),
        json(&direct.per_close_reason)
    );
    assert_eq!(json(&configured.streaks), json(&direct.streaks));
    assert_eq!(json(&configured.risk_metrics), json(&direct.risk_metrics));
    assert_eq!(
        json(&configured.duration_stats),
        json(&direct.duration_stats)
    );
    assert_eq!(
        json(&configured.monthly_returns),
        json(&direct.monthly_returns)
    );
    assert_eq!(json(&configured.positions), json(&direct.positions));
    assert_eq!(
        json(&configured.provider_evaluation),
        json(&direct.provider_evaluation)
    );
}
