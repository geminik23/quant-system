use chrono::{DateTime, Utc};
use qs_core::{
    EntryResolutionContext, ExecutionPricer, ManagementProfile, OrderType, PositionRef,
    PriceGridSource, PriceQuote, RawSignal, RuleConfig, SizingPolicy, allocate_target_steps,
    compute_instrument_size_for_spec_with_prices, resolve_unprofiled_entry,
};
use qs_instruments::{Decimal, InstrumentSpec, ListingStatus};
use qs_risk::IntentKind;
use qs_strategy::{CommandFeedback, CommandTerminalStatus, ConfiguredActionKind};

use crate::types::bounded_reason;
use crate::{
    CompletedIntent, CompletionKind, ConcreteQuantity, EntryRequest, ExecutionCapabilities,
    ExecutionIntent, ExecutionOperation, ExecutionRequest, ExposureRequest, ManagementOwner,
    PositionLookup, PositionSnapshot, PositionSnapshotStatus, PreparationAudit, PreparationError,
    PreparationOutcome, PreparationSizingBasis, PreparedExecution, RequestIdentity, StateResolver,
    TargetInstruction,
};

/// Immutable facts used by pure request preparation.
pub struct PreparationContext<'a> {
    pub as_of: chrono::NaiveDateTime,
    pub quote: Option<&'a PriceQuote>,
    pub instrument: Option<&'a InstrumentSpec>,
    pub sizing_policy: SizingPolicy,
    pub balance_before: f64,
    pub native_to_account_rate: Option<f64>,
    pub profile: Option<&'a ManagementProfile>,
    pub sizing_basis: PreparationSizingBasis,
    pub state: &'a dyn StateResolver,
    pub capabilities: &'a ExecutionCapabilities,
}

/// New convenience sizing policy. Existing callers and defaults are unchanged.
pub const fn default_risk_sizing() -> SizingPolicy {
    SizingPolicy::BalanceRiskPercent { percent: 1.0 }
}

/// Prepare one intent without provider IO or state mutation.
pub fn prepare(
    intent: &ExecutionIntent,
    context: &PreparationContext<'_>,
) -> Result<PreparationOutcome, PreparationError> {
    if context.as_of < intent.decision_time {
        return Err(PreparationError::PreparationBeforeDecision {
            prepared_at: context.as_of,
            decision_time: intent.decision_time,
        });
    }
    match &intent.signal {
        RawSignal::Entry { .. } => prepare_entry(intent, context),
        RawSignal::Close { position, .. } => prepare_close(intent, context, position, None),
        RawSignal::ClosePartial {
            position, ratio, ..
        } => prepare_close(intent, context, position, Some(*ratio)),
        RawSignal::CancelPending { position, .. } => prepare_cancel(intent, context, position),
        RawSignal::ModifyStoploss {
            position, price, ..
        } => prepare_stop(intent, context, position, Some(*price)),
        RawSignal::MoveStoplossToEntry { position, .. } => {
            prepare_stop(intent, context, position, None)
        }
        RawSignal::AddTarget { .. } => unsupported("add_target"),
        RawSignal::RemoveTarget { .. } => unsupported("remove_target"),
        RawSignal::ModifyTarget { .. } => unsupported("modify_target"),
        RawSignal::AddRule { .. } => unsupported("add_rule"),
        RawSignal::RemoveRule { .. } => unsupported("remove_rule"),
        RawSignal::ScaleIn { .. } => unsupported("scale_in"),
        RawSignal::CloseAllOf { .. } => unsupported("close_all_of"),
        RawSignal::CloseAll { .. } => unsupported("close_all"),
        RawSignal::CancelAllPending { .. } => unsupported("cancel_all_pending"),
        RawSignal::ModifyAllStoploss { .. } => unsupported("modify_all_stoploss"),
        RawSignal::CloseAllInGroup { .. } => unsupported("close_all_in_group"),
        RawSignal::ModifyAllStoplossInGroup { .. } => unsupported("modify_all_stoploss_in_group"),
    }
}

fn unsupported(operation: &'static str) -> Result<PreparationOutcome, PreparationError> {
    Err(PreparationError::UnsupportedOperation { operation })
}

fn prepare_entry(
    intent: &ExecutionIntent,
    context: &PreparationContext<'_>,
) -> Result<PreparationOutcome, PreparationError> {
    let quote = context
        .quote
        .ok_or(PreparationError::MissingQuote { operation: "entry" })?;
    ExecutionPricer::validate_quote(quote)
        .map_err(|error| PreparationError::InvalidQuote(error.to_string()))?;
    if quote.ts > context.as_of {
        return Err(PreparationError::FutureQuote {
            quote_time: quote.ts,
            prepared_at: context.as_of,
        });
    }
    let instrument = context
        .instrument
        .ok_or(PreparationError::MissingInstrument { operation: "entry" })?;
    instrument
        .validate()
        .map_err(|error| PreparationError::Sizing(error.to_string()))?;
    let prepared_at = DateTime::<Utc>::from_naive_utc_and_offset(context.as_of, Utc);
    if !instrument.effective.contains(prepared_at) {
        return Err(PreparationError::InstrumentUnavailable {
            reason: "the specification is not effective at the decision time".into(),
        });
    }
    if instrument.status != ListingStatus::Trading {
        return Err(PreparationError::InstrumentUnavailable {
            reason: format!("listing status is {:?}", instrument.status),
        });
    }

    let (symbol, side, order_type, original_signal_price, risk_multiplier) = match &intent.signal {
        RawSignal::Entry {
            symbol,
            side,
            order_type,
            price,
            risk_multiplier,
            ..
        } => (
            symbol.as_str(),
            *side,
            *order_type,
            *price,
            *risk_multiplier,
        ),
        _ => unreachable!("entry preparation is called only for Entry"),
    };
    if quote.symbol != symbol {
        return Err(PreparationError::SymbolMismatch {
            field: "quote",
            signal: symbol.to_owned(),
            actual: quote.symbol.clone(),
        });
    }
    if instrument.instrument.listing.as_str() != symbol {
        return Err(PreparationError::SymbolMismatch {
            field: "instrument",
            signal: symbol.to_owned(),
            actual: instrument.instrument.listing.as_str().to_owned(),
        });
    }

    let preparation_price = quote.open_price(side);
    let mut resolution_signal = intent.signal.clone();
    if let RawSignal::Entry {
        order_type: OrderType::Market,
        price,
        ..
    } = &mut resolution_signal
        && price.is_none()
    {
        *price = Some(preparation_price);
    }
    let resolution_context = EntryResolutionContext {
        price_grid: instrument.price.grid,
        price_grid_source: PriceGridSource::InstrumentPriceGrid,
    };
    let resolved = match context.profile {
        Some(profile) => profile
            .apply_entry_signal_with_context(&resolution_signal, resolution_context)
            .map_err(|error| PreparationError::Profile(error.to_string()))?,
        None => resolve_unprofiled_entry(&resolution_signal)
            .map_err(|error| PreparationError::Profile(error.to_string()))?,
    }
    .expect("entry resolution returns an entry for an Entry signal");

    ensure_entry_capabilities(context.capabilities, order_type, &resolved.rules, &resolved)?;

    let order_level = match order_type {
        OrderType::Market => None,
        OrderType::Limit | OrderType::Stop => resolved.price,
    };
    let order_reference = match order_type {
        OrderType::Market => preparation_price,
        OrderType::Limit | OrderType::Stop => order_level.ok_or_else(|| {
            PreparationError::Profile(format!("{order_type} entry has no resolved price"))
        })?,
    };
    let sizing_reference_price = match (order_type, context.sizing_basis, original_signal_price) {
        (
            OrderType::Market,
            PreparationSizingBasis::SignalEntryPriceWithQuoteFallback,
            Some(price),
        ) => price,
        (OrderType::Market, _, _) => preparation_price,
        (OrderType::Limit | OrderType::Stop, _, _) => order_reference,
    };
    let sizing = compute_instrument_size_for_spec_with_prices(
        &context.sizing_policy,
        risk_multiplier,
        context.balance_before,
        side,
        sizing_reference_price,
        order_reference,
        resolved.stoploss,
        instrument,
        context.native_to_account_rate,
    )
    .map_err(|error| PreparationError::Sizing(error.to_string()))?;
    let step_amount = sizing.final_lot / sizing.final_lot_steps as f64;
    let quantity = ConcreteQuantity {
        steps: sizing.final_lot_steps,
        amount: sizing.final_lot,
        step_amount,
        unit: instrument.economics.quantity_unit,
    };
    let target_steps = allocate_target_steps(
        sizing.final_lot_steps,
        &resolved.target_resolution.weights,
        resolved.target_resolution.remainder,
    )
    .map_err(|error| PreparationError::Profile(error.to_string()))?;
    let targets = resolved
        .targets
        .iter()
        .zip(target_steps)
        .map(|(target, steps)| TargetInstruction {
            price: target.price,
            close_ratio: target.close_ratio,
            quantity: ConcreteQuantity {
                steps,
                amount: step_amount * steps as f64,
                step_amount,
                unit: instrument.economics.quantity_unit,
            },
        })
        .collect();
    let (provider_rules, external_management) = match context.capabilities.ongoing_management {
        ManagementOwner::Provider => (resolved.rules.clone(), Vec::new()),
        ManagementOwner::ExternalRuntime => (Vec::new(), resolved.rules.clone()),
        ManagementOwner::Unsupported => (Vec::new(), Vec::new()),
    };
    let request = ExecutionRequest {
        identity: RequestIdentity::child(intent, 0)?,
        operation: ExecutionOperation::Entry(EntryRequest {
            instrument: instrument.instrument.clone(),
            symbol: resolved.symbol.clone(),
            side: resolved.side,
            order_type: resolved.order_type,
            level: order_level,
            price_grid: instrument.price.grid,
            quantity,
            initial_stop: resolved.stoploss,
            targets,
            provider_rules,
            group: resolved.group.clone(),
            trade_id: resolved.trade_id.clone(),
        }),
    };
    let audit = PreparationAudit {
        instrument: instrument.instrument.clone(),
        prepared_at: context.as_of,
        price_grid_source: PriceGridSource::InstrumentPriceGrid,
        original_signal_price,
        preparation_price,
        sizing_reference_price,
        sizing_basis: context.sizing_basis,
        protective_stop: resolved.stoploss,
        sizing: sizing.clone(),
        level_resolution: resolved.level_resolution,
    };
    Ok(PreparationOutcome::Prepared(Box::new(PreparedExecution {
        command_id: intent.command_id.clone(),
        action: ConfiguredActionKind::Entry,
        requests: vec![request],
        audit: Some(audit),
        external_management,
        exposure: Some(ExposureRequest {
            symbol: symbol.to_owned(),
            side,
            kind: IntentKind::Entry,
            requested_risk: sizing.requested_account_risk,
        }),
    })))
}

fn ensure_entry_capabilities(
    capabilities: &ExecutionCapabilities,
    order_type: OrderType,
    rules: &[RuleConfig],
    resolved: &qs_core::ResolvedEntry,
) -> Result<(), PreparationError> {
    let supported = match order_type {
        OrderType::Market => capabilities.market_entry,
        OrderType::Limit => capabilities.limit_entry,
        OrderType::Stop => capabilities.stop_entry,
    };
    if !supported {
        return Err(PreparationError::UnsupportedCapability {
            operation: match order_type {
                OrderType::Market => "market_entry",
                OrderType::Limit => "limit_entry",
                OrderType::Stop => "stop_entry",
            },
        });
    }
    if resolved.stoploss.is_some() && !capabilities.initial_stop {
        return Err(PreparationError::UnsupportedCapability {
            operation: "initial_stop",
        });
    }
    if !resolved.targets.is_empty() && !capabilities.initial_targets {
        return Err(PreparationError::UnsupportedCapability {
            operation: "initial_targets",
        });
    }
    if !rules.is_empty() && capabilities.ongoing_management == ManagementOwner::Unsupported {
        return Err(PreparationError::MissingManagementOwner);
    }
    Ok(())
}

fn prepare_close(
    intent: &ExecutionIntent,
    context: &PreparationContext<'_>,
    reference: &PositionRef,
    ratio: Option<f64>,
) -> Result<PreparationOutcome, PreparationError> {
    let action = if ratio.is_some() {
        ConfiguredActionKind::ClosePartial
    } else {
        ConfiguredActionKind::Close
    };
    let operation = if ratio.is_some() {
        "partial_close"
    } else {
        "full_close"
    };
    let snapshot = match resolve_snapshot(context.state, reference)? {
        Some(snapshot) => snapshot,
        None => return Ok(skipped(intent, action, "position is already absent")),
    };
    require_status(&snapshot, PositionSnapshotStatus::Open, operation)?;
    validate_position_quantity(&snapshot)?;
    let steps = match ratio {
        Some(ratio) => {
            if !context.capabilities.partial_close {
                return Err(PreparationError::UnsupportedCapability { operation });
            }
            let requested = ((snapshot.total_entered_steps as f64 * ratio) + 1.0e-12).floor();
            if !requested.is_finite() || requested <= 0.0 || requested > u64::MAX as f64 {
                return Err(PreparationError::ZeroCloseQuantity);
            }
            (requested as u64).min(snapshot.remaining_steps)
        }
        None => {
            if !context.capabilities.full_close {
                return Err(PreparationError::UnsupportedCapability { operation });
            }
            snapshot.remaining_steps
        }
    };
    if steps == 0 {
        return Err(PreparationError::ZeroCloseQuantity);
    }
    let quantity = quantity(&snapshot, steps)?;
    Ok(PreparationOutcome::Prepared(Box::new(PreparedExecution {
        command_id: intent.command_id.clone(),
        action,
        requests: vec![ExecutionRequest {
            identity: RequestIdentity::child(intent, 0)?,
            operation: ExecutionOperation::Close {
                instrument: snapshot.instrument,
                position_id: snapshot.position_id,
                quantity,
                full: ratio.is_none(),
            },
        }],
        audit: None,
        external_management: Vec::new(),
        exposure: None,
    })))
}

fn prepare_cancel(
    intent: &ExecutionIntent,
    context: &PreparationContext<'_>,
    reference: &PositionRef,
) -> Result<PreparationOutcome, PreparationError> {
    let snapshot = match resolve_snapshot(context.state, reference)? {
        Some(snapshot) => snapshot,
        None => {
            return Ok(skipped(
                intent,
                ConfiguredActionKind::CancelPending,
                "pending order is already absent",
            ));
        }
    };
    require_status(&snapshot, PositionSnapshotStatus::Pending, "cancel_pending")?;
    if !context.capabilities.cancel_pending {
        return Err(PreparationError::UnsupportedCapability {
            operation: "cancel_pending",
        });
    }
    let order_id = snapshot.order_id.clone().ok_or_else(|| {
        PreparationError::InvalidPositionSnapshot(
            "pending position must carry a provider order_id".into(),
        )
    })?;
    Ok(PreparationOutcome::Prepared(Box::new(PreparedExecution {
        command_id: intent.command_id.clone(),
        action: ConfiguredActionKind::CancelPending,
        requests: vec![ExecutionRequest {
            identity: RequestIdentity::child(intent, 0)?,
            operation: ExecutionOperation::CancelPending {
                instrument: snapshot.instrument,
                position_id: snapshot.position_id,
                order_id,
            },
        }],
        audit: None,
        external_management: Vec::new(),
        exposure: None,
    })))
}

fn prepare_stop(
    intent: &ExecutionIntent,
    context: &PreparationContext<'_>,
    reference: &PositionRef,
    requested: Option<f64>,
) -> Result<PreparationOutcome, PreparationError> {
    let snapshot =
        resolve_snapshot(context.state, reference)?.ok_or(PreparationError::KnownAbsent {
            operation: "modify_stop",
        })?;
    require_status(&snapshot, PositionSnapshotStatus::Open, "modify_stop")?;
    if !context.capabilities.modify_stop {
        return Err(PreparationError::UnsupportedCapability {
            operation: "modify_stop",
        });
    }
    let (stoploss, action) = match requested {
        Some(price) => (price, ConfiguredActionKind::ModifyStoploss),
        None => (
            snapshot.average_entry_price.ok_or_else(|| {
                PreparationError::InvalidPositionSnapshot(
                    "open position needs average_entry_price for move-to-entry".into(),
                )
            })?,
            ConfiguredActionKind::MoveStoplossToEntry,
        ),
    };
    validate_grid_price("stoploss", stoploss, snapshot.price_grid)?;
    Ok(PreparationOutcome::Prepared(Box::new(PreparedExecution {
        command_id: intent.command_id.clone(),
        action,
        requests: vec![ExecutionRequest {
            identity: RequestIdentity::child(intent, 0)?,
            operation: ExecutionOperation::ModifyStop {
                instrument: snapshot.instrument,
                position_id: snapshot.position_id,
                stoploss,
                price_grid: snapshot.price_grid,
            },
        }],
        audit: None,
        external_management: Vec::new(),
        exposure: None,
    })))
}

fn resolve_snapshot(
    state: &dyn StateResolver,
    reference: &PositionRef,
) -> Result<Option<PositionSnapshot>, PreparationError> {
    match state.resolve(reference) {
        PositionLookup::Found(snapshot) => Ok(Some(*snapshot)),
        PositionLookup::KnownAbsent => Ok(None),
        PositionLookup::Unknown => Err(PreparationError::UnknownReference),
        PositionLookup::Ambiguous { count } => Err(PreparationError::AmbiguousReference { count }),
    }
}

fn require_status(
    snapshot: &PositionSnapshot,
    required: PositionSnapshotStatus,
    operation: &'static str,
) -> Result<(), PreparationError> {
    if snapshot.status != required {
        return Err(PreparationError::InvalidPositionStatus {
            position_id: snapshot.position_id.clone(),
            actual: snapshot.status.as_str(),
            required: required.as_str(),
            operation,
        });
    }
    Ok(())
}

fn validate_position_quantity(snapshot: &PositionSnapshot) -> Result<(), PreparationError> {
    if snapshot.total_entered_steps == 0
        || snapshot.remaining_steps == 0
        || snapshot.remaining_steps > snapshot.total_entered_steps
        || !snapshot.quantity_step.is_finite()
        || snapshot.quantity_step <= 0.0
    {
        return Err(PreparationError::InvalidPositionSnapshot(format!(
            "invalid quantity facts: total_steps={}, remaining_steps={}, step={}",
            snapshot.total_entered_steps, snapshot.remaining_steps, snapshot.quantity_step
        )));
    }
    Ok(())
}

fn quantity(snapshot: &PositionSnapshot, steps: u64) -> Result<ConcreteQuantity, PreparationError> {
    let amount = snapshot.quantity_step * steps as f64;
    if !amount.is_finite() || amount <= 0.0 {
        return Err(PreparationError::InvalidPositionSnapshot(format!(
            "quantity amount is invalid for {steps} steps of {}",
            snapshot.quantity_step
        )));
    }
    Ok(ConcreteQuantity {
        steps,
        amount,
        step_amount: snapshot.quantity_step,
        unit: snapshot.quantity_unit,
    })
}

fn validate_grid_price(
    field: &'static str,
    price: f64,
    grid: qs_instruments::DecimalGrid,
) -> Result<(), PreparationError> {
    if !price.is_finite() || price <= 0.0 {
        return Err(PreparationError::InvalidPrice {
            field,
            value: price,
        });
    }
    let decimal = Decimal::checked_from_f64(price).map_err(|_| PreparationError::InvalidPrice {
        field,
        value: price,
    })?;
    if !grid
        .contains(decimal)
        .map_err(|error| PreparationError::InvalidPositionSnapshot(error.to_string()))?
    {
        return Err(PreparationError::PriceOffGrid {
            field,
            value: price,
        });
    }
    Ok(())
}

fn skipped(
    intent: &ExecutionIntent,
    _action: ConfiguredActionKind,
    reason: &str,
) -> PreparationOutcome {
    PreparationOutcome::Completed(CompletedIntent {
        command_id: intent.command_id.clone(),
        kind: CompletionKind::Skipped,
        feedback: vec![CommandFeedback::Terminal {
            command_id: intent.command_id.clone(),
            status: CommandTerminalStatus::Skipped,
            reason: Some(bounded_reason(reason)),
        }],
    })
}
