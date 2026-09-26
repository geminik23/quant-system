#![allow(dead_code)]

use std::collections::{BTreeSet, VecDeque};

use chrono::{NaiveDate, NaiveDateTime};
use qs_core::{
    ManagementProfile, OrderType, PositionRef, PriceQuote, RawSignal, RuleConfigDef, Side,
    StoplossMode, TargetSelection, TargetSource,
};
use qs_execution::{
    ExecutionCapabilities, ExecutionPort, ExecutionReport, ExecutionRequest, PortFuture,
    PositionLookup, PositionSnapshot, PositionSnapshotStatus, ReportStreamError, StateResolver,
    Submission, SubmissionError,
};
use qs_instruments::{
    AssetId, Decimal, DecimalGrid, EconomicsModelId, EffectiveInterval, InstrumentAssets,
    InstrumentEconomics, InstrumentId, InstrumentSpec, ListingStatus, MarketKind, PositiveDecimal,
    PriceRules, QuantityRules, QuantityUnit,
};

pub fn ts(seconds: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(10, 0, seconds)
        .unwrap()
}

fn positive(value: &str) -> PositiveDecimal {
    value.parse().unwrap()
}

pub fn instrument_spec() -> InstrumentSpec {
    let usd = AssetId::new("USD").unwrap();
    InstrumentSpec {
        revision: "1.0.0".parse().unwrap(),
        instrument: InstrumentId::new(
            "broker-a".parse().unwrap(),
            MarketKind::new(MarketKind::FX_CFD).unwrap(),
            "EURUSD".parse().unwrap(),
        ),
        effective: EffectiveInterval::new("2026-01-01T00:00:00Z".parse().unwrap(), None).unwrap(),
        status: ListingStatus::Trading,
        assets: InstrumentAssets {
            base: Some("EUR".parse().unwrap()),
            quote: Some(usd.clone()),
            settlement: usd.clone(),
            fee_assets: BTreeSet::new(),
        },
        price: PriceRules {
            grid: DecimalGrid::new(Decimal::ZERO, positive("0.00001")),
            display_scale: 5,
        },
        quantity: QuantityRules {
            grid: DecimalGrid::new(Decimal::ZERO, positive("0.01")),
            minimum: positive("0.01"),
            maximum: Some(positive("100")),
            storage_scale: 2,
        },
        notional: None,
        economics: InstrumentEconomics {
            pnl_model: EconomicsModelId::new(EconomicsModelId::FX_QUOTE_LINEAR_V1).unwrap(),
            quantity_unit: QuantityUnit::StandardLot,
            contract_multiplier: positive("100000"),
            settlement_asset: usd,
            fee_model: None,
            funding_model: None,
            margin_model: None,
        },
        aliases: BTreeSet::from(["EURUSD".parse().unwrap()]),
    }
}

pub fn quote() -> PriceQuote {
    PriceQuote {
        symbol: "EURUSD".into(),
        ts: ts(0),
        bid: 1.10000,
        ask: 1.10020,
    }
}

pub fn entry_signal(order_type: OrderType, price: Option<f64>) -> RawSignal {
    RawSignal::Entry {
        ts: ts(0),
        symbol: "EURUSD".into(),
        side: Side::Buy,
        order_type,
        price,
        risk_multiplier: 1.0,
        stoploss: Some(1.09500),
        targets: vec![1.10500, 1.11000],
        group: Some("neutral".into()),
        trade_id: Some("trade-1".into()),
        entry_class: None,
    }
}

pub fn position_ref() -> PositionRef {
    PositionRef::ByTradeId {
        trade_id: "trade-1".into(),
    }
}

pub fn open_snapshot(total_steps: u64, remaining_steps: u64) -> PositionSnapshot {
    let spec = instrument_spec();
    PositionSnapshot {
        position_id: "provider-position-1".into(),
        order_id: None,
        instrument: spec.instrument,
        symbol: "EURUSD".into(),
        side: Side::Buy,
        status: PositionSnapshotStatus::Open,
        total_entered_steps: total_steps,
        remaining_steps,
        quantity_step: 0.01,
        quantity_unit: QuantityUnit::StandardLot,
        average_entry_price: Some(1.10020),
        price_grid: spec.price.grid,
        trade_id: Some("trade-1".into()),
        group_id: Some("neutral".into()),
    }
}

pub fn pending_snapshot() -> PositionSnapshot {
    let mut snapshot = open_snapshot(0, 0);
    snapshot.status = PositionSnapshotStatus::Pending;
    snapshot.order_id = Some("provider-order-1".into());
    snapshot.average_entry_price = None;
    snapshot
}

#[derive(Clone)]
pub struct StaticState(pub PositionLookup);

impl StateResolver for StaticState {
    fn resolve(&self, _: &PositionRef) -> PositionLookup {
        self.0.clone()
    }
}

pub fn capabilities() -> ExecutionCapabilities {
    ExecutionCapabilities::all_with_external_management()
}

pub fn trailing_profile() -> ManagementProfile {
    ManagementProfile {
        name: "trailing".into(),
        target_selection: Some(TargetSelection::All),
        use_targets: Vec::new(),
        close_ratios: Vec::new(),
        target_source: TargetSource::FromSignal,
        stoploss_mode: StoplossMode::FromSignal,
        rules: vec![RuleConfigDef::TrailingStop { distance: 0.001 }],
        group_override: None,
        let_remainder_run: false,
        entry_geometry: qs_core::profile::EntryGeometryPolicy::Strict,
    }
}

pub struct RecordingPort {
    pub capabilities: ExecutionCapabilities,
    pub requests: Vec<ExecutionRequest>,
    pub reports: VecDeque<ExecutionReport>,
    pub submit_error: Option<SubmissionError>,
}

impl RecordingPort {
    pub fn new(capabilities: ExecutionCapabilities) -> Self {
        Self {
            capabilities,
            requests: Vec::new(),
            reports: VecDeque::new(),
            submit_error: None,
        }
    }
}

impl ExecutionPort for RecordingPort {
    fn capabilities(&self) -> ExecutionCapabilities {
        self.capabilities
    }

    fn submit<'a>(
        &'a mut self,
        request: ExecutionRequest,
    ) -> PortFuture<'a, Result<Submission, SubmissionError>> {
        Box::pin(async move {
            request
                .validate()
                .map_err(|error| SubmissionError::DefinitelyNotSubmitted {
                    reason: error.to_string(),
                })?;
            if let Some(error) = self.submit_error.take() {
                return Err(error);
            }
            let request_id = request.identity.request_id.clone();
            self.requests.push(request);
            Ok(Submission {
                request_id,
                provider_reference: Some("recorded".into()),
            })
        })
    }

    fn next_report<'a>(
        &'a mut self,
    ) -> PortFuture<'a, Result<Option<ExecutionReport>, ReportStreamError>> {
        Box::pin(async move { Ok(self.reports.pop_front()) })
    }
}
