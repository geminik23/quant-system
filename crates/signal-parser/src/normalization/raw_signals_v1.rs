use serde::Deserialize;

use qs_core::{OrderType, Side};

use crate::ingestion::{
    DateTimeUtc, PayloadEncoding, PayloadSchemaRef, SourceEvent, StructuredPayload,
};

use super::component::{DraftBatch, FinalizedBatch, SignalDecoder, SignalFinalizer};
use super::context::BaseContextSnapshot;
use super::diagnostic::{ComponentReport, ComponentResult, DiagnosticSet, RejectionReason};
use super::signal::{PositionDraftRef, RuleDraft, SignalDraft, SignalDraftAction, finalize_draft};
use super::value::{
    ContractList, ContractValueError, FiniteF64, GroupText, PositiveFiniteF64, RuleNameText,
    SymbolText, TradeKeyText,
};

pub const RAW_SIGNALS_V1_SCHEMA: &str = "quant-system/raw-signals@1";

#[derive(Debug, Clone, Copy, Default)]
pub struct CanonicalRawSignalsDecoder;

impl SignalDecoder for CanonicalRawSignalsDecoder {
    fn decode(
        &self,
        _event: &SourceEvent,
        payload: &StructuredPayload,
        _context: &BaseContextSnapshot,
    ) -> ComponentResult<DraftBatch> {
        if payload.schema().as_str() != RAW_SIGNALS_V1_SCHEMA
            || payload.encoding() != PayloadEncoding::Json
        {
            return Ok(ComponentReport::rejected(reason("unsupported_schema")));
        }
        let mut deserializer = serde_json::Deserializer::from_slice(payload.data().as_slice());
        let envelope = match RawSignalsEnvelopeV1::deserialize(&mut deserializer) {
            Ok(value) => value,
            Err(_) => return Ok(ComponentReport::rejected(reason("invalid_json_schema"))),
        };
        if deserializer.end().is_err() {
            return Ok(ComponentReport::rejected(reason("trailing_json_bytes")));
        }
        if envelope.schema_version != 1 {
            return Ok(ComponentReport::rejected(reason(
                "unsupported_schema_version",
            )));
        }
        if envelope.signals.is_empty() || envelope.signals.len() > 32 {
            return Ok(ComponentReport::rejected(reason("invalid_signal_count")));
        }
        let drafts = match envelope
            .signals
            .into_iter()
            .map(RawSignalDtoV1::into_draft)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(value) => value,
            Err(_) => return Ok(ComponentReport::rejected(reason("invalid_signal_value"))),
        };
        Ok(ComponentReport::accepted(
            ContractList::try_new(drafts, "decoded drafts").expect("batch count was checked"),
        ))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StandardSignalFinalizer;

impl SignalFinalizer for StandardSignalFinalizer {
    fn finalize(
        &self,
        drafts: DraftBatch,
        _event: &SourceEvent,
        _context: &BaseContextSnapshot,
    ) -> ComponentResult<FinalizedBatch> {
        let finalized = drafts
            .into_inner()
            .into_iter()
            .map(finalize_draft)
            .collect();
        Ok(ComponentReport::accepted(
            ContractList::try_new(finalized, "finalized signals")
                .expect("draft batch already satisfies the same ceiling"),
        ))
    }
}

fn reason(value: &str) -> RejectionReason {
    RejectionReason::try_new(value).expect("static rejection code is valid")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSignalsEnvelopeV1 {
    schema_version: u32,
    signals: Vec<RawSignalDtoV1>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
enum RawSignalDtoV1 {
    Entry {
        ts: String,
        symbol: String,
        side: SideDto,
        order_type: OrderTypeDto,
        #[serde(deserialize_with = "deserialize_nullable")]
        price: Option<f64>,
        risk: f64,
        #[serde(deserialize_with = "deserialize_nullable")]
        stoploss: Option<f64>,
        targets: Vec<f64>,
        #[serde(deserialize_with = "deserialize_nullable")]
        group: Option<String>,
        #[serde(deserialize_with = "deserialize_nullable")]
        trade_id: Option<String>,
    },
    Close {
        ts: String,
        position: PositionDtoV1,
    },
    ClosePartial {
        ts: String,
        position: PositionDtoV1,
        ratio: f64,
    },
    ModifyStoploss {
        ts: String,
        position: PositionDtoV1,
        price: f64,
    },
    MoveStoplossToEntry {
        ts: String,
        position: PositionDtoV1,
    },
    AddTarget {
        ts: String,
        position: PositionDtoV1,
        price: f64,
        close_ratio: f64,
    },
    RemoveTarget {
        ts: String,
        position: PositionDtoV1,
        price: f64,
    },
    ModifyTarget {
        ts: String,
        position: PositionDtoV1,
        old_price: f64,
        new_price: f64,
    },
    AddRule {
        ts: String,
        position: PositionDtoV1,
        rule: RuleDtoV1,
    },
    RemoveRule {
        ts: String,
        position: PositionDtoV1,
        rule_name: String,
    },
    ScaleIn {
        ts: String,
        position: PositionDtoV1,
        #[serde(deserialize_with = "deserialize_nullable")]
        price: Option<f64>,
        size: f64,
    },
    CancelPending {
        ts: String,
        position: PositionDtoV1,
    },
    CloseAllOf {
        ts: String,
        symbol: String,
    },
    CloseAll {
        ts: String,
    },
    CancelAllPending {
        ts: String,
    },
    ModifyAllStoploss {
        ts: String,
        symbol: String,
        price: f64,
    },
    CloseAllInGroup {
        ts: String,
        group_id: String,
    },
    ModifyAllStoplossInGroup {
        ts: String,
        group_id: String,
        price: f64,
    },
}

impl RawSignalDtoV1 {
    fn into_draft(self) -> Result<SignalDraft, RawSignalsV1Error> {
        let (ts, action) = match self {
            Self::Entry {
                ts,
                symbol,
                side,
                order_type,
                price,
                risk,
                stoploss,
                targets,
                group,
                trade_id,
            } => {
                let targets = targets
                    .into_iter()
                    .map(|value| finite(value, "target"))
                    .collect::<Result<Vec<_>, _>>()?;
                (
                    timestamp(&ts)?,
                    SignalDraftAction::Entry {
                        symbol: symbol_text(symbol)?,
                        side: side.into(),
                        order_type: order_type.into(),
                        price: price.map(|value| finite(value, "price")).transpose()?,
                        risk: PositiveFiniteF64::try_new(risk, "risk")?,
                        stoploss: stoploss
                            .map(|value| finite(value, "stoploss"))
                            .transpose()?,
                        targets: ContractList::try_new(targets, "targets")?,
                        group: group.map(group_text).transpose()?,
                        trade_id: trade_id.map(trade_key).transpose()?,
                    },
                )
            }
            Self::Close { ts, position } => (
                timestamp(&ts)?,
                SignalDraftAction::Close {
                    position: position.into_draft()?,
                },
            ),
            Self::ClosePartial {
                ts,
                position,
                ratio,
            } => (
                timestamp(&ts)?,
                SignalDraftAction::ClosePartial {
                    position: position.into_draft()?,
                    ratio: finite(ratio, "ratio")?,
                },
            ),
            Self::ModifyStoploss {
                ts,
                position,
                price,
            } => (
                timestamp(&ts)?,
                SignalDraftAction::ModifyStoploss {
                    position: position.into_draft()?,
                    price: finite(price, "price")?,
                },
            ),
            Self::MoveStoplossToEntry { ts, position } => (
                timestamp(&ts)?,
                SignalDraftAction::MoveStoplossToEntry {
                    position: position.into_draft()?,
                },
            ),
            Self::AddTarget {
                ts,
                position,
                price,
                close_ratio,
            } => (
                timestamp(&ts)?,
                SignalDraftAction::AddTarget {
                    position: position.into_draft()?,
                    price: finite(price, "price")?,
                    close_ratio: finite(close_ratio, "close ratio")?,
                },
            ),
            Self::RemoveTarget {
                ts,
                position,
                price,
            } => (
                timestamp(&ts)?,
                SignalDraftAction::RemoveTarget {
                    position: position.into_draft()?,
                    price: finite(price, "price")?,
                },
            ),
            Self::ModifyTarget {
                ts,
                position,
                old_price,
                new_price,
            } => (
                timestamp(&ts)?,
                SignalDraftAction::ModifyTarget {
                    position: position.into_draft()?,
                    old_price: finite(old_price, "old price")?,
                    new_price: finite(new_price, "new price")?,
                },
            ),
            Self::AddRule { ts, position, rule } => (
                timestamp(&ts)?,
                SignalDraftAction::AddRule {
                    position: position.into_draft()?,
                    rule: rule.into_draft()?,
                },
            ),
            Self::RemoveRule {
                ts,
                position,
                rule_name,
            } => (
                timestamp(&ts)?,
                SignalDraftAction::RemoveRule {
                    position: position.into_draft()?,
                    rule_name: RuleNameText::try_new(rule_name, "rule name")?,
                },
            ),
            Self::ScaleIn {
                ts,
                position,
                price,
                size,
            } => (
                timestamp(&ts)?,
                SignalDraftAction::ScaleIn {
                    position: position.into_draft()?,
                    price: price.map(|value| finite(value, "price")).transpose()?,
                    size: finite(size, "size")?,
                },
            ),
            Self::CancelPending { ts, position } => (
                timestamp(&ts)?,
                SignalDraftAction::CancelPending {
                    position: position.into_draft()?,
                },
            ),
            Self::CloseAllOf { ts, symbol } => (
                timestamp(&ts)?,
                SignalDraftAction::CloseAllOf {
                    symbol: symbol_text(symbol)?,
                },
            ),
            Self::CloseAll { ts } => (timestamp(&ts)?, SignalDraftAction::CloseAll),
            Self::CancelAllPending { ts } => (timestamp(&ts)?, SignalDraftAction::CancelAllPending),
            Self::ModifyAllStoploss { ts, symbol, price } => (
                timestamp(&ts)?,
                SignalDraftAction::ModifyAllStoploss {
                    symbol: symbol_text(symbol)?,
                    price: finite(price, "price")?,
                },
            ),
            Self::CloseAllInGroup { ts, group_id } => (
                timestamp(&ts)?,
                SignalDraftAction::CloseAllInGroup {
                    group_id: group_text(group_id)?,
                },
            ),
            Self::ModifyAllStoplossInGroup {
                ts,
                group_id,
                price,
            } => (
                timestamp(&ts)?,
                SignalDraftAction::ModifyAllStoplossInGroup {
                    group_id: group_text(group_id)?,
                    price: finite(price, "price")?,
                },
            ),
        };
        SignalDraft::try_new(ts, None, action, DiagnosticSet::empty(), vec![])
            .map_err(RawSignalsV1Error::Signal)
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum PositionDtoV1 {
    ByTradeId { trade_id: String },
    AllOnSymbol { symbol: String },
    AllInGroup { group_id: String },
}

impl PositionDtoV1 {
    fn into_draft(self) -> Result<PositionDraftRef, RawSignalsV1Error> {
        Ok(match self {
            Self::ByTradeId { trade_id } => PositionDraftRef::ByTradeId {
                trade_id: trade_key(trade_id)?,
            },
            Self::AllOnSymbol { symbol } => PositionDraftRef::AllOnSymbol {
                symbol: symbol_text(symbol)?,
            },
            Self::AllInGroup { group_id } => PositionDraftRef::AllInGroup {
                group_id: group_text(group_id)?,
            },
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum RuleDtoV1 {
    FixedStoploss { price: f64 },
    TrailingStop { distance: f64 },
    TakeProfit { price: f64, close_ratio: f64 },
    BreakevenWhen { trigger_price: f64 },
    BreakevenWhenOffset { trigger_price_offset: f64 },
    BreakevenAfterTargets { after_n: u32 },
    TimeExit { max_seconds: u64 },
}

impl RuleDtoV1 {
    fn into_draft(self) -> Result<RuleDraft, RawSignalsV1Error> {
        Ok(match self {
            Self::FixedStoploss { price } => RuleDraft::FixedStoploss {
                price: finite(price, "rule price")?,
            },
            Self::TrailingStop { distance } => RuleDraft::TrailingStop {
                distance: finite(distance, "rule distance")?,
            },
            Self::TakeProfit { price, close_ratio } => RuleDraft::TakeProfit {
                price: finite(price, "rule price")?,
                close_ratio: finite(close_ratio, "rule close ratio")?,
            },
            Self::BreakevenWhen { trigger_price } => RuleDraft::BreakevenWhen {
                trigger_price: finite(trigger_price, "trigger price")?,
            },
            Self::BreakevenWhenOffset {
                trigger_price_offset,
            } => RuleDraft::BreakevenWhenOffset {
                trigger_price_offset: finite(trigger_price_offset, "trigger offset")?,
            },
            Self::BreakevenAfterTargets { after_n } => RuleDraft::BreakevenAfterTargets { after_n },
            Self::TimeExit { max_seconds } => RuleDraft::TimeExit { max_seconds },
        })
    }
}

fn deserialize_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

#[derive(Debug, Deserialize)]
enum SideDto {
    Buy,
    Sell,
}

impl From<SideDto> for Side {
    fn from(value: SideDto) -> Self {
        match value {
            SideDto::Buy => Self::Buy,
            SideDto::Sell => Self::Sell,
        }
    }
}

#[derive(Debug, Deserialize)]
enum OrderTypeDto {
    Market,
    Limit,
    Stop,
}

impl From<OrderTypeDto> for OrderType {
    fn from(value: OrderTypeDto) -> Self {
        match value {
            OrderTypeDto::Market => Self::Market,
            OrderTypeDto::Limit => Self::Limit,
            OrderTypeDto::Stop => Self::Stop,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum RawSignalsV1Error {
    #[error(transparent)]
    Value(#[from] ContractValueError),
    #[error(transparent)]
    Source(#[from] crate::ingestion::SourceValidationError),
    #[error(transparent)]
    Signal(#[from] super::signal::SignalContractError),
}

fn timestamp(value: &str) -> Result<DateTimeUtc, RawSignalsV1Error> {
    Ok(DateTimeUtc::parse(value)?)
}

fn finite(value: f64, field: &'static str) -> Result<FiniteF64, RawSignalsV1Error> {
    Ok(FiniteF64::try_new(value, field)?)
}

fn symbol_text(value: String) -> Result<SymbolText, RawSignalsV1Error> {
    Ok(SymbolText::try_new(value, "symbol")?)
}

fn group_text(value: String) -> Result<GroupText, RawSignalsV1Error> {
    Ok(GroupText::try_new(value, "group")?)
}

fn trade_key(value: String) -> Result<TradeKeyText, RawSignalsV1Error> {
    Ok(TradeKeyText::try_new(value, "trade key")?)
}

pub fn raw_signals_v1_schema() -> PayloadSchemaRef {
    PayloadSchemaRef::new(RAW_SIGNALS_V1_SCHEMA).expect("built-in schema is valid")
}
