use qs_core::{OrderType, PositionRef, RawSignal, RuleConfigDef, Side};

use super::identity::{CanonicalWriter, IdentityError};
use super::signal::InstrumentHint;
use super::value::ContractBytes;

pub const NORMALIZED_SIGNAL_SEMANTIC_MAX_BYTES: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedSignalSemanticProjection(ContractBytes<NORMALIZED_SIGNAL_SEMANTIC_MAX_BYTES>);

impl NormalizedSignalSemanticProjection {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_inner()
    }
}

pub fn normalized_signal_semantic_projection(
    signal: &RawSignal,
    instrument_hint: Option<&InstrumentHint>,
) -> Result<NormalizedSignalSemanticProjection, IdentityError> {
    let mut writer = CanonicalWriter::new();
    writer.u16(1);
    encode_raw_signal(signal, &mut writer)?;
    encode_instrument_hint(instrument_hint, &mut writer)?;
    Ok(NormalizedSignalSemanticProjection(ContractBytes::try_new(
        writer.into_bytes(),
        "normalized signal semantic projection",
    )?))
}

fn encode_instrument_hint(
    hint: Option<&InstrumentHint>,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    let Some(hint) = hint else {
        writer.bool(false);
        return Ok(());
    };
    writer.bool(true);
    writer.text(hint.symbol().as_str())?;
    encode_option_text(hint.venue_hint(), writer)?;
    encode_option_text(hint.market_kind_hint(), writer)
}

fn encode_raw_signal(
    signal: &RawSignal,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    match signal {
        RawSignal::Entry {
            ts,
            symbol,
            side,
            order_type,
            price,
            risk_multiplier,
            stoploss,
            targets,
            group,
            trade_id,
        } => {
            writer.u16(1);
            encode_timestamp(*ts, writer);
            writer.text(symbol)?;
            writer.u16(side_tag(*side));
            writer.u16(order_type_tag(*order_type));
            encode_option_f64(*price, writer)?;
            writer.finite_f64(*risk_multiplier)?;
            encode_option_f64(*stoploss, writer)?;
            writer.u32(targets.len() as u32);
            for target in targets {
                writer.finite_f64(*target)?;
            }
            encode_option_text(group.as_deref(), writer)?;
            encode_option_text(trade_id.as_deref(), writer)?;
        }
        RawSignal::Close { ts, position } => {
            writer.u16(2);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
        }
        RawSignal::ClosePartial {
            ts,
            position,
            ratio,
        } => {
            writer.u16(3);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
            writer.finite_f64(*ratio)?;
        }
        RawSignal::ModifyStoploss {
            ts,
            position,
            price,
        } => {
            writer.u16(4);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
            writer.finite_f64(*price)?;
        }
        RawSignal::MoveStoplossToEntry { ts, position } => {
            writer.u16(5);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
        }
        RawSignal::AddTarget {
            ts,
            position,
            price,
            close_ratio,
        } => {
            writer.u16(6);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
            writer.finite_f64(*price)?;
            writer.finite_f64(*close_ratio)?;
        }
        RawSignal::RemoveTarget {
            ts,
            position,
            price,
        } => {
            writer.u16(7);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
            writer.finite_f64(*price)?;
        }
        RawSignal::ModifyTarget {
            ts,
            position,
            old_price,
            new_price,
        } => {
            writer.u16(8);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
            writer.finite_f64(*old_price)?;
            writer.finite_f64(*new_price)?;
        }
        RawSignal::AddRule { ts, position, rule } => {
            writer.u16(9);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
            encode_rule(rule, writer)?;
        }
        RawSignal::RemoveRule {
            ts,
            position,
            rule_name,
        } => {
            writer.u16(10);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
            writer.text(rule_name)?;
        }
        RawSignal::ScaleIn {
            ts,
            position,
            price,
            size,
        } => {
            writer.u16(11);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
            encode_option_f64(*price, writer)?;
            writer.finite_f64(*size)?;
        }
        RawSignal::CancelPending { ts, position } => {
            writer.u16(12);
            encode_timestamp(*ts, writer);
            encode_position(position, writer)?;
        }
        RawSignal::CloseAllOf { ts, symbol } => {
            writer.u16(13);
            encode_timestamp(*ts, writer);
            writer.text(symbol)?;
        }
        RawSignal::CloseAll { ts } => {
            writer.u16(14);
            encode_timestamp(*ts, writer);
        }
        RawSignal::CancelAllPending { ts } => {
            writer.u16(15);
            encode_timestamp(*ts, writer);
        }
        RawSignal::ModifyAllStoploss { ts, symbol, price } => {
            writer.u16(16);
            encode_timestamp(*ts, writer);
            writer.text(symbol)?;
            writer.finite_f64(*price)?;
        }
        RawSignal::CloseAllInGroup { ts, group_id } => {
            writer.u16(17);
            encode_timestamp(*ts, writer);
            writer.text(group_id)?;
        }
        RawSignal::ModifyAllStoplossInGroup {
            ts,
            group_id,
            price,
        } => {
            writer.u16(18);
            encode_timestamp(*ts, writer);
            writer.text(group_id)?;
            writer.finite_f64(*price)?;
        }
    }
    Ok(())
}

fn encode_position(
    position: &PositionRef,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    match position {
        PositionRef::ByTradeId { trade_id } => {
            writer.u16(1);
            writer.text(trade_id)?;
        }
        PositionRef::AllOnSymbol { symbol } => {
            writer.u16(2);
            writer.text(symbol)?;
        }
        PositionRef::AllInGroup { group_id } => {
            writer.u16(3);
            writer.text(group_id)?;
        }
    }
    Ok(())
}

fn encode_rule(rule: &RuleConfigDef, writer: &mut CanonicalWriter) -> Result<(), IdentityError> {
    match rule {
        RuleConfigDef::FixedStoploss { price } => {
            writer.u16(1);
            writer.finite_f64(*price)?;
        }
        RuleConfigDef::TrailingStop { distance } => {
            writer.u16(2);
            writer.finite_f64(*distance)?;
        }
        RuleConfigDef::TakeProfit { price, close_ratio } => {
            writer.u16(3);
            writer.finite_f64(*price)?;
            writer.finite_f64(*close_ratio)?;
        }
        RuleConfigDef::BreakevenWhen { trigger_price } => {
            writer.u16(4);
            writer.finite_f64(*trigger_price)?;
        }
        RuleConfigDef::BreakevenWhenOffset {
            trigger_price_offset,
        } => {
            writer.u16(5);
            writer.finite_f64(*trigger_price_offset)?;
        }
        RuleConfigDef::BreakevenAfterTargets { after_n } => {
            writer.u16(6);
            writer.u32(*after_n);
        }
        RuleConfigDef::TimeExit { max_seconds } => {
            writer.u16(7);
            writer.u64(*max_seconds);
        }
    }
    Ok(())
}

fn encode_timestamp(value: chrono::NaiveDateTime, writer: &mut CanonicalWriter) {
    let value = value.and_utc();
    writer.i64(value.timestamp());
    writer.u32(value.timestamp_subsec_nanos());
}

fn encode_option_text(
    value: Option<&str>,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    match value {
        Some(value) => {
            writer.bool(true);
            writer.text(value)?;
        }
        None => writer.bool(false),
    }
    Ok(())
}

fn encode_option_f64(
    value: Option<f64>,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    match value {
        Some(value) => {
            writer.bool(true);
            writer.finite_f64(value)?;
        }
        None => writer.bool(false),
    }
    Ok(())
}

fn side_tag(value: Side) -> u16 {
    match value {
        Side::Buy => 1,
        Side::Sell => 2,
    }
}

fn order_type_tag(value: OrderType) -> u16 {
    match value {
        OrderType::Market => 1,
        OrderType::Limit => 2,
        OrderType::Stop => 3,
    }
}
