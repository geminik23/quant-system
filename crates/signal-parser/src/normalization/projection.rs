use qs_core::{OrderType, PositionRef, RawSignal, RuleConfigDef, Side};

use super::identity::{
    CanonicalEncode, CanonicalWriter, IdentityError, PipelineIdentity, hash_domain,
};
use super::report::{EvaluationIdentity, NormalizationEvaluationReport, NormalizationOutcome};
use super::signal::{CorrelationHint, InstrumentHint, NormalizationCandidate};
use super::value::Sha256Digest;

pub const NORMALIZED_SIGNAL_SEMANTIC_DOMAIN: &str = "quant-system/normalized-signal-semantic@1";
pub const EVALUATION_SEMANTIC_DOMAIN: &str = "quant-system/evaluation-semantic-digest@1";
pub const NORMALIZED_SIGNAL_ID_DOMAIN: &str = "quant-system/normalized-signal-id@1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NormalizedSignalSemanticDigest(Sha256Digest);

impl NormalizedSignalSemanticDigest {
    pub fn digest(self) -> Sha256Digest {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EvaluationSemanticDigest(Sha256Digest);

impl EvaluationSemanticDigest {
    pub fn digest(self) -> Sha256Digest {
        self.0
    }
}

pub fn normalized_signal_semantic_digest(
    signal: &RawSignal,
    instrument_hint: Option<&InstrumentHint>,
) -> Result<NormalizedSignalSemanticDigest, IdentityError> {
    let mut writer = CanonicalWriter::new();
    encode_raw_signal(signal, &mut writer)?;
    encode_instrument_hint(instrument_hint, &mut writer)?;
    Ok(NormalizedSignalSemanticDigest(hash_domain(
        NORMALIZED_SIGNAL_SEMANTIC_DOMAIN,
        &writer.into_bytes(),
    )))
}

pub fn normalized_signal_id_digest(
    applied_event_id: &[u8; 32],
    candidate: &NormalizationCandidate,
) -> Result<Sha256Digest, IdentityError> {
    let mut writer = CanonicalWriter::new();
    writer.digest(&Sha256Digest::new(*applied_event_id));
    encode_pipeline_identity(candidate.evidence().pipeline(), &mut writer)?;
    writer.u32(candidate.candidate_ordinal());
    let semantic =
        normalized_signal_semantic_digest(candidate.signal(), candidate.instrument_hint())?;
    writer.u16(1);
    writer.digest(&semantic.digest());
    Ok(hash_domain(
        NORMALIZED_SIGNAL_ID_DOMAIN,
        &writer.into_bytes(),
    ))
}

pub fn evaluation_semantic_digest(
    report: &NormalizationEvaluationReport,
) -> Result<EvaluationSemanticDigest, IdentityError> {
    let mut writer = CanonicalWriter::new();
    encode_evaluation_identity(report.identity(), &mut writer)?;
    match report.outcome() {
        NormalizationOutcome::Accepted { candidates } => {
            writer.u16(1);
            writer.u32(candidates.as_slice().len() as u32);
            for candidate in candidates.as_slice() {
                encode_candidate(candidate, &mut writer)?;
            }
        }
        NormalizationOutcome::Ignored { reason } => {
            writer.u16(2);
            writer.text(reason.as_str())?;
        }
        NormalizationOutcome::Ambiguous { evidence } => {
            writer.u16(3);
            writer.u32(evidence.alternatives().len() as u32);
            for alternative in evidence.alternatives() {
                match alternative.pipeline() {
                    Some(pipeline) => {
                        writer.bool(true);
                        encode_pipeline_identity(pipeline, &mut writer)?;
                    }
                    None => writer.bool(false),
                }
                writer.u32(alternative.alternative_ordinal());
                writer.u32(alternative.value_count());
            }
            writer.u32(evidence.total_alternative_values());
        }
        NormalizationOutcome::Rejected { reason } => {
            writer.u16(4);
            writer.text(reason.as_str())?;
        }
    }
    Ok(EvaluationSemanticDigest(hash_domain(
        EVALUATION_SEMANTIC_DOMAIN,
        &writer.into_bytes(),
    )))
}

pub(crate) fn encode_pipeline_identity(
    pipeline: &PipelineIdentity,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    writer.text(pipeline.id().as_str())?;
    pipeline.version().encode(writer)?;
    writer.digest(&pipeline.graph().digest());
    Ok(())
}

fn encode_evaluation_identity(
    identity: &EvaluationIdentity,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    writer.u16(1);
    writer.digest(&identity.routing_graph().digest());
    match identity.selected_pipeline() {
        Some(pipeline) => {
            writer.bool(true);
            encode_pipeline_identity(pipeline, writer)?;
        }
        None => writer.bool(false),
    }
    Ok(())
}

fn encode_candidate(
    candidate: &NormalizationCandidate,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    writer.u32(candidate.candidate_ordinal());
    let semantic =
        normalized_signal_semantic_digest(candidate.signal(), candidate.instrument_hint())?;
    writer.u16(1);
    writer.digest(&semantic.digest());
    writer.u32(candidate.correlation_hints().len() as u32);
    for hint in candidate.correlation_hints() {
        encode_correlation_hint(hint, writer)?;
    }
    Ok(())
}

fn encode_correlation_hint(
    hint: &CorrelationHint,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    writer.text(hint.key().as_str())?;
    match hint.confidence() {
        Some(confidence) => {
            writer.bool(true);
            writer.finite_f64(confidence.get())?;
        }
        None => writer.bool(false),
    }
    Ok(())
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
