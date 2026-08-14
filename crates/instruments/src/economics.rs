use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    EconomicsImplementationId, EconomicsModelId, InstrumentEconomics, ResolvedInstrumentRef,
};

/// Economic operation that must be supplied by executable code.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EconomicOperation {
    PositionValue,
    RealizedPnl,
    UnrealizedPnl,
    Inventory,
    Fees,
    Funding,
    Margin,
    Liquidation,
}

/// Executable economics capability supplied by a runtime implementation.
pub trait EconomicsCapabilityProvider: Send + Sync {
    fn implementation_id(&self) -> &EconomicsImplementationId;

    fn supports(&self, model: &EconomicsModelId, operation: EconomicOperation) -> bool;
}

/// One operation bound to a concrete implementation and model.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundEconomicCapability {
    pub operation: EconomicOperation,
    pub model: EconomicsModelId,
    pub implementation: EconomicsImplementationId,
}

/// Fail-closed executable capability binding for one resolved instrument.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EconomicsBinding {
    pub instrument: ResolvedInstrumentRef,
    pub capabilities: BTreeMap<EconomicOperation, BoundEconomicCapability>,
}

/// Bind every requested economic operation to a registered implementation.
pub fn bind_economics(
    instrument: &ResolvedInstrumentRef,
    economics: &InstrumentEconomics,
    required: impl IntoIterator<Item = EconomicOperation>,
    providers: &[&dyn EconomicsCapabilityProvider],
) -> Result<EconomicsBinding, EconomicsCapabilityError> {
    let mut capabilities = BTreeMap::new();
    for operation in required {
        let model = model_for_operation(economics, operation)?.clone();
        let mut implementations = providers
            .iter()
            .filter(|provider| provider.supports(&model, operation))
            .map(|provider| provider.implementation_id().clone())
            .collect::<Vec<_>>();
        implementations.sort();
        implementations.dedup();
        let implementation = match implementations.as_slice() {
            [] => {
                return Err(EconomicsCapabilityError::Unsupported {
                    operation,
                    model: model.clone(),
                });
            }
            [implementation] => implementation.clone(),
            _ => {
                return Err(EconomicsCapabilityError::Ambiguous {
                    operation,
                    model: model.clone(),
                    implementations,
                });
            }
        };
        capabilities.insert(
            operation,
            BoundEconomicCapability {
                operation,
                model,
                implementation,
            },
        );
    }
    Ok(EconomicsBinding {
        instrument: instrument.clone(),
        capabilities,
    })
}

fn model_for_operation(
    economics: &InstrumentEconomics,
    operation: EconomicOperation,
) -> Result<&EconomicsModelId, EconomicsCapabilityError> {
    match operation {
        EconomicOperation::PositionValue
        | EconomicOperation::RealizedPnl
        | EconomicOperation::UnrealizedPnl
        | EconomicOperation::Inventory => Ok(&economics.pnl_model),
        EconomicOperation::Fees => economics
            .fee_model
            .as_ref()
            .ok_or(EconomicsCapabilityError::MissingModel { operation }),
        EconomicOperation::Funding => economics
            .funding_model
            .as_ref()
            .ok_or(EconomicsCapabilityError::MissingModel { operation }),
        EconomicOperation::Margin | EconomicOperation::Liquidation => economics
            .margin_model
            .as_ref()
            .ok_or(EconomicsCapabilityError::MissingModel { operation }),
    }
}

/// Failures while binding declarative economics to executable capabilities.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EconomicsCapabilityError {
    #[error("instrument economics do not declare a model for {operation:?}")]
    MissingModel { operation: EconomicOperation },
    #[error("no implementation supports {operation:?} for model {model}")]
    Unsupported {
        operation: EconomicOperation,
        model: EconomicsModelId,
    },
    #[error("multiple implementations support {operation:?} for model {model}")]
    Ambiguous {
        operation: EconomicOperation,
        model: EconomicsModelId,
        implementations: Vec<EconomicsImplementationId>,
    },
}
