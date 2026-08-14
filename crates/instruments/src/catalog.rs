use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    AssetId, AssetSpec, EffectiveInterval, InstrumentAlias, InstrumentId, InstrumentSpec,
    ListingVenueId, MarketKind, SpecValidationError,
};

/// Strict authoring document compiled into an immutable catalog snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogDocument {
    pub schema_version: u32,
    pub version: String,
    pub assets: Vec<AssetSpec>,
    pub instruments: Vec<InstrumentSpec>,
}

/// Human-readable identity of one immutable catalog snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogSnapshotId {
    pub version: String,
}

/// Immutable, indexed catalog revision safe to share across in-flight operations.
#[derive(Clone, Debug)]
pub struct InstrumentCatalogSnapshot {
    id: CatalogSnapshotId,
    assets: BTreeMap<AssetId, AssetSpec>,
    history: BTreeMap<InstrumentId, Vec<InstrumentSpec>>,
    aliases: BTreeMap<InstrumentAlias, BTreeSet<InstrumentId>>,
}

impl InstrumentCatalogSnapshot {
    pub fn compile(mut document: CatalogDocument) -> Result<Self, CatalogCompileError> {
        if document.schema_version != 1 {
            return Err(CatalogCompileError::UnsupportedSchema(
                document.schema_version,
            ));
        }
        if document.version.trim().is_empty()
            || document.version.trim() != document.version
            || document.version.len() > 64
            || !document.version.is_ascii()
        {
            return Err(CatalogCompileError::InvalidVersion);
        }

        document
            .assets
            .sort_by(|left, right| left.asset.cmp(&right.asset));
        document.instruments.sort_by(|left, right| {
            left.instrument
                .cmp(&right.instrument)
                .then(left.effective.valid_from.cmp(&right.effective.valid_from))
                .then(left.revision.cmp(&right.revision))
        });

        let mut assets = BTreeMap::new();
        for asset in &document.assets {
            asset.validate()?;
            if assets.insert(asset.asset.clone(), asset.clone()).is_some() {
                return Err(CatalogCompileError::DuplicateAsset(asset.asset.clone()));
            }
        }

        let mut history: BTreeMap<InstrumentId, Vec<InstrumentSpec>> = BTreeMap::new();
        let mut aliases: BTreeMap<InstrumentAlias, BTreeSet<InstrumentId>> = BTreeMap::new();
        for spec in &document.instruments {
            spec.validate()?;
            validate_assets(spec, &assets)?;
            for alias in &spec.aliases {
                aliases
                    .entry(alias.clone())
                    .or_default()
                    .insert(spec.instrument.clone());
            }
            history
                .entry(spec.instrument.clone())
                .or_default()
                .push(spec.clone());
        }

        for (instrument, specifications) in &mut history {
            specifications.sort_by(|left, right| {
                left.effective
                    .valid_from
                    .cmp(&right.effective.valid_from)
                    .then(left.revision.cmp(&right.revision))
            });
            for pair in specifications.windows(2) {
                if pair[0].effective.overlaps(&pair[1].effective) {
                    return Err(CatalogCompileError::OverlappingInterval {
                        instrument: instrument.clone(),
                        left: pair[0].effective,
                        right: pair[1].effective,
                    });
                }
            }
        }

        Ok(Self {
            id: CatalogSnapshotId {
                version: document.version,
            },
            assets,
            history,
            aliases,
        })
    }

    pub fn id(&self) -> &CatalogSnapshotId {
        &self.id
    }

    pub fn asset(&self, asset: &AssetId) -> Option<&AssetSpec> {
        self.assets.get(asset)
    }

    pub fn instrument_ids(&self) -> impl Iterator<Item = &InstrumentId> {
        self.history.keys()
    }

    pub fn spec_at(
        &self,
        instrument: &InstrumentId,
        at: DateTime<Utc>,
    ) -> Result<ResolvedInstrument, InstrumentResolutionError> {
        let history = self
            .history
            .get(instrument)
            .ok_or(InstrumentResolutionError::Unknown)?;
        let resolved = history
            .iter()
            .find(|candidate| candidate.effective.contains(at))
            .ok_or_else(|| InstrumentResolutionError::Inactive {
                instrument: instrument.clone(),
                intervals: history.iter().map(|entry| entry.effective).collect(),
            })?;
        Ok(self.resolved(resolved))
    }

    pub fn resolve(
        &self,
        selector: &InstrumentSelector,
        context: &InstrumentResolutionContext,
        at: DateTime<Utc>,
    ) -> Result<ResolvedInstrument, InstrumentResolutionError> {
        match selector {
            InstrumentSelector::Exact { instrument } => {
                if !context.allowed_instruments.contains(instrument) {
                    return Err(InstrumentResolutionError::Disallowed {
                        instrument: instrument.clone(),
                    });
                }
                self.spec_at(instrument, at)
            }
            InstrumentSelector::Alias {
                alias,
                listing_venue,
                market_kind,
            } => {
                let candidates = self
                    .aliases
                    .get(alias)
                    .ok_or(InstrumentResolutionError::Unknown)?;
                let listing_venue = listing_venue
                    .as_ref()
                    .or(context.default_listing_venue.as_ref());
                let market_kind = market_kind
                    .as_ref()
                    .or(context.default_market_kind.as_ref());

                let filtered = candidates
                    .iter()
                    .filter(|instrument| {
                        listing_venue.is_none_or(|venue| &instrument.listing_venue == venue)
                            && market_kind.is_none_or(|kind| &instrument.market_kind == kind)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if filtered.is_empty() {
                    return Err(InstrumentResolutionError::Unknown);
                }

                let allowed = filtered
                    .iter()
                    .filter(|instrument| context.allowed_instruments.contains(*instrument))
                    .cloned()
                    .collect::<Vec<_>>();
                if allowed.is_empty() {
                    return Err(InstrumentResolutionError::Disallowed {
                        instrument: filtered[0].clone(),
                    });
                }

                let mut active = Vec::new();
                let mut inactive = Vec::new();
                for instrument in allowed {
                    let history = self
                        .history
                        .get(&instrument)
                        .ok_or(InstrumentResolutionError::Unknown)?;
                    let matching = history
                        .iter()
                        .filter(|entry| entry.aliases.contains(alias))
                        .collect::<Vec<_>>();
                    if let Some(resolved) =
                        matching.iter().find(|entry| entry.effective.contains(at))
                    {
                        active.push(self.resolved(resolved));
                    } else {
                        inactive.push((
                            instrument,
                            matching.into_iter().map(|entry| entry.effective).collect(),
                        ));
                    }
                }
                if active.len() == 1 {
                    return Ok(active.remove(0));
                }
                if active.len() > 1 {
                    return Err(InstrumentResolutionError::Ambiguous {
                        candidates: active
                            .into_iter()
                            .map(|resolved| resolved.reference.instrument)
                            .collect(),
                    });
                }
                let (instrument, intervals) = inactive.remove(0);
                Err(InstrumentResolutionError::Inactive {
                    instrument,
                    intervals,
                })
            }
        }
    }

    fn resolved(&self, resolved: &InstrumentSpec) -> ResolvedInstrument {
        ResolvedInstrument {
            reference: ResolvedInstrumentRef {
                instrument: resolved.instrument.clone(),
                catalog: self.id.clone(),
                spec_revision: resolved.revision.clone(),
            },
            spec: Arc::new(resolved.clone()),
        }
    }
}

fn validate_assets(
    spec: &InstrumentSpec,
    assets: &BTreeMap<AssetId, AssetSpec>,
) -> Result<(), CatalogCompileError> {
    let mut referenced = BTreeSet::new();
    referenced.extend(spec.assets.base.iter().cloned());
    referenced.extend(spec.assets.quote.iter().cloned());
    referenced.insert(spec.assets.settlement.clone());
    referenced.extend(spec.assets.fee_assets.iter().cloned());
    referenced.insert(spec.economics.settlement_asset.clone());
    if let Some(notional) = &spec.notional {
        referenced.insert(notional.asset.clone());
    }
    for asset in referenced {
        if !assets.contains_key(&asset) {
            return Err(CatalogCompileError::UnknownAsset {
                instrument: spec.instrument.clone(),
                asset,
            });
        }
    }
    Ok(())
}

/// Selector for exact identity or an explicitly scoped alias.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InstrumentSelector {
    Exact {
        instrument: InstrumentId,
    },
    Alias {
        alias: InstrumentAlias,
        listing_venue: Option<ListingVenueId>,
        market_kind: Option<MarketKind>,
    },
}

/// Immutable deployment or account constraints applied during resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentResolutionContext {
    pub allowed_instruments: BTreeSet<InstrumentId>,
    pub default_listing_venue: Option<ListingVenueId>,
    pub default_market_kind: Option<MarketKind>,
}

/// Persistable identity of one specification resolved from one catalog snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedInstrumentRef {
    pub instrument: InstrumentId,
    pub catalog: CatalogSnapshotId,
    pub spec_revision: crate::SpecRevision,
}

/// Resolved reference and immutable specification value.
#[derive(Clone, Debug)]
pub struct ResolvedInstrument {
    pub reference: ResolvedInstrumentRef,
    pub spec: Arc<InstrumentSpec>,
}

/// Catalog compilation failures.
#[derive(Debug, thiserror::Error)]
pub enum CatalogCompileError {
    #[error("unsupported catalog schema version {0}")]
    UnsupportedSchema(u32),
    #[error("catalog version must contain between 1 and 64 bytes")]
    InvalidVersion,
    #[error("duplicate asset {0}")]
    DuplicateAsset(AssetId),
    #[error("instrument {instrument} references unknown asset {asset}")]
    UnknownAsset {
        instrument: InstrumentId,
        asset: AssetId,
    },
    #[error("instrument {instrument} has overlapping effective intervals")]
    OverlappingInterval {
        instrument: InstrumentId,
        left: EffectiveInterval,
        right: EffectiveInterval,
    },
    #[error(transparent)]
    InvalidSpec(#[from] SpecValidationError),
}

/// Deterministic instrument-resolution failures.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InstrumentResolutionError {
    #[error("instrument is unknown")]
    Unknown,
    #[error("instrument {instrument} is outside the resolution allowlist")]
    Disallowed { instrument: InstrumentId },
    #[error("instrument selector is ambiguous")]
    Ambiguous { candidates: Vec<InstrumentId> },
    #[error("instrument {instrument} is inactive at the requested time")]
    Inactive {
        instrument: InstrumentId,
        intervals: Vec<EffectiveInterval>,
    },
}
