//! Exact, dependency-light instrument and asset domain contracts.

mod binding;
mod catalog;
mod decimal;
mod economics;
mod grid;

mod identity;
mod spec;

pub use binding::{PlatformInstrumentBinding, StoredSeriesBinding};
pub use catalog::{
    CatalogCompileError, CatalogDocument, CatalogSnapshotId, InstrumentCatalogSnapshot,
    InstrumentResolutionContext, InstrumentResolutionError, InstrumentSelector, ResolvedInstrument,
    ResolvedInstrumentRef,
};
pub use decimal::{
    Decimal, DecimalError, MAX_DECIMAL_SCALE, Money, NonNegativeDecimal, PositiveDecimal, Price,
    Quantity,
};
pub use economics::{
    BoundEconomicCapability, EconomicOperation, EconomicsBinding, EconomicsCapabilityError,
    EconomicsCapabilityProvider, bind_economics,
};
pub use grid::{AdjustmentDirection, DecimalGrid, GridAdjustment, GridError, GridRounding};

pub use identity::{
    AssetId, EconomicsImplementationId, EconomicsModelId, ExecutionVenueId, IdentifierError,
    InstrumentAlias, InstrumentId, InstrumentIdError, ListingId, ListingVenueId,
    MarketDataSourceId, MarketKind, SpecRevision, TradingPlatformId,
};
pub use spec::{
    AssetKind, AssetSpec, EffectiveInterval, EffectiveIntervalError, InstrumentAssets,
    InstrumentEconomics, InstrumentSpec, ListingStatus, NotionalRules, PriceRules, QuantityRules,
    QuantityUnit, SpecValidationError,
};
