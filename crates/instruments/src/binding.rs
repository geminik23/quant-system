use serde::{Deserialize, Serialize};

use crate::{
    EffectiveInterval, ExecutionVenueId, MarketDataSourceId, ResolvedInstrumentRef,
    TradingPlatformId,
};

/// Binds an existing stored series namespace to one resolved instrument.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredSeriesBinding {
    pub data_source: MarketDataSourceId,
    pub source_partition: String,
    pub source_symbol: String,
    pub instrument: ResolvedInstrumentRef,
    pub effective: EffectiveInterval,
}

/// Binds a broker execution context and trading platform identifier to one instrument.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformInstrumentBinding {
    pub execution_venue: ExecutionVenueId,
    pub platform: TradingPlatformId,
    pub platform_instrument_id: String,
    pub instrument: ResolvedInstrumentRef,
    pub effective: EffectiveInterval,
}
