use qs_backtest::ConfiguredHistoricalBindings;
use qs_strategy::{
    ConfiguredStrategyRequirements, MaterialLibrary, ParameterBinding, StrategyConfig,
};

use crate::geometry::SeriesGeometry;

/// A parameterized family of configured strategies.
///
/// A family maps one typed point to a complete bound strategy document and to the historical series geometry backing its logical sources. Implementations must be deterministic so batch output does not depend on worker scheduling.
pub trait StrategyFamily: Sync {
    type Params: Clone + Send + Sync;

    fn family_id(&self) -> &str;

    fn points(&self) -> Vec<Self::Params>;

    fn parameter_binding(&self, point: &Self::Params) -> ParameterBinding;

    fn config(&self, point: &Self::Params) -> StrategyConfig;

    fn geometry(&self, symbol: &str, point: &Self::Params) -> Vec<SeriesGeometry>;

    fn bindings(
        &self,
        symbol: &str,
        point: &Self::Params,
        requirements: &ConfiguredStrategyRequirements,
    ) -> Result<ConfiguredHistoricalBindings, String> {
        ConfiguredHistoricalBindings::from_geometry(self.geometry(symbol, point), requirements)
            .map_err(|error| error.to_string())
    }

    fn library(&self) -> MaterialLibrary {
        MaterialLibrary::builtins()
    }
}
