//! Deterministic account-currency conversion from historical FX ticks.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

use chrono::{Duration, NaiveDateTime};
use qs_core::PriceQuote;
use qs_symbols::{SymbolRegistry, normalize_currency_code};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

/// Direction in which an FX pair is used for a conversion leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FxPairDirection {
    /// Convert from the pair base currency to its quote currency.
    Direct,
    /// Convert from the pair quote currency to its base currency.
    Inverse,
}

/// One available FX symbol and its registered currencies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FxPair {
    pub symbol: String,
    pub base_currency: String,
    pub quote_currency: String,
}

/// One directed currency-conversion leg.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversionLeg {
    pub pair: FxPair,
    pub direction: FxPairDirection,
}

impl ConversionLeg {
    /// Currency consumed by this leg.
    pub fn from_currency(&self) -> &str {
        match self.direction {
            FxPairDirection::Direct => &self.pair.base_currency,
            FxPairDirection::Inverse => &self.pair.quote_currency,
        }
    }

    /// Currency produced by this leg.
    pub fn to_currency(&self) -> &str {
        match self.direction {
            FxPairDirection::Direct => &self.pair.quote_currency,
            FxPairDirection::Inverse => &self.pair.base_currency,
        }
    }
}

/// Deterministic route selected from a caller-provided available-symbol set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConversionRoute {
    Identity {
        currency: String,
    },
    Direct {
        pair: FxPair,
    },
    Inverse {
        pair: FxPair,
    },
    TwoLeg {
        pivot_currency: String,
        first: ConversionLeg,
        second: ConversionLeg,
    },
}

impl ConversionRoute {
    /// Source currency for this route.
    pub fn from_currency(&self) -> &str {
        match self {
            Self::Identity { currency } => currency,
            Self::Direct { pair } => &pair.base_currency,
            Self::Inverse { pair } => &pair.quote_currency,
            Self::TwoLeg { first, .. } => first.from_currency(),
        }
    }

    /// Destination currency for this route.
    pub fn to_currency(&self) -> &str {
        match self {
            Self::Identity { currency } => currency,
            Self::Direct { pair } => &pair.quote_currency,
            Self::Inverse { pair } => &pair.base_currency,
            Self::TwoLeg { second, .. } => second.to_currency(),
        }
    }

    /// Enumerate route symbols in execution order.
    pub fn symbols(&self) -> impl Iterator<Item = &str> {
        let symbols = match self {
            Self::Identity { .. } => [None, None],
            Self::Direct { pair } | Self::Inverse { pair } => [Some(pair.symbol.as_str()), None],
            Self::TwoLeg { first, second, .. } => [
                Some(first.pair.symbol.as_str()),
                Some(second.pair.symbol.as_str()),
            ],
        };
        symbols.into_iter().flatten()
    }
}

/// Quote side used to execute one signed conversion leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConversionPriceSide {
    Bid,
    Ask,
}

/// Validation failure for a tick quote submitted to the quote book.
#[derive(Debug, Clone, Copy, PartialEq, Error)]
pub enum QuoteValidationError {
    #[error("bid must be finite and positive, got {0}")]
    InvalidBid(f64),
    #[error("ask must be finite and positive, got {0}")]
    InvalidAsk(f64),
    #[error("bid {bid} is greater than ask {ask}")]
    Crossed { bid: f64, ask: f64 },
}

/// Errors produced by route resolution and quote-backed conversion.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum ConversionError {
    #[error("invalid {role} currency code '{value}': expected 3 ASCII letters")]
    InvalidCurrencyCode { role: &'static str, value: String },
    #[error(
        "no conversion route from {from_currency} to {to_currency} in the available symbol set"
    )]
    RouteNotFound {
        from_currency: String,
        to_currency: String,
    },
    #[error("maximum quote staleness must be non-negative, got {millis} ms")]
    InvalidStaleness { millis: i64 },
    #[error("conversion amount must be finite, got {0}")]
    InvalidAmount(f64),
    #[error("quote symbol '{0}' is not registered")]
    UnknownQuoteSymbol(String),
    #[error("quote symbol '{0}' is not a forex symbol")]
    NonForexQuoteSymbol(String),
    #[error("invalid quote for '{symbol}' at {quote_ts}: {reason}")]
    InvalidQuote {
        symbol: String,
        quote_ts: NaiveDateTime,
        reason: QuoteValidationError,
    },
    #[error(
        "no quote for '{symbol}' at or before {operation_ts}; next future quote is {next_quote_ts:?}"
    )]
    NoCausalQuote {
        symbol: String,
        operation_ts: NaiveDateTime,
        next_quote_ts: Option<NaiveDateTime>,
    },
    #[error(
        "stale quote for '{symbol}': {age_millis} ms old at {operation_ts}, maximum is {max_staleness_millis} ms"
    )]
    StaleQuote {
        symbol: String,
        quote_ts: NaiveDateTime,
        operation_ts: NaiveDateTime,
        age_millis: i64,
        max_staleness_millis: i64,
    },
    #[error("invalid conversion route: {0}")]
    InvalidRoute(String),
    #[error("conversion produced a non-finite result on '{symbol}'")]
    NonFiniteResult { symbol: String },
    #[error("canonical quote symbol must not be empty")]
    EmptyCanonicalQuoteSymbol,
}

/// Validation errors for an immutable run currency plan.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum RunCurrencyPlanError {
    #[error("invalid currency code for {field}: '{value}' must be 3 ASCII letters")]
    InvalidCurrencyCode { field: String, value: String },
    #[error("{kind} symbol must not be empty")]
    EmptySymbol { kind: &'static str },
    #[error("primary symbol '{symbol}' has no P&L currency mapping")]
    MissingPrimaryPnlCurrency { symbol: String },
    #[error("P&L currency mapping references non-primary symbol '{symbol}'")]
    UnexpectedPrimaryPnlCurrency { symbol: String },
    #[error("source currency '{source_currency}' has no conversion route")]
    MissingConversionRoute { source_currency: String },
    #[error("multiple conversion routes normalize to source currency '{source_currency}'")]
    DuplicateSourceCurrency { source_currency: String },
    #[error(
        "conversion route key '{source_currency}' does not match route source '{route_source_currency}'"
    )]
    RouteSourceMismatch {
        source_currency: String,
        route_source_currency: String,
    },
    #[error(
        "conversion route for '{source_currency}' ends in '{route_destination_currency}', expected account currency '{account_currency}'"
    )]
    RouteDestinationMismatch {
        source_currency: String,
        route_destination_currency: String,
        account_currency: String,
    },
    #[error("invalid conversion route for '{source_currency}': {reason}")]
    InvalidRoute {
        source_currency: String,
        reason: String,
    },
    #[error(
        "route for '{source_currency}' uses symbol '{symbol}' which is not in conversion_symbols"
    )]
    UndeclaredRouteSymbol {
        source_currency: String,
        symbol: String,
    },
    #[error("warmup quote symbol '{symbol}' is not in conversion_symbols")]
    UndeclaredWarmupSymbol { symbol: String },
    #[error("invalid warmup quote for '{symbol}' at {quote_ts}: {reason}")]
    InvalidWarmupQuote {
        symbol: String,
        quote_ts: NaiveDateTime,
        reason: QuoteValidationError,
    },
    #[error("duplicate warmup quote for '{symbol}' at {quote_ts}")]
    DuplicateWarmupQuote {
        symbol: String,
        quote_ts: NaiveDateTime,
    },
}

/// Auditable execution details for one route leg.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversionLegAudit {
    pub sequence: usize,
    pub symbol: String,
    pub direction: FxPairDirection,
    pub from_currency: String,
    pub to_currency: String,
    pub input_amount: f64,
    pub output_amount: f64,
    pub quote_ts: NaiveDateTime,
    pub quote_age_millis: i64,
    pub bid: f64,
    pub ask: f64,
    pub price_side: ConversionPriceSide,
    pub executable_price: f64,
    pub conversion_rate: f64,
}

/// Result of one identity, direct, inverse, or two-leg conversion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversionResult {
    pub from_currency: String,
    pub to_currency: String,
    pub input_amount: f64,
    pub output_amount: f64,
    pub operation_ts: NaiveDateTime,
    pub route: ConversionRoute,
    pub legs: Vec<ConversionLegAudit>,
}

/// Immutable, validated currency handoff for one backtest run.
#[derive(Debug, Clone, Serialize)]
pub struct RunCurrencyPlan {
    account_currency: String,
    primary_symbols: BTreeSet<String>,
    conversion_symbols: BTreeSet<String>,
    pnl_currency_by_primary_symbol: BTreeMap<String, String>,
    conversion_route_by_source_currency: BTreeMap<String, ConversionRoute>,
    strict_before_warmup_quotes: Vec<PriceQuote>,
}

impl PartialEq for RunCurrencyPlan {
    fn eq(&self, other: &Self) -> bool {
        self.account_currency == other.account_currency
            && self.primary_symbols == other.primary_symbols
            && self.conversion_symbols == other.conversion_symbols
            && self.pnl_currency_by_primary_symbol == other.pnl_currency_by_primary_symbol
            && self.conversion_route_by_source_currency == other.conversion_route_by_source_currency
            && self.strict_before_warmup_quotes.len() == other.strict_before_warmup_quotes.len()
            && self
                .strict_before_warmup_quotes
                .iter()
                .zip(&other.strict_before_warmup_quotes)
                .all(|(left, right)| {
                    left.symbol == right.symbol
                        && left.ts == right.ts
                        && left.bid.to_bits() == right.bid.to_bits()
                        && left.ask.to_bits() == right.ask.to_bits()
                })
    }
}

#[derive(Deserialize)]
struct RunCurrencyPlanWire {
    account_currency: String,
    primary_symbols: BTreeSet<String>,
    conversion_symbols: BTreeSet<String>,
    pnl_currency_by_primary_symbol: BTreeMap<String, String>,
    conversion_route_by_source_currency: BTreeMap<String, ConversionRoute>,
    strict_before_warmup_quotes: Vec<PriceQuote>,
}

impl RunCurrencyPlan {
    /// Construct and validate an immutable run currency plan.
    pub fn new(
        account_currency: impl Into<String>,
        primary_symbols: BTreeSet<String>,
        conversion_symbols: BTreeSet<String>,
        pnl_currency_by_primary_symbol: BTreeMap<String, String>,
        conversion_route_by_source_currency: BTreeMap<String, ConversionRoute>,
        mut strict_before_warmup_quotes: Vec<PriceQuote>,
    ) -> Result<Self, RunCurrencyPlanError> {
        let account_currency =
            normalize_plan_currency("account_currency", &account_currency.into())?;
        validate_plan_symbols("primary", &primary_symbols)?;
        validate_plan_symbols("conversion", &conversion_symbols)?;

        for symbol in pnl_currency_by_primary_symbol.keys() {
            if !primary_symbols.contains(symbol) {
                return Err(RunCurrencyPlanError::UnexpectedPrimaryPnlCurrency {
                    symbol: symbol.clone(),
                });
            }
        }

        let mut normalized_pnl_currencies = BTreeMap::new();
        for symbol in &primary_symbols {
            let raw_currency = pnl_currency_by_primary_symbol.get(symbol).ok_or_else(|| {
                RunCurrencyPlanError::MissingPrimaryPnlCurrency {
                    symbol: symbol.clone(),
                }
            })?;
            let currency = normalize_plan_currency(
                &format!("pnl_currency_by_primary_symbol[{symbol}]"),
                raw_currency,
            )?;
            normalized_pnl_currencies.insert(symbol.clone(), currency);
        }

        let mut normalized_routes = BTreeMap::new();
        for (raw_source_currency, route) in conversion_route_by_source_currency {
            let source_currency = normalize_plan_currency(
                "conversion_route_by_source_currency key",
                &raw_source_currency,
            )?;
            if normalized_routes.contains_key(&source_currency) {
                return Err(RunCurrencyPlanError::DuplicateSourceCurrency { source_currency });
            }
            let route = normalize_plan_route(&source_currency, route)?;
            if route.from_currency() != source_currency {
                return Err(RunCurrencyPlanError::RouteSourceMismatch {
                    source_currency,
                    route_source_currency: route.from_currency().to_owned(),
                });
            }
            if route.to_currency() != account_currency {
                return Err(RunCurrencyPlanError::RouteDestinationMismatch {
                    source_currency,
                    route_destination_currency: route.to_currency().to_owned(),
                    account_currency: account_currency.clone(),
                });
            }
            for symbol in route.symbols() {
                if !conversion_symbols.contains(symbol) {
                    return Err(RunCurrencyPlanError::UndeclaredRouteSymbol {
                        source_currency,
                        symbol: symbol.to_owned(),
                    });
                }
            }
            normalized_routes.insert(source_currency, route);
        }

        for source_currency in normalized_pnl_currencies.values() {
            if !normalized_routes.contains_key(source_currency) {
                return Err(RunCurrencyPlanError::MissingConversionRoute {
                    source_currency: source_currency.clone(),
                });
            }
        }

        let mut warmup_keys = BTreeSet::new();
        for quote in &strict_before_warmup_quotes {
            if quote.symbol.is_empty() {
                return Err(RunCurrencyPlanError::EmptySymbol { kind: "warmup" });
            }
            if !conversion_symbols.contains(&quote.symbol) {
                return Err(RunCurrencyPlanError::UndeclaredWarmupSymbol {
                    symbol: quote.symbol.clone(),
                });
            }
            validate_quote(quote).map_err(|reason| RunCurrencyPlanError::InvalidWarmupQuote {
                symbol: quote.symbol.clone(),
                quote_ts: quote.ts,
                reason,
            })?;
            if !warmup_keys.insert((quote.symbol.clone(), quote.ts)) {
                return Err(RunCurrencyPlanError::DuplicateWarmupQuote {
                    symbol: quote.symbol.clone(),
                    quote_ts: quote.ts,
                });
            }
        }
        strict_before_warmup_quotes.sort_by(|left, right| {
            left.ts
                .cmp(&right.ts)
                .then_with(|| left.symbol.cmp(&right.symbol))
        });

        Ok(Self {
            account_currency,
            primary_symbols,
            conversion_symbols,
            pnl_currency_by_primary_symbol: normalized_pnl_currencies,
            conversion_route_by_source_currency: normalized_routes,
            strict_before_warmup_quotes,
        })
    }

    pub fn account_currency(&self) -> &str {
        &self.account_currency
    }

    pub fn primary_symbols(&self) -> &BTreeSet<String> {
        &self.primary_symbols
    }

    pub fn conversion_symbols(&self) -> &BTreeSet<String> {
        &self.conversion_symbols
    }

    pub fn pnl_currency_by_primary_symbol(&self) -> &BTreeMap<String, String> {
        &self.pnl_currency_by_primary_symbol
    }

    pub fn conversion_route_by_source_currency(&self) -> &BTreeMap<String, ConversionRoute> {
        &self.conversion_route_by_source_currency
    }

    pub fn strict_before_warmup_quotes(&self) -> &[PriceQuote] {
        &self.strict_before_warmup_quotes
    }

    pub fn pnl_currency_for_primary_symbol(&self, primary_symbol: &str) -> Option<&str> {
        self.pnl_currency_by_primary_symbol
            .get(primary_symbol)
            .map(String::as_str)
    }

    /// Get the conversion route selected for a primary symbol's P&L currency.
    pub fn route_for_primary_symbol(&self, primary_symbol: &str) -> Option<&ConversionRoute> {
        let source_currency = self.pnl_currency_by_primary_symbol.get(primary_symbol)?;
        self.conversion_route_by_source_currency
            .get(source_currency)
    }

    /// Enumerate all distinct route symbols in canonical order.
    pub fn route_symbols(&self) -> BTreeSet<&str> {
        self.conversion_route_by_source_currency
            .values()
            .flat_map(ConversionRoute::symbols)
            .collect()
    }
}

impl<'de> Deserialize<'de> for RunCurrencyPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RunCurrencyPlanWire::deserialize(deserializer)?;
        Self::new(
            wire.account_currency,
            wire.primary_symbols,
            wire.conversion_symbols,
            wire.pnl_currency_by_primary_symbol,
            wire.conversion_route_by_source_currency,
            wire.strict_before_warmup_quotes,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Resolve one directed FX pair from the available-symbol set.
///
/// A direct pair is preferred to an inverse pair. Within either orientation,
/// canonical symbol order provides deterministic tie-breaking.
pub fn resolve_fx_pair(
    registry: &SymbolRegistry,
    from_currency: &str,
    to_currency: &str,
    available_symbols: &BTreeSet<String>,
) -> Result<Option<ConversionLeg>, ConversionError> {
    let from_currency = normalize_conversion_currency("source", from_currency)?;
    let to_currency = normalize_conversion_currency("destination", to_currency)?;
    let available = canonical_available_symbols(registry, available_symbols);
    let pairs = available_fx_pairs(registry, &available);
    Ok(find_pair(&pairs, &from_currency, &to_currency))
}

/// Resolve an identity, direct, inverse, or deterministic two-leg FX route.
pub fn resolve_conversion_route(
    registry: &SymbolRegistry,
    from_currency: &str,
    to_currency: &str,
    available_symbols: &BTreeSet<String>,
) -> Result<ConversionRoute, ConversionError> {
    let from_currency = normalize_conversion_currency("source", from_currency)?;
    let to_currency = normalize_conversion_currency("destination", to_currency)?;

    if from_currency == to_currency {
        return Ok(ConversionRoute::Identity {
            currency: from_currency,
        });
    }

    let available = canonical_available_symbols(registry, available_symbols);
    let pairs = available_fx_pairs(registry, &available);
    if let Some(leg) = find_pair(&pairs, &from_currency, &to_currency) {
        return Ok(one_leg_route(leg));
    }

    let mut pivots = BTreeSet::new();
    for pair in &pairs {
        pivots.insert(pair.base_currency.clone());
        pivots.insert(pair.quote_currency.clone());
    }
    pivots.remove(&from_currency);
    pivots.remove(&to_currency);

    for pivot_currency in pivots {
        let Some(first) = find_pair(&pairs, &from_currency, &pivot_currency) else {
            continue;
        };
        let Some(second) = find_pair(&pairs, &pivot_currency, &to_currency) else {
            continue;
        };
        return Ok(ConversionRoute::TwoLeg {
            pivot_currency,
            first,
            second,
        });
    }

    Err(ConversionError::RouteNotFound {
        from_currency,
        to_currency,
    })
}

/// Historical FX tick store with causal, staleness-bounded lookup.
#[derive(Debug, Clone)]
pub struct ConversionQuoteBook {
    max_staleness: Duration,
    quotes: BTreeMap<String, BTreeMap<NaiveDateTime, PriceQuote>>,
}

impl ConversionQuoteBook {
    /// Create a quote book with one maximum age applied to every conversion leg.
    pub fn new(max_staleness: Duration) -> Result<Self, ConversionError> {
        if max_staleness < Duration::zero() {
            return Err(ConversionError::InvalidStaleness {
                millis: max_staleness.num_milliseconds(),
            });
        }
        Ok(Self {
            max_staleness,
            quotes: BTreeMap::new(),
        })
    }

    /// Configured maximum quote age.
    pub fn max_staleness(&self) -> Duration {
        self.max_staleness
    }

    /// Prune replay history while retaining each symbol's latest causal quote.
    ///
    /// Quotes after `replay_ts` are retained. This is opt-in because lookups before the retained quote are no longer available after pruning.
    pub fn prune_replay_history(&mut self, replay_ts: NaiveDateTime) -> usize {
        let mut removed = 0;
        for series in self.quotes.values_mut() {
            let latest_causal_ts = series
                .range(..=replay_ts)
                .next_back()
                .map(|(quote_ts, _)| *quote_ts);
            if let Some(latest_causal_ts) = latest_causal_ts {
                let previous_len = series.len();
                *series = series.split_off(&latest_causal_ts);
                removed += previous_len - series.len();
            }
        }
        removed
    }

    /// Retain only causal predecessors needed by a replay schedule.
    ///
    /// The latest quote at or before `replay_ts` is always retained for each symbol. Each required operation time contributes at most one additional predecessor, so intervening ticks do not accumulate while a primary series is sparse. Historical lookup behavior is unchanged unless this method is called.
    pub fn retain_replay_causal_predecessors(
        &mut self,
        replay_ts: NaiveDateTime,
        required_operation_times: impl IntoIterator<Item = NaiveDateTime>,
    ) -> usize {
        let mut required_times: BTreeSet<_> = required_operation_times.into_iter().collect();
        required_times.insert(replay_ts);

        let mut removed = 0;
        for series in self.quotes.values_mut() {
            let retained_timestamps: BTreeSet<_> = required_times
                .iter()
                .filter_map(|operation_ts| {
                    series
                        .range(..=*operation_ts)
                        .next_back()
                        .map(|(quote_ts, _)| *quote_ts)
                })
                .collect();
            let previous_len = series.len();
            series.retain(|quote_ts, _| retained_timestamps.contains(quote_ts));
            removed += previous_len - series.len();
        }
        removed
    }

    /// Validate and record a tick quote under its canonical FX symbol.
    ///
    /// A quote at the same symbol and timestamp replaces the prior quote and is
    /// returned to make duplicate handling auditable by the caller.
    pub fn record_tick(
        &mut self,
        registry: &SymbolRegistry,
        mut quote: PriceQuote,
    ) -> Result<Option<PriceQuote>, ConversionError> {
        let raw_symbol = quote.symbol.clone();
        let canonical = registry
            .normalize(&raw_symbol)
            .ok_or_else(|| ConversionError::UnknownQuoteSymbol(raw_symbol.clone()))?;
        let spec = registry
            .spec(canonical)
            .ok_or(ConversionError::UnknownQuoteSymbol(raw_symbol))?;
        if spec.category != "forex" {
            return Err(ConversionError::NonForexQuoteSymbol(canonical.to_owned()));
        }

        quote.symbol = canonical.to_owned();
        self.record_canonical_tick(quote)
    }

    /// Validate and record a caller-canonicalized tick without a symbol registry.
    pub fn record_canonical_tick(
        &mut self,
        quote: PriceQuote,
    ) -> Result<Option<PriceQuote>, ConversionError> {
        if quote.symbol.is_empty() {
            return Err(ConversionError::EmptyCanonicalQuoteSymbol);
        }
        validate_quote(&quote).map_err(|reason| ConversionError::InvalidQuote {
            symbol: quote.symbol.clone(),
            quote_ts: quote.ts,
            reason,
        })?;
        Ok(self
            .quotes
            .entry(quote.symbol.clone())
            .or_default()
            .insert(quote.ts, quote))
    }

    /// Convert an amount with a previously resolved route.
    pub fn convert_route(
        &self,
        amount: f64,
        operation_ts: NaiveDateTime,
        route: &ConversionRoute,
    ) -> Result<ConversionResult, ConversionError> {
        if !amount.is_finite() {
            return Err(ConversionError::InvalidAmount(amount));
        }

        let mut legs = Vec::new();
        let output_amount = match route {
            ConversionRoute::Identity { .. } => amount,
            ConversionRoute::Direct { pair } => {
                let leg = ConversionLeg {
                    pair: pair.clone(),
                    direction: FxPairDirection::Direct,
                };
                self.convert_leg(amount, operation_ts, &leg, 1, &mut legs)?
            }
            ConversionRoute::Inverse { pair } => {
                let leg = ConversionLeg {
                    pair: pair.clone(),
                    direction: FxPairDirection::Inverse,
                };
                self.convert_leg(amount, operation_ts, &leg, 1, &mut legs)?
            }
            ConversionRoute::TwoLeg {
                pivot_currency,
                first,
                second,
            } => {
                if first.to_currency() != pivot_currency
                    || second.from_currency() != pivot_currency
                    || first.to_currency() != second.from_currency()
                {
                    return Err(ConversionError::InvalidRoute(format!(
                        "two-leg route does not join at pivot {pivot_currency}"
                    )));
                }
                let pivot_amount = self.convert_leg(amount, operation_ts, first, 1, &mut legs)?;
                self.convert_leg(pivot_amount, operation_ts, second, 2, &mut legs)?
            }
        };

        Ok(ConversionResult {
            from_currency: route.from_currency().to_owned(),
            to_currency: route.to_currency().to_owned(),
            input_amount: amount,
            output_amount,
            operation_ts,
            route: route.clone(),
            legs,
        })
    }

    /// Resolve a route from the provided source-symbol set and execute it.
    pub fn convert_with_symbols(
        &self,
        registry: &SymbolRegistry,
        amount: f64,
        from_currency: &str,
        to_currency: &str,
        operation_ts: NaiveDateTime,
        available_symbols: &BTreeSet<String>,
    ) -> Result<ConversionResult, ConversionError> {
        let route =
            resolve_conversion_route(registry, from_currency, to_currency, available_symbols)?;
        self.convert_route(amount, operation_ts, &route)
    }

    fn convert_leg(
        &self,
        amount: f64,
        operation_ts: NaiveDateTime,
        leg: &ConversionLeg,
        sequence: usize,
        audits: &mut Vec<ConversionLegAudit>,
    ) -> Result<f64, ConversionError> {
        let quote = self.causal_quote(&leg.pair.symbol, operation_ts)?;
        let negative = amount.is_sign_negative();
        let (price_side, executable_price, conversion_rate) = match (leg.direction, negative) {
            (FxPairDirection::Direct, false) => (ConversionPriceSide::Bid, quote.bid, quote.bid),
            (FxPairDirection::Direct, true) => (ConversionPriceSide::Ask, quote.ask, quote.ask),
            (FxPairDirection::Inverse, false) => {
                (ConversionPriceSide::Ask, quote.ask, 1.0 / quote.ask)
            }
            (FxPairDirection::Inverse, true) => {
                (ConversionPriceSide::Bid, quote.bid, 1.0 / quote.bid)
            }
        };
        let output_amount = amount * conversion_rate;
        if !output_amount.is_finite() {
            return Err(ConversionError::NonFiniteResult {
                symbol: leg.pair.symbol.clone(),
            });
        }

        audits.push(ConversionLegAudit {
            sequence,
            symbol: leg.pair.symbol.clone(),
            direction: leg.direction,
            from_currency: leg.from_currency().to_owned(),
            to_currency: leg.to_currency().to_owned(),
            input_amount: amount,
            output_amount,
            quote_ts: quote.ts,
            quote_age_millis: (operation_ts - quote.ts).num_milliseconds(),
            bid: quote.bid,
            ask: quote.ask,
            price_side,
            executable_price,
            conversion_rate,
        });
        Ok(output_amount)
    }

    fn causal_quote(
        &self,
        symbol: &str,
        operation_ts: NaiveDateTime,
    ) -> Result<&PriceQuote, ConversionError> {
        let Some(series) = self.quotes.get(symbol) else {
            return Err(ConversionError::NoCausalQuote {
                symbol: symbol.to_owned(),
                operation_ts,
                next_quote_ts: None,
            });
        };
        let Some((quote_ts, quote)) = series.range(..=operation_ts).next_back() else {
            let next_quote_ts = series
                .range((Excluded(operation_ts), Unbounded))
                .next()
                .map(|(ts, _)| *ts);
            return Err(ConversionError::NoCausalQuote {
                symbol: symbol.to_owned(),
                operation_ts,
                next_quote_ts,
            });
        };

        let age = operation_ts - *quote_ts;
        if age > self.max_staleness {
            return Err(ConversionError::StaleQuote {
                symbol: symbol.to_owned(),
                quote_ts: *quote_ts,
                operation_ts,
                age_millis: age.num_milliseconds(),
                max_staleness_millis: self.max_staleness.num_milliseconds(),
            });
        }
        Ok(quote)
    }
}

fn normalize_plan_currency(field: &str, value: &str) -> Result<String, RunCurrencyPlanError> {
    normalize_currency_code(value).ok_or_else(|| RunCurrencyPlanError::InvalidCurrencyCode {
        field: field.to_owned(),
        value: value.to_owned(),
    })
}

fn validate_plan_symbols(
    kind: &'static str,
    symbols: &BTreeSet<String>,
) -> Result<(), RunCurrencyPlanError> {
    if symbols.iter().any(|symbol| symbol.is_empty()) {
        return Err(RunCurrencyPlanError::EmptySymbol { kind });
    }
    Ok(())
}

fn normalize_plan_route(
    source_currency: &str,
    route: ConversionRoute,
) -> Result<ConversionRoute, RunCurrencyPlanError> {
    let route = match route {
        ConversionRoute::Identity { currency } => ConversionRoute::Identity {
            currency: normalize_plan_currency(
                &format!("route[{source_currency}].currency"),
                &currency,
            )?,
        },
        ConversionRoute::Direct { pair } => ConversionRoute::Direct {
            pair: normalize_plan_pair(source_currency, pair)?,
        },
        ConversionRoute::Inverse { pair } => ConversionRoute::Inverse {
            pair: normalize_plan_pair(source_currency, pair)?,
        },
        ConversionRoute::TwoLeg {
            pivot_currency,
            first,
            second,
        } => {
            let pivot_currency = normalize_plan_currency(
                &format!("route[{source_currency}].pivot_currency"),
                &pivot_currency,
            )?;
            let first = ConversionLeg {
                pair: normalize_plan_pair(source_currency, first.pair)?,
                direction: first.direction,
            };
            let second = ConversionLeg {
                pair: normalize_plan_pair(source_currency, second.pair)?,
                direction: second.direction,
            };
            if first.to_currency() != pivot_currency
                || second.from_currency() != pivot_currency
                || first.to_currency() != second.from_currency()
            {
                return Err(RunCurrencyPlanError::InvalidRoute {
                    source_currency: source_currency.to_owned(),
                    reason: format!("two-leg route does not join at pivot {pivot_currency}"),
                });
            }
            ConversionRoute::TwoLeg {
                pivot_currency,
                first,
                second,
            }
        }
    };
    Ok(route)
}

fn normalize_plan_pair(
    source_currency: &str,
    pair: FxPair,
) -> Result<FxPair, RunCurrencyPlanError> {
    if pair.symbol.is_empty() {
        return Err(RunCurrencyPlanError::InvalidRoute {
            source_currency: source_currency.to_owned(),
            reason: "route pair symbol must not be empty".to_owned(),
        });
    }
    let base_currency = normalize_plan_currency(
        &format!("route[{source_currency}].base_currency"),
        &pair.base_currency,
    )?;
    let quote_currency = normalize_plan_currency(
        &format!("route[{source_currency}].quote_currency"),
        &pair.quote_currency,
    )?;
    if base_currency == quote_currency {
        return Err(RunCurrencyPlanError::InvalidRoute {
            source_currency: source_currency.to_owned(),
            reason: format!(
                "pair '{}' has identical base and quote currencies",
                pair.symbol
            ),
        });
    }
    Ok(FxPair {
        symbol: pair.symbol,
        base_currency,
        quote_currency,
    })
}

fn normalize_conversion_currency(
    role: &'static str,
    value: &str,
) -> Result<String, ConversionError> {
    normalize_currency_code(value).ok_or_else(|| ConversionError::InvalidCurrencyCode {
        role,
        value: value.to_owned(),
    })
}

fn canonical_available_symbols(
    registry: &SymbolRegistry,
    available_symbols: &BTreeSet<String>,
) -> BTreeSet<String> {
    available_symbols
        .iter()
        .filter_map(|symbol| registry.normalize(symbol))
        .map(str::to_owned)
        .collect()
}

fn available_fx_pairs(
    registry: &SymbolRegistry,
    available_symbols: &BTreeSet<String>,
) -> Vec<FxPair> {
    available_symbols
        .iter()
        .filter_map(|symbol| {
            let spec = registry.spec(symbol)?;
            if spec.category != "forex" {
                return None;
            }
            let metadata = registry.currency_metadata(symbol)?;
            Some(FxPair {
                symbol: symbol.clone(),
                base_currency: metadata.base_currency.clone()?,
                quote_currency: metadata.quote_currency.clone()?,
            })
        })
        .collect()
}

fn find_pair(pairs: &[FxPair], from_currency: &str, to_currency: &str) -> Option<ConversionLeg> {
    pairs
        .iter()
        .find(|pair| pair.base_currency == from_currency && pair.quote_currency == to_currency)
        .cloned()
        .map(|pair| ConversionLeg {
            pair,
            direction: FxPairDirection::Direct,
        })
        .or_else(|| {
            pairs
                .iter()
                .find(|pair| {
                    pair.base_currency == to_currency && pair.quote_currency == from_currency
                })
                .cloned()
                .map(|pair| ConversionLeg {
                    pair,
                    direction: FxPairDirection::Inverse,
                })
        })
}

fn one_leg_route(leg: ConversionLeg) -> ConversionRoute {
    match leg.direction {
        FxPairDirection::Direct => ConversionRoute::Direct { pair: leg.pair },
        FxPairDirection::Inverse => ConversionRoute::Inverse { pair: leg.pair },
    }
}

fn validate_quote(quote: &PriceQuote) -> Result<(), QuoteValidationError> {
    if !quote.bid.is_finite() || quote.bid <= 0.0 {
        return Err(QuoteValidationError::InvalidBid(quote.bid));
    }
    if !quote.ask.is_finite() || quote.ask <= 0.0 {
        return Err(QuoteValidationError::InvalidAsk(quote.ask));
    }
    if quote.bid > quote.ask {
        return Err(QuoteValidationError::Crossed {
            bid: quote.bid,
            ask: quote.ask,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use chrono::NaiveDate;

    use super::*;

    fn registry() -> SymbolRegistry {
        let mut toml = String::new();
        for (symbol, base, quote) in [
            ("eurusd", "EUR", "USD"),
            ("usdjpy", "USD", "JPY"),
            ("eurjpy", "EUR", "JPY"),
            ("gbpusd", "GBP", "USD"),
            ("eurgbp", "EUR", "GBP"),
            ("gbpjpy", "GBP", "JPY"),
            ("usdchf", "USD", "CHF"),
            ("eurchf", "EUR", "CHF"),
        ] {
            writeln!(
                toml,
                r#"
[[symbol]]
canonical = "{symbol}"
aliases = []
pip_position = 4
digits = 5
category = "forex"
base_currency = "{base}"
quote_currency = "{quote}"
pnl_currency = "{quote}"
lot_base_units = 100000
lot_step_units = 1000"#
            )
            .unwrap();
        }
        SymbolRegistry::from_toml(&toml).unwrap()
    }

    fn available(symbols: &[&str]) -> BTreeSet<String> {
        symbols.iter().map(|symbol| (*symbol).to_owned()).collect()
    }

    fn ts(seconds: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, seconds)
            .unwrap()
    }

    fn quote(symbol: &str, seconds: u32, bid: f64, ask: f64) -> PriceQuote {
        PriceQuote {
            symbol: symbol.to_owned(),
            ts: ts(seconds),
            bid,
            ask,
        }
    }

    type PlanInputs = (
        BTreeSet<String>,
        BTreeSet<String>,
        BTreeMap<String, String>,
        BTreeMap<String, ConversionRoute>,
        Vec<PriceQuote>,
    );

    fn plan_inputs() -> PlanInputs {
        let primary_symbols = available(&["eurusd", "usdjpy"]);
        let conversion_symbols = available(&["usdjpy"]);
        let pnl_currencies = BTreeMap::from([
            ("eurusd".to_owned(), "usd".to_owned()),
            ("usdjpy".to_owned(), "jpy".to_owned()),
        ]);
        let routes = BTreeMap::from([
            (
                "usd".to_owned(),
                ConversionRoute::Identity {
                    currency: "usd".to_owned(),
                },
            ),
            (
                "jpy".to_owned(),
                ConversionRoute::Inverse {
                    pair: FxPair {
                        symbol: "usdjpy".to_owned(),
                        base_currency: "usd".to_owned(),
                        quote_currency: "jpy".to_owned(),
                    },
                },
            ),
        ]);
        let warmup_quotes = vec![quote("usdjpy", 2, 150.0, 150.1)];
        (
            primary_symbols,
            conversion_symbols,
            pnl_currencies,
            routes,
            warmup_quotes,
        )
    }

    #[test]
    fn route_resolution_supports_identity_direct_and_inverse() {
        let registry = registry();
        let symbols = available(&["EUR/USD"]);

        assert!(matches!(
            resolve_conversion_route(&registry, "usd", "USD", &symbols).unwrap(),
            ConversionRoute::Identity { currency } if currency == "USD"
        ));
        assert!(matches!(
            resolve_conversion_route(&registry, "eur", "usd", &symbols).unwrap(),
            ConversionRoute::Direct { pair } if pair.symbol == "eurusd"
        ));
        assert!(matches!(
            resolve_conversion_route(&registry, "USD", "EUR", &symbols).unwrap(),
            ConversionRoute::Inverse { pair } if pair.symbol == "eurusd"
        ));
    }

    #[test]
    fn two_leg_route_uses_deterministic_pivot_and_available_source_filter() {
        let registry = registry();
        let symbols = available(&["gbpjpy", "eurgbp", "eurjpy", "gbpusd", "usdjpy"]);
        let direct = resolve_conversion_route(&registry, "GBP", "JPY", &symbols).unwrap();
        assert!(matches!(direct, ConversionRoute::Direct { pair } if pair.symbol == "gbpjpy"));

        let filtered = available(&["eurgbp", "eurjpy", "gbpusd", "usdjpy"]);
        let route = resolve_conversion_route(&registry, "GBP", "JPY", &filtered).unwrap();
        match route {
            ConversionRoute::TwoLeg {
                pivot_currency,
                first,
                second,
            } => {
                assert_eq!(pivot_currency, "EUR");
                assert_eq!(first.pair.symbol, "eurgbp");
                assert_eq!(first.direction, FxPairDirection::Inverse);
                assert_eq!(second.pair.symbol, "eurjpy");
                assert_eq!(second.direction, FxPairDirection::Direct);
            }
            other => panic!("expected two-leg route, got {other:?}"),
        }
    }

    #[test]
    fn route_resolution_rejects_cross_source_fallback() {
        let registry = registry();
        let symbols = available(&["gbpusd"]);
        assert!(matches!(
            resolve_conversion_route(&registry, "GBP", "JPY", &symbols),
            Err(ConversionError::RouteNotFound { .. })
        ));
    }

    #[test]
    fn quote_book_rejects_invalid_ticks() {
        let registry = registry();
        let mut book = ConversionQuoteBook::new(Duration::seconds(5)).unwrap();
        let error = book
            .record_tick(&registry, quote("eurusd", 0, 1.2, 1.1))
            .unwrap_err();
        assert!(matches!(
            error,
            ConversionError::InvalidQuote {
                reason: QuoteValidationError::Crossed { .. },
                ..
            }
        ));
    }

    #[test]
    fn direct_and_inverse_conversion_use_signed_bid_ask() {
        let registry = registry();
        let symbols = available(&["eurusd"]);
        let mut book = ConversionQuoteBook::new(Duration::seconds(5)).unwrap();
        book.record_tick(&registry, quote("EUR/USD", 0, 1.1, 1.2))
            .unwrap();

        let direct = resolve_conversion_route(&registry, "EUR", "USD", &symbols).unwrap();
        let positive = book.convert_route(100.0, ts(1), &direct).unwrap();
        let negative = book.convert_route(-100.0, ts(1), &direct).unwrap();
        assert!((positive.output_amount - 110.0).abs() < 1.0e-12);
        assert_eq!(positive.legs[0].price_side, ConversionPriceSide::Bid);
        assert!((negative.output_amount + 120.0).abs() < 1.0e-12);
        assert_eq!(negative.legs[0].price_side, ConversionPriceSide::Ask);

        let inverse = resolve_conversion_route(&registry, "USD", "EUR", &symbols).unwrap();
        let positive = book.convert_route(120.0, ts(1), &inverse).unwrap();
        let negative = book.convert_route(-110.0, ts(1), &inverse).unwrap();
        assert!((positive.output_amount - 100.0).abs() < 1.0e-12);
        assert_eq!(positive.legs[0].price_side, ConversionPriceSide::Ask);
        assert!((negative.output_amount + 100.0).abs() < 1.0e-12);
        assert_eq!(negative.legs[0].price_side, ConversionPriceSide::Bid);
    }

    #[test]
    fn two_leg_conversion_is_signed_and_auditable() {
        let registry = registry();
        let symbols = available(&["eurgbp", "eurjpy"]);
        let route = resolve_conversion_route(&registry, "GBP", "JPY", &symbols).unwrap();
        let mut book = ConversionQuoteBook::new(Duration::seconds(5)).unwrap();
        book.record_tick(&registry, quote("eurgbp", 0, 0.8, 0.9))
            .unwrap();
        book.record_tick(&registry, quote("eurjpy", 1, 160.0, 161.0))
            .unwrap();

        let positive = book.convert_route(90.0, ts(2), &route).unwrap();
        assert!((positive.output_amount - 16_000.0).abs() < 1.0e-9);
        assert_eq!(positive.legs.len(), 2);
        assert_eq!(positive.legs[0].price_side, ConversionPriceSide::Ask);
        assert_eq!(positive.legs[1].price_side, ConversionPriceSide::Bid);
        assert_eq!(positive.legs[0].quote_ts, ts(0));
        assert_eq!(positive.legs[1].quote_ts, ts(1));

        let negative = book.convert_route(-80.0, ts(2), &route).unwrap();
        assert!((negative.output_amount + 16_100.0).abs() < 1.0e-9);
        assert_eq!(negative.legs[0].price_side, ConversionPriceSide::Bid);
        assert_eq!(negative.legs[1].price_side, ConversionPriceSide::Ask);
    }

    #[test]
    fn causal_lookup_uses_latest_past_quote_and_never_future_quote() {
        let registry = registry();
        let symbols = available(&["eurusd"]);
        let route = resolve_conversion_route(&registry, "EUR", "USD", &symbols).unwrap();
        let mut book = ConversionQuoteBook::new(Duration::seconds(10)).unwrap();
        book.record_tick(&registry, quote("eurusd", 0, 1.1, 1.2))
            .unwrap();
        book.record_tick(&registry, quote("eurusd", 10, 2.1, 2.2))
            .unwrap();

        let result = book.convert_route(100.0, ts(5), &route).unwrap();
        assert!((result.output_amount - 110.0).abs() < 1.0e-12);
        assert_eq!(result.legs[0].quote_ts, ts(0));

        let mut future_only = ConversionQuoteBook::new(Duration::seconds(10)).unwrap();
        future_only
            .record_tick(&registry, quote("eurusd", 10, 2.1, 2.2))
            .unwrap();
        assert!(matches!(
            future_only.convert_route(100.0, ts(5), &route),
            Err(ConversionError::NoCausalQuote {
                next_quote_ts: Some(next),
                ..
            }) if next == ts(10)
        ));
    }

    #[test]
    fn causal_lookup_rejects_stale_quote_without_fallback() {
        let registry = registry();
        let symbols = available(&["eurusd"]);
        let route = resolve_conversion_route(&registry, "EUR", "USD", &symbols).unwrap();
        let mut book = ConversionQuoteBook::new(Duration::seconds(5)).unwrap();
        book.record_tick(&registry, quote("eurusd", 0, 1.1, 1.2))
            .unwrap();

        assert!(matches!(
            book.convert_route(100.0, ts(6), &route),
            Err(ConversionError::StaleQuote {
                age_millis: 6_000,
                max_staleness_millis: 5_000,
                ..
            })
        ));
    }

    #[test]
    fn replay_pruning_keeps_the_latest_causal_quote_and_future_quotes() {
        let symbols = available(&["eurusd"]);
        let route = resolve_conversion_route(&registry(), "EUR", "USD", &symbols).unwrap();
        let mut book = ConversionQuoteBook::new(Duration::seconds(10)).unwrap();
        book.record_canonical_tick(quote("eurusd", 0, 1.0, 1.1))
            .unwrap();
        book.record_canonical_tick(quote("eurusd", 2, 2.0, 2.1))
            .unwrap();
        book.record_canonical_tick(quote("eurusd", 4, 4.0, 4.1))
            .unwrap();

        assert_eq!(book.prune_replay_history(ts(2)), 1);
        assert_eq!(book.quotes["eurusd"].len(), 2);
        let current = book.convert_route(100.0, ts(3), &route).unwrap();
        assert_eq!(current.legs[0].quote_ts, ts(2));
        let future = book.convert_route(100.0, ts(4), &route).unwrap();
        assert_eq!(future.legs[0].quote_ts, ts(4));
        assert!(matches!(
            book.convert_route(100.0, ts(1), &route),
            Err(ConversionError::NoCausalQuote {
                next_quote_ts: Some(next),
                ..
            }) if next == ts(2)
        ));
    }

    #[test]
    fn schedule_retention_keeps_only_required_predecessors_and_latest_replay_quotes() {
        let symbols = available(&["eurusd"]);
        let route = resolve_conversion_route(&registry(), "EUR", "USD", &symbols).unwrap();
        let mut book = ConversionQuoteBook::new(Duration::seconds(200)).unwrap();
        for second in 0..=50 {
            book.record_canonical_tick(quote(
                "eurusd",
                second,
                1.0 + second as f64,
                1.1 + second as f64,
            ))
            .unwrap();
        }

        let required = [
            ts(10) + Duration::milliseconds(500),
            ts(40) + Duration::milliseconds(500),
        ];
        assert_eq!(book.retain_replay_causal_predecessors(ts(50), required), 48);
        assert_eq!(
            book.quotes["eurusd"].keys().copied().collect::<Vec<_>>(),
            vec![ts(10), ts(40), ts(50)]
        );
        assert_eq!(
            book.convert_route(1.0, required[0], &route).unwrap().legs[0].quote_ts,
            ts(10)
        );
        assert_eq!(
            book.convert_route(1.0, required[1], &route).unwrap().legs[0].quote_ts,
            ts(40)
        );
        assert_eq!(
            book.convert_route(1.0, ts(50), &route).unwrap().legs[0].quote_ts,
            ts(50)
        );
    }

    #[test]
    fn schedule_retention_bounds_high_frequency_ticks_before_sparse_primary_quote() {
        let mut book = ConversionQuoteBook::new(Duration::seconds(200)).unwrap();
        let start = ts(0);
        let effective_ts = start + Duration::milliseconds(2_500);

        for millisecond in 0..=10_000 {
            let replay_ts = start + Duration::milliseconds(millisecond);
            let mut tick = quote("eurusd", 0, 1.0, 1.1);
            tick.ts = replay_ts;
            book.record_canonical_tick(tick).unwrap();
            book.retain_replay_causal_predecessors(replay_ts, [effective_ts]);
            assert!(
                book.quotes["eurusd"].len() <= 2,
                "only the scheduled predecessor and latest replay quote may remain"
            );
        }

        assert_eq!(
            book.quotes["eurusd"].keys().copied().collect::<Vec<_>>(),
            vec![effective_ts, start + Duration::milliseconds(10_000)]
        );
    }

    #[test]
    fn historical_quote_book_retains_history_without_explicit_pruning() {
        let symbols = available(&["eurusd"]);
        let route = resolve_conversion_route(&registry(), "EUR", "USD", &symbols).unwrap();
        let mut book = ConversionQuoteBook::new(Duration::seconds(10)).unwrap();
        book.record_canonical_tick(quote("eurusd", 0, 1.0, 1.1))
            .unwrap();
        book.record_canonical_tick(quote("eurusd", 4, 4.0, 4.1))
            .unwrap();

        let historical = book.convert_route(100.0, ts(1), &route).unwrap();
        assert_eq!(historical.legs[0].quote_ts, ts(0));
        assert_eq!(book.quotes["eurusd"].len(), 2);
    }

    #[test]
    fn run_currency_plan_is_immutable_normalized_and_serializable() {
        let (primary, conversion, pnl, routes, mut warmup) = plan_inputs();
        warmup.push(quote("usdjpy", 1, 149.0, 149.1));
        let plan = RunCurrencyPlan::new(" usd ", primary, conversion, pnl, routes, warmup).unwrap();

        assert_eq!(plan.account_currency(), "USD");
        assert_eq!(plan.pnl_currency_for_primary_symbol("usdjpy"), Some("JPY"));
        assert!(matches!(
            plan.route_for_primary_symbol("eurusd"),
            Some(ConversionRoute::Identity { currency }) if currency == "USD"
        ));
        assert!(matches!(
            plan.route_for_primary_symbol("usdjpy"),
            Some(ConversionRoute::Inverse { pair }) if pair.symbol == "usdjpy"
        ));
        assert_eq!(plan.route_symbols(), BTreeSet::from(["usdjpy"]));
        assert_eq!(plan.strict_before_warmup_quotes()[0].ts, ts(1));
        assert_eq!(plan.strict_before_warmup_quotes()[1].ts, ts(2));

        let json = serde_json::to_string(&plan).unwrap();
        let decoded: RunCurrencyPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.account_currency(), "USD");
        assert_eq!(decoded.primary_symbols(), plan.primary_symbols());
        assert_eq!(decoded.conversion_symbols(), plan.conversion_symbols());
        assert_eq!(
            decoded.pnl_currency_by_primary_symbol(),
            plan.pnl_currency_by_primary_symbol()
        );
        assert_eq!(
            decoded.conversion_route_by_source_currency(),
            plan.conversion_route_by_source_currency()
        );
        assert_eq!(decoded.strict_before_warmup_quotes().len(), 2);
    }

    #[test]
    fn run_currency_plan_requires_complete_primary_mappings_and_routes() {
        let (primary, conversion, mut pnl, routes, warmup) = plan_inputs();
        pnl.remove("usdjpy");
        assert!(matches!(
            RunCurrencyPlan::new(
                "USD",
                primary.clone(),
                conversion.clone(),
                pnl,
                routes.clone(),
                warmup.clone(),
            ),
            Err(RunCurrencyPlanError::MissingPrimaryPnlCurrency { symbol })
                if symbol == "usdjpy"
        ));

        let (_, _, pnl, mut routes, _) = plan_inputs();
        routes.remove("jpy");
        assert!(matches!(
            RunCurrencyPlan::new("USD", primary, conversion, pnl, routes, warmup),
            Err(RunCurrencyPlanError::MissingConversionRoute { source_currency })
                if source_currency == "JPY"
        ));
    }

    #[test]
    fn run_currency_plan_rejects_wrong_destination_and_undeclared_route_symbol() {
        let (primary, mut conversion, pnl, mut routes, warmup) = plan_inputs();
        routes.insert(
            "jpy".to_owned(),
            ConversionRoute::Inverse {
                pair: FxPair {
                    symbol: "eurjpy".to_owned(),
                    base_currency: "EUR".to_owned(),
                    quote_currency: "JPY".to_owned(),
                },
            },
        );
        conversion.insert("eurjpy".to_owned());
        assert!(matches!(
            RunCurrencyPlan::new(
                "USD",
                primary.clone(),
                conversion,
                pnl.clone(),
                routes,
                warmup.clone(),
            ),
            Err(RunCurrencyPlanError::RouteDestinationMismatch {
                source_currency,
                route_destination_currency,
                ..
            }) if source_currency == "JPY" && route_destination_currency == "EUR"
        ));

        let (_, conversion, _, routes, _) = plan_inputs();
        let conversion = conversion
            .into_iter()
            .filter(|symbol| symbol != "usdjpy")
            .collect();
        assert!(matches!(
            RunCurrencyPlan::new("USD", primary, conversion, pnl, routes, warmup),
            Err(RunCurrencyPlanError::UndeclaredRouteSymbol { symbol, .. })
                if symbol == "usdjpy"
        ));
    }

    #[test]
    fn run_currency_plan_validates_warmup_quotes() {
        let (primary, conversion, pnl, routes, _) = plan_inputs();
        assert!(matches!(
            RunCurrencyPlan::new(
                "USD",
                primary.clone(),
                conversion.clone(),
                pnl.clone(),
                routes.clone(),
                vec![quote("eurusd", 0, 1.0, 1.1)],
            ),
            Err(RunCurrencyPlanError::UndeclaredWarmupSymbol { symbol })
                if symbol == "eurusd"
        ));
        assert!(matches!(
            RunCurrencyPlan::new(
                "USD",
                primary.clone(),
                conversion.clone(),
                pnl.clone(),
                routes.clone(),
                vec![quote("usdjpy", 0, 151.0, 150.0)],
            ),
            Err(RunCurrencyPlanError::InvalidWarmupQuote {
                reason: QuoteValidationError::Crossed { .. },
                ..
            })
        ));
        let duplicate = quote("usdjpy", 0, 150.0, 150.1);
        assert!(matches!(
            RunCurrencyPlan::new(
                "USD",
                primary,
                conversion,
                pnl,
                routes,
                vec![duplicate.clone(), duplicate],
            ),
            Err(RunCurrencyPlanError::DuplicateWarmupQuote { .. })
        ));
    }

    #[test]
    fn record_canonical_tick_does_not_require_registry() {
        let mut book = ConversionQuoteBook::new(Duration::seconds(5)).unwrap();
        book.record_canonical_tick(quote("eurusd", 0, 1.1, 1.2))
            .unwrap();
        let route = ConversionRoute::Direct {
            pair: FxPair {
                symbol: "eurusd".to_owned(),
                base_currency: "EUR".to_owned(),
                quote_currency: "USD".to_owned(),
            },
        };
        let result = book.convert_route(100.0, ts(1), &route).unwrap();
        assert!((result.output_amount - 110.0).abs() < 1.0e-12);

        assert!(matches!(
            book.record_canonical_tick(quote("eurusd", 2, 1.2, 1.1)),
            Err(ConversionError::InvalidQuote {
                reason: QuoteValidationError::Crossed { .. },
                ..
            })
        ));
        assert!(matches!(
            book.record_canonical_tick(quote("", 2, 1.1, 1.2)),
            Err(ConversionError::EmptyCanonicalQuoteSymbol)
        ));
    }
}
