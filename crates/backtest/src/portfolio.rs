//! Quote-aware portfolio snapshots and online account accounting.

use std::collections::BTreeMap;

use chrono::NaiveDateTime;
use qs_core::{FillModel, PriceQuote, Side};
use serde::{Deserialize, Serialize};

use crate::artifacts::OpenPositionSnapshot;
use crate::currency::{ConversionQuoteBook, ConversionResult, ConversionRoute, RunCurrencyPlan};

/// One account observation. Aggregate values are `None` when they cannot be
/// computed completely (for example, because any open position is unpriced).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct EquityPoint {
    pub ts: NaiveDateTime,
    /// Runner-defined observation stage for consumers that need event context.
    pub observation_kind: Option<String>,
    /// Stable observation order before any output sampling is applied.
    pub observation_sequence: Option<u64>,
    pub realized_pnl: Option<f64>,
    /// Initial balance plus realized P&L.
    pub cash_balance: Option<f64>,
    pub unrealized_pnl: Option<f64>,
    pub equity: Option<f64>,
    /// Current peak-to-equity drawdown, represented as a non-negative amount.
    pub drawdown: Option<f64>,
    pub drawdown_pct: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub max_drawdown_pct: Option<f64>,
    pub gross_exposure: Option<f64>,
    /// Loss from the current liquidation mark to effective protective stops.
    pub open_risk: Option<f64>,
    pub open_position_count: usize,
    pub stale_position_count: usize,
    pub unpriced_position_count: usize,
    pub unavailable_open_risk_count: usize,
}

/// Campaign P&L extrema, including a zero baseline before the first mark.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CampaignExcursion {
    pub mae: f64,
    pub mfe: f64,
    pub observations: u64,
}

impl Default for CampaignExcursion {
    fn default() -> Self {
        Self {
            mae: 0.0,
            mfe: 0.0,
            observations: 0,
        }
    }
}

impl CampaignExcursion {
    pub fn observe(&mut self, campaign_pnl: f64) -> bool {
        if !campaign_pnl.is_finite() {
            return false;
        }
        self.mae = self.mae.min(campaign_pnl);
        self.mfe = self.mfe.max(campaign_pnl);
        self.observations += 1;
        true
    }
}

/// Stateful quote and account recorder suitable for incremental runner use.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PortfolioRecorder {
    initial_balance: f64,
    realized_pnl: f64,
    contract_sizes: BTreeMap<String, f64>,
    default_contract_size: f64,
    stale_quote_after_millis: Option<i64>,
    fill_model: FillModel,
    currency_plan: Option<RunCurrencyPlan>,
    latest_quotes: BTreeMap<String, PriceQuote>,
    campaigns: BTreeMap<String, CampaignExcursion>,
    latest_open_positions: Vec<OpenPositionSnapshot>,
    equity_curve: Vec<EquityPoint>,
    peak_equity: Option<f64>,
    max_drawdown: Option<f64>,
    max_drawdown_pct: Option<f64>,
}

impl Default for PortfolioRecorder {
    fn default() -> Self {
        Self::new(0.0, BTreeMap::new())
    }
}

impl PortfolioRecorder {
    /// Create a recorder. Missing symbols use contract size `1.0`, matching the
    /// current backtest executor's backward-compatible behavior.
    pub fn new(
        initial_balance: f64,
        contract_sizes: impl IntoIterator<Item = (String, f64)>,
    ) -> Self {
        let valid_initial_balance = initial_balance.is_finite().then_some(initial_balance);
        Self {
            initial_balance,
            realized_pnl: 0.0,
            contract_sizes: contract_sizes.into_iter().collect(),
            default_contract_size: 1.0,
            stale_quote_after_millis: None,
            fill_model: FillModel::BidAsk,
            currency_plan: None,
            latest_quotes: BTreeMap::new(),
            campaigns: BTreeMap::new(),
            latest_open_positions: Vec::new(),
            equity_curve: Vec::new(),
            peak_equity: valid_initial_balance,
            max_drawdown: valid_initial_balance.map(|_| 0.0),
            max_drawdown_pct: valid_initial_balance
                .filter(|balance| *balance > 0.0)
                .map(|_| 0.0),
        }
    }

    pub fn with_fill_model(mut self, fill_model: FillModel) -> Self {
        self.fill_model = fill_model;
        self
    }

    pub fn with_stale_quote_after_millis(mut self, stale_after_millis: Option<i64>) -> Self {
        self.stale_quote_after_millis = stale_after_millis.map(|value| value.max(0));
        self
    }

    pub fn with_currency_plan(mut self, currency_plan: Option<RunCurrencyPlan>) -> Self {
        self.currency_plan = currency_plan;
        self
    }

    pub fn set_default_contract_size(&mut self, contract_size: f64) -> bool {
        if !contract_size.is_finite() || contract_size <= 0.0 {
            return false;
        }
        self.default_contract_size = contract_size;
        true
    }

    pub fn set_contract_size(&mut self, symbol: impl Into<String>, contract_size: f64) -> bool {
        if !contract_size.is_finite() || contract_size <= 0.0 {
            return false;
        }
        self.contract_sizes.insert(symbol.into(), contract_size);
        true
    }

    /// Retain the newest quote for a symbol. Returns `false` for an out-of-order
    /// older quote that was ignored.
    pub fn record_quote(&mut self, quote: PriceQuote) -> bool {
        if self
            .latest_quotes
            .get(&quote.symbol)
            .is_some_and(|current| current.ts > quote.ts)
        {
            return false;
        }
        self.latest_quotes.insert(quote.symbol.clone(), quote);
        true
    }

    pub fn quote(&self, symbol: &str) -> Option<&PriceQuote> {
        self.latest_quotes.get(symbol)
    }

    pub fn initial_balance(&self) -> f64 {
        self.initial_balance
    }

    pub fn realized_pnl(&self) -> f64 {
        self.realized_pnl
    }

    /// Add a realized close result. Non-finite values are rejected.
    pub fn add_realized_pnl(&mut self, pnl: f64) -> bool {
        if !pnl.is_finite() || !(self.realized_pnl + pnl).is_finite() {
            return false;
        }
        self.realized_pnl += pnl;
        true
    }

    /// Replace the cumulative realized P&L (useful when adapting an executor
    /// that already owns the authoritative realized total).
    pub fn set_realized_pnl(&mut self, pnl: f64) -> bool {
        if !pnl.is_finite() {
            return false;
        }
        self.realized_pnl = pnl;
        true
    }

    /// Mark normalized open positions at the latest per-symbol quotes and append one equity point.
    pub fn record(
        &mut self,
        ts: NaiveDateTime,
        positions: impl IntoIterator<Item = OpenPositionSnapshot>,
    ) -> EquityPoint {
        self.record_with_currency(ts, positions, None)
    }

    pub fn observe(
        &mut self,
        ts: NaiveDateTime,
        positions: impl IntoIterator<Item = OpenPositionSnapshot>,
    ) -> EquityPoint {
        self.observe_with_currency(ts, positions, None)
    }

    pub fn record_with_currency(
        &mut self,
        ts: NaiveDateTime,
        positions: impl IntoIterator<Item = OpenPositionSnapshot>,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> EquityPoint {
        let point = self.observe_with_currency(ts, positions, conversion_quotes);
        self.equity_curve.push(point.clone());
        point
    }

    /// Mark positions and update exact accounting state without appending to the legacy curve.
    pub fn observe_with_currency(
        &mut self,
        ts: NaiveDateTime,
        positions: impl IntoIterator<Item = OpenPositionSnapshot>,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> EquityPoint {
        let mut marked_positions = Vec::new();
        let mut unrealized_pnl = 0.0;
        let mut gross_exposure = 0.0;
        let mut open_risk = 0.0;
        let mut stale_position_count = 0;
        let mut unpriced_position_count = 0;
        let mut unavailable_unrealized_count = 0;
        let mut unavailable_exposure_count = 0;
        let mut unavailable_open_risk_count = 0;

        for mut position in positions {
            position.clear_mark();
            let (native_currency, account_currency) = self.currency_labels(&position.symbol);
            position.native_currency = native_currency;
            position.account_currency = account_currency;
            let Some((quote, mark_price, contract_size)) = self.pricing_inputs(&position) else {
                unpriced_position_count += 1;
                unavailable_unrealized_count += 1;
                unavailable_exposure_count += 1;
                unavailable_open_risk_count += 1;
                marked_positions.push(position);
                continue;
            };

            if self.is_stale(quote, ts) {
                stale_position_count += 1;
            }

            let native_unrealized = match position.side {
                Side::Buy => mark_price - position.average_entry_price,
                Side::Sell => position.average_entry_price - mark_price,
            } * position.remaining_size
                * contract_size;
            let exposure_sign = match position.side {
                Side::Buy => 1.0,
                Side::Sell => -1.0,
            };
            let native_signed_exposure =
                exposure_sign * mark_price * position.remaining_size * contract_size;

            if !native_unrealized.is_finite() || !native_signed_exposure.is_finite() {
                unpriced_position_count += 1;
                unavailable_unrealized_count += 1;
                unavailable_exposure_count += 1;
                unavailable_open_risk_count += 1;
                marked_positions.push(position);
                continue;
            }

            position.quote_ts = Some(quote.ts);
            position.mark_price = Some(mark_price);
            position.native_unrealized_pnl = Some(native_unrealized);
            position.native_signed_exposure = Some(native_signed_exposure);

            let mut valuation_unavailable = false;
            match self.convert_native_amount(
                &position.symbol,
                native_unrealized,
                ts,
                conversion_quotes,
            ) {
                Some((account_unrealized, conversion)) => {
                    position.unrealized_pnl = Some(account_unrealized);
                    position.unrealized_pnl_conversion = conversion;
                    unrealized_pnl += account_unrealized;
                }
                None => {
                    unavailable_unrealized_count += 1;
                    valuation_unavailable = true;
                }
            }
            match self.convert_native_amount(
                &position.symbol,
                native_signed_exposure,
                ts,
                conversion_quotes,
            ) {
                Some((account_signed_exposure, conversion)) => {
                    let account_exposure = account_signed_exposure.abs();
                    if account_exposure.is_finite() {
                        position.gross_exposure = Some(account_exposure);
                        position.gross_exposure_conversion = conversion;
                        gross_exposure += account_exposure;
                    } else {
                        unavailable_exposure_count += 1;
                        valuation_unavailable = true;
                    }
                }
                None => {
                    unavailable_exposure_count += 1;
                    valuation_unavailable = true;
                }
            }
            if valuation_unavailable {
                unpriced_position_count += 1;
            }

            let native_stop_liability = position.effective_stop.and_then(|stop| {
                if !stop.price.is_finite() {
                    return None;
                }
                let stop_pnl = match position.side {
                    Side::Buy => stop.price - mark_price,
                    Side::Sell => mark_price - stop.price,
                } * position.remaining_size
                    * contract_size;
                stop_pnl.is_finite().then_some(stop_pnl.min(0.0))
            });
            if let Some(native_liability) = native_stop_liability {
                position.native_open_risk = Some(-native_liability);
                match self.convert_native_amount(
                    &position.symbol,
                    native_liability,
                    ts,
                    conversion_quotes,
                ) {
                    Some((account_liability, conversion)) => {
                        let account_risk = (-account_liability).max(0.0);
                        if account_risk.is_finite() {
                            position.open_risk = Some(account_risk);
                            position.open_risk_conversion = conversion;
                            open_risk += account_risk;
                        } else {
                            unavailable_open_risk_count += 1;
                        }
                    }
                    None => unavailable_open_risk_count += 1,
                }
            } else {
                unavailable_open_risk_count += 1;
            }

            if let Some(account_unrealized) = position.unrealized_pnl {
                let campaign_pnl = position.realized_pnl + account_unrealized;
                let campaign = self
                    .campaigns
                    .entry(position.position_id.clone())
                    .or_default();
                if campaign.observe(campaign_pnl) {
                    position.campaign_mae = Some(campaign.mae);
                    position.campaign_mfe = Some(campaign.mfe);
                }
            }

            marked_positions.push(position);
        }

        let open_position_count = marked_positions.len();
        let complete_unrealized = unavailable_unrealized_count == 0;
        let complete_exposure = unavailable_exposure_count == 0;
        let realized = self.realized_pnl.is_finite().then_some(self.realized_pnl);
        let cash_balance = realized.and_then(|pnl| {
            let balance = self.initial_balance + pnl;
            balance.is_finite().then_some(balance)
        });
        let total_unrealized = complete_unrealized.then_some(unrealized_pnl);
        let equity = cash_balance
            .zip(total_unrealized)
            .and_then(|(cash, floating)| {
                let value = cash + floating;
                value.is_finite().then_some(value)
            });
        let total_exposure = complete_exposure.then_some(gross_exposure);
        let total_open_risk = (unavailable_open_risk_count == 0).then_some(open_risk);
        let (drawdown, drawdown_pct) = self.observe_equity(equity);

        let point = EquityPoint {
            ts,
            observation_kind: None,
            observation_sequence: None,
            realized_pnl: realized,
            cash_balance,
            unrealized_pnl: total_unrealized,
            equity,
            drawdown,
            drawdown_pct,
            max_drawdown: self.max_drawdown,
            max_drawdown_pct: self.max_drawdown_pct,
            gross_exposure: total_exposure,
            open_risk: total_open_risk,
            open_position_count,
            stale_position_count,
            unpriced_position_count,
            unavailable_open_risk_count,
        };
        self.latest_open_positions = marked_positions;
        point
    }

    fn currency_labels(&self, symbol: &str) -> (Option<String>, Option<String>) {
        let Some(plan) = self.currency_plan.as_ref() else {
            return (None, None);
        };
        (
            plan.pnl_currency_for_primary_symbol(symbol)
                .map(str::to_owned),
            Some(plan.account_currency().to_owned()),
        )
    }

    fn convert_native_amount(
        &self,
        symbol: &str,
        amount: f64,
        operation_ts: NaiveDateTime,
        conversion_quotes: Option<&ConversionQuoteBook>,
    ) -> Option<(f64, Option<ConversionResult>)> {
        if !amount.is_finite() {
            return None;
        }
        let Some(plan) = self.currency_plan.as_ref() else {
            return Some((amount, None));
        };
        let route = plan.route_for_primary_symbol(symbol)?;
        let conversion = match conversion_quotes {
            Some(quotes) => quotes.convert_route(amount, operation_ts, route).ok()?,
            None => match route {
                ConversionRoute::Identity { .. } => ConversionResult {
                    from_currency: route.from_currency().to_owned(),
                    to_currency: route.to_currency().to_owned(),
                    input_amount: amount,
                    output_amount: amount,
                    operation_ts,
                    route: route.clone(),
                    legs: Vec::new(),
                },
                _ => return None,
            },
        };
        conversion
            .output_amount
            .is_finite()
            .then_some((conversion.output_amount, Some(conversion)))
    }

    fn pricing_inputs<'a>(
        &'a self,
        position: &OpenPositionSnapshot,
    ) -> Option<(&'a PriceQuote, f64, f64)> {
        if !position.average_entry_price.is_finite()
            || !position.remaining_size.is_finite()
            || position.remaining_size < 0.0
        {
            return None;
        }
        let quote = self.latest_quotes.get(&position.symbol)?;
        let mark_price = quote.eval_price(position.side, self.fill_model);
        let contract_size = self
            .contract_sizes
            .get(&position.symbol)
            .copied()
            .unwrap_or(self.default_contract_size);
        if !mark_price.is_finite() || !contract_size.is_finite() || contract_size <= 0.0 {
            return None;
        }
        Some((quote, mark_price, contract_size))
    }

    fn is_stale(&self, quote: &PriceQuote, ts: NaiveDateTime) -> bool {
        let Some(limit) = self.stale_quote_after_millis else {
            return false;
        };
        let age = ts.signed_duration_since(quote.ts).num_milliseconds();
        age > limit
    }

    fn observe_equity(&mut self, equity: Option<f64>) -> (Option<f64>, Option<f64>) {
        let Some(equity) = equity else {
            return (None, None);
        };
        let peak = match self.peak_equity {
            Some(peak) if peak >= equity => peak,
            _ => {
                self.peak_equity = Some(equity);
                equity
            }
        };
        let drawdown = (peak - equity).max(0.0);
        self.max_drawdown = Some(self.max_drawdown.unwrap_or(0.0).max(drawdown));

        let drawdown_pct = (peak > 0.0).then_some(drawdown / peak);
        if let Some(value) = drawdown_pct {
            self.max_drawdown_pct = Some(self.max_drawdown_pct.unwrap_or(0.0).max(value));
        }
        (Some(drawdown), drawdown_pct)
    }

    pub fn campaign_excursion(&self, position_id: &str) -> Option<CampaignExcursion> {
        self.campaigns.get(position_id).copied()
    }

    /// Finalize and remove campaign excursion state, including final realized
    /// net P&L as the last observation.
    pub fn finish_campaign(
        &mut self,
        position_id: &str,
        final_net_pnl: f64,
    ) -> Option<CampaignExcursion> {
        let mut campaign = self.campaigns.remove(position_id).unwrap_or_default();
        campaign.observe(final_net_pnl).then_some(campaign)
    }

    pub fn latest_open_positions(&self) -> &[OpenPositionSnapshot] {
        &self.latest_open_positions
    }

    pub fn equity_curve(&self) -> &[EquityPoint] {
        &self.equity_curve
    }

    pub fn max_drawdown(&self) -> Option<f64> {
        self.max_drawdown
    }

    pub fn max_drawdown_pct(&self) -> Option<f64> {
        self.max_drawdown_pct
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate};
    use qs_core::{EffectiveStop, StopOrigin};

    use crate::currency::{ConversionPriceSide, FxPair};

    fn ts(second: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 3, 4)
            .unwrap()
            .and_hms_opt(5, 6, second)
            .unwrap()
    }

    fn quote(symbol: &str, second: u32, bid: f64, ask: f64) -> PriceQuote {
        PriceQuote {
            symbol: symbol.into(),
            ts: ts(second),
            bid,
            ask,
        }
    }

    fn position(id: &str, symbol: &str, side: Side, entry: f64, size: f64) -> OpenPositionSnapshot {
        OpenPositionSnapshot::new(id, symbol, side, entry, size)
    }

    fn eur_account_plan() -> RunCurrencyPlan {
        RunCurrencyPlan::new(
            "USD",
            ["S".to_owned()].into_iter().collect(),
            ["EURUSD".to_owned()].into_iter().collect(),
            [("S".to_owned(), "EUR".to_owned())].into_iter().collect(),
            [(
                "EUR".to_owned(),
                ConversionRoute::Direct {
                    pair: FxPair {
                        symbol: "EURUSD".to_owned(),
                        base_currency: "EUR".to_owned(),
                        quote_currency: "USD".to_owned(),
                    },
                },
            )]
            .into_iter()
            .collect(),
            Vec::new(),
        )
        .unwrap()
    }

    #[test]
    fn marks_longs_at_bid_and_shorts_at_ask_with_contract_sizes() {
        let mut recorder = PortfolioRecorder::new(
            1_000.0,
            [("LONG".to_owned(), 10.0), ("SHORT".to_owned(), 10.0)],
        );
        recorder.record_quote(quote("LONG", 0, 105.0, 106.0));
        recorder.record_quote(quote("SHORT", 0, 90.0, 91.0));

        let point = recorder.record(
            ts(0),
            [
                position("long", "LONG", Side::Buy, 100.0, 2.0),
                position("short", "SHORT", Side::Sell, 100.0, 1.0),
            ],
        );

        // Long: (bid 105 - 100) * 2 * 10 = 100.
        // Short: (100 - ask 91) * 1 * 10 = 90.
        assert_eq!(point.unrealized_pnl, Some(190.0));
        assert_eq!(point.equity, Some(1_190.0));
        let marked = recorder.latest_open_positions();
        assert_eq!(marked[0].mark_price, Some(105.0));
        assert_eq!(marked[1].mark_price, Some(91.0));
    }

    #[test]
    fn converts_signed_marks_and_stop_liability_before_aggregation() {
        let plan = eur_account_plan();
        let mut conversions = ConversionQuoteBook::new(Duration::hours(1)).unwrap();
        conversions
            .record_canonical_tick(quote("EURUSD", 0, 2.0, 3.0))
            .unwrap();
        let mut recorder =
            PortfolioRecorder::new(1_000.0, BTreeMap::new()).with_currency_plan(Some(plan));
        recorder.record_quote(quote("S", 0, 100.0, 100.0));
        let mut open = position("p", "S", Side::Buy, 110.0, 1.0);
        open.effective_stop = Some(EffectiveStop::new(90.0, StopOrigin::Initial));

        let point = recorder.record_with_currency(ts(0), [open], Some(&conversions));

        assert_eq!(point.unrealized_pnl, Some(-30.0));
        assert_eq!(point.gross_exposure, Some(200.0));
        assert_eq!(point.open_risk, Some(30.0));
        assert_eq!(point.equity, Some(970.0));
        let marked = &recorder.latest_open_positions()[0];
        assert_eq!(marked.native_unrealized_pnl, Some(-10.0));
        assert_eq!(marked.native_signed_exposure, Some(100.0));
        assert_eq!(marked.native_open_risk, Some(10.0));
        assert_eq!(marked.native_currency.as_deref(), Some("EUR"));
        assert_eq!(marked.account_currency.as_deref(), Some("USD"));
        assert_eq!(
            marked.unrealized_pnl_conversion.as_ref().unwrap().legs[0].price_side,
            ConversionPriceSide::Ask
        );
        assert_eq!(
            marked.gross_exposure_conversion.as_ref().unwrap().legs[0].price_side,
            ConversionPriceSide::Bid
        );
        assert_eq!(
            marked.open_risk_conversion.as_ref().unwrap().legs[0].price_side,
            ConversionPriceSide::Ask
        );
    }

    #[test]
    fn missing_conversion_retains_native_marks_and_unavailable_aggregates() {
        let plan = eur_account_plan();
        let conversions = ConversionQuoteBook::new(Duration::hours(1)).unwrap();
        let mut recorder =
            PortfolioRecorder::new(1_000.0, BTreeMap::new()).with_currency_plan(Some(plan));
        recorder.record_quote(quote("S", 0, 100.0, 100.0));
        let mut open = position("p", "S", Side::Buy, 110.0, 1.0);
        open.effective_stop = Some(EffectiveStop::new(90.0, StopOrigin::Initial));

        let point = recorder.record_with_currency(ts(0), [open], Some(&conversions));

        assert_eq!(point.cash_balance, Some(1_000.0));
        assert_eq!(point.unrealized_pnl, None);
        assert_eq!(point.equity, None);
        assert_eq!(point.gross_exposure, None);
        assert_eq!(point.open_risk, None);
        assert_eq!(point.unpriced_position_count, 1);
        assert_eq!(point.unavailable_open_risk_count, 1);
        let marked = &recorder.latest_open_positions()[0];
        assert_eq!(marked.native_unrealized_pnl, Some(-10.0));
        assert_eq!(marked.native_signed_exposure, Some(100.0));
        assert_eq!(marked.native_open_risk, Some(10.0));
        assert_eq!(marked.unrealized_pnl, None);
        assert_eq!(marked.gross_exposure, None);
        assert_eq!(marked.open_risk, None);
    }

    #[test]
    fn combines_realized_and_unrealized_equity() {
        let mut recorder = PortfolioRecorder::new(1_000.0, BTreeMap::new());
        assert!(recorder.add_realized_pnl(25.0));
        recorder.record_quote(quote("S", 0, 12.0, 13.0));
        let point = recorder.record(ts(0), [position("p", "S", Side::Buy, 10.0, 2.0)]);

        assert_eq!(point.realized_pnl, Some(25.0));
        assert_eq!(point.cash_balance, Some(1_025.0));
        assert_eq!(point.unrealized_pnl, Some(4.0));
        assert_eq!(point.equity, Some(1_029.0));
    }

    #[test]
    fn missing_quote_makes_aggregate_mark_values_unavailable() {
        let mut recorder = PortfolioRecorder::new(1_000.0, BTreeMap::new());
        recorder.record_quote(quote("PRICED", 0, 11.0, 12.0));
        let point = recorder.record(
            ts(0),
            [
                position("p1", "PRICED", Side::Buy, 10.0, 1.0),
                position("p2", "MISSING", Side::Buy, 10.0, 1.0),
            ],
        );

        assert_eq!(point.cash_balance, Some(1_000.0));
        assert_eq!(point.unrealized_pnl, None);
        assert_eq!(point.equity, None);
        assert_eq!(point.gross_exposure, None);
        assert_eq!(point.drawdown, None);
        assert_eq!(point.unpriced_position_count, 1);
        assert_eq!(recorder.latest_open_positions()[1].mark_price, None);
    }

    #[test]
    fn stale_quotes_are_counted_but_still_marked() {
        let mut recorder = PortfolioRecorder::new(1_000.0, BTreeMap::new())
            .with_stale_quote_after_millis(Some(1_000));
        recorder.record_quote(quote("S", 0, 11.0, 12.0));
        let point = recorder.record(ts(2), [position("p", "S", Side::Buy, 10.0, 1.0)]);

        assert_eq!(point.stale_position_count, 1);
        assert_eq!(point.unpriced_position_count, 0);
        assert_eq!(point.equity, Some(1_001.0));
    }

    #[test]
    fn online_drawdown_uses_initial_balance_and_prior_peaks() {
        let mut recorder = PortfolioRecorder::new(1_000.0, BTreeMap::new());
        recorder.record_quote(quote("S", 0, 1_010.0, 1_011.0));
        let high = recorder.record(ts(0), [position("p", "S", Side::Buy, 1_000.0, 1.0)]);
        assert_eq!(high.equity, Some(1_010.0));
        assert_eq!(high.drawdown, Some(0.0));

        recorder.record_quote(quote("S", 1, 990.0, 991.0));
        let low = recorder.record(ts(1), [position("p", "S", Side::Buy, 1_000.0, 1.0)]);
        assert_eq!(low.equity, Some(990.0));
        assert_eq!(low.drawdown, Some(20.0));
        assert_eq!(low.max_drawdown, Some(20.0));
        assert!((low.drawdown_pct.unwrap() - 20.0 / 1_010.0).abs() < 1.0e-12);
        assert_eq!(recorder.max_drawdown(), Some(20.0));
    }

    #[test]
    fn tracks_campaign_mae_mfe_across_marks_and_partial_realization() {
        let mut recorder = PortfolioRecorder::new(1_000.0, BTreeMap::new());
        recorder.record_quote(quote("S", 0, 95.0, 96.0));
        recorder.record(ts(0), [position("p", "S", Side::Buy, 100.0, 1.0)]);
        assert_eq!(recorder.campaign_excursion("p").unwrap().mae, -5.0);

        recorder.record_quote(quote("S", 1, 110.0, 111.0));
        let mut partially_closed = position("p", "S", Side::Buy, 100.0, 1.0);
        partially_closed.realized_pnl = 10.0;
        recorder.record(ts(1), [partially_closed]);
        let campaign = recorder.campaign_excursion("p").unwrap();
        assert_eq!(campaign.mae, -5.0);
        assert_eq!(campaign.mfe, 20.0);
        assert_eq!(campaign.observations, 2);

        let finished = recorder.finish_campaign("p", -8.0).unwrap();
        assert_eq!(finished.mae, -8.0);
        assert_eq!(finished.mfe, 20.0);
        assert!(recorder.campaign_excursion("p").is_none());
    }

    #[test]
    fn computes_concurrent_exposure_and_open_risk_to_effective_stops() {
        let mut recorder = PortfolioRecorder::new(1_000.0, [("S".to_owned(), 10.0)]);
        recorder.record_quote(quote("S", 0, 105.0, 106.0));
        let mut open = position("p", "S", Side::Buy, 100.0, 2.0);
        open.effective_stop = Some(EffectiveStop::new(95.0, StopOrigin::Initial));

        let point = recorder.record(ts(0), [open]);
        assert_eq!(point.gross_exposure, Some(2_100.0));
        assert_eq!(point.open_risk, Some(200.0));
        assert_eq!(point.unavailable_open_risk_count, 0);
    }

    #[test]
    fn missing_stop_makes_aggregate_open_risk_explicitly_unavailable() {
        let mut recorder = PortfolioRecorder::new(1_000.0, BTreeMap::new());
        recorder.record_quote(quote("S", 0, 105.0, 106.0));
        let point = recorder.record(ts(0), [position("p", "S", Side::Buy, 100.0, 1.0)]);

        assert_eq!(point.open_risk, None);
        assert_eq!(point.unavailable_open_risk_count, 1);
        assert_eq!(point.gross_exposure, Some(105.0));
    }

    #[test]
    fn ignores_older_quotes_and_rejects_non_finite_account_updates() {
        let mut recorder = PortfolioRecorder::new(1_000.0, BTreeMap::new());
        assert!(recorder.record_quote(quote("S", 2, 12.0, 13.0)));
        assert!(!recorder.record_quote(quote("S", 1, 99.0, 100.0)));
        assert_eq!(recorder.quote("S").unwrap().bid, 12.0);
        assert!(!recorder.add_realized_pnl(f64::NAN));
        assert!(!recorder.set_contract_size("S", 0.0));
    }

    #[test]
    fn observe_updates_exact_state_without_appending_to_the_curve() {
        let mut recorder = PortfolioRecorder::new(1_000.0, BTreeMap::new());
        recorder.record_quote(quote("S", 0, 90.0, 91.0));

        let observed = recorder.observe(ts(0), [position("p", "S", Side::Buy, 100.0, 1.0)]);

        assert_eq!(observed.equity, Some(990.0));
        assert_eq!(observed.drawdown, Some(10.0));
        assert_eq!(recorder.max_drawdown(), Some(10.0));
        assert_eq!(recorder.latest_open_positions()[0].mark_price, Some(90.0));
        assert_eq!(recorder.campaign_excursion("p").unwrap().mae, -10.0);
        assert!(recorder.equity_curve().is_empty());

        recorder.record_quote(quote("S", 1, 95.0, 96.0));
        recorder.record(ts(1), [position("p", "S", Side::Buy, 100.0, 1.0)]);
        assert_eq!(recorder.equity_curve().len(), 1);
        assert_eq!(recorder.campaign_excursion("p").unwrap().observations, 2);
    }

    #[test]
    fn equity_point_serde_defaults_keep_old_payloads_readable() {
        let point: EquityPoint = serde_json::from_str(r#"{"ts":"2026-03-04T05:06:00"}"#).unwrap();
        assert_eq!(point.observation_kind, None);
        assert_eq!(point.observation_sequence, None);
        assert_eq!(point.equity, None);
        assert_eq!(point.stale_position_count, 0);
        assert_eq!(point.unpriced_position_count, 0);
    }
}
