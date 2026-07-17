//! Pure, deterministic execution pricing.
//!
//! This module is intentionally independent from `TradeEngine`: callers can
//! price a fill from a quote without mutating positions or engine state.

use thiserror::Error;

use crate::types::{
    ExecutionConvention, ExecutionFill, ExecutionModel, FillPurpose, PriceQuote, Side,
};

/// Errors produced while validating an execution pricing request.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum ExecutionError {
    #[error("quote bid must be finite and positive, got {0}")]
    InvalidBid(f64),
    #[error("quote ask must be finite and positive, got {0}")]
    InvalidAsk(f64),
    #[error("crossed quote: bid {bid} is greater than ask {ask}")]
    CrossedQuote { bid: f64, ask: f64 },
    #[error("pip_size must be finite and positive, got {0}")]
    InvalidPipSize(f64),
    #[error("slippage pips must be finite, got {0}")]
    InvalidSlippage(f64),
    #[error("{0:?} requires a requested price")]
    MissingRequestedPrice(FillPurpose),
    #[error("requested price must be finite and positive, got {0}")]
    InvalidRequestedPrice(f64),
}

pub type ExecutionResult<T> = std::result::Result<T, ExecutionError>;

/// Pure execution-price calculator configured by an [`ExecutionModel`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExecutionPricer {
    model: ExecutionModel,
}

impl ExecutionPricer {
    pub const fn new(model: ExecutionModel) -> Self {
        Self { model }
    }

    pub const fn model(&self) -> ExecutionModel {
        self.model
    }

    /// Validate a quote independently of pricing it.
    pub fn validate_quote(quote: &PriceQuote) -> ExecutionResult<()> {
        if !quote.bid.is_finite() || quote.bid <= 0.0 {
            return Err(ExecutionError::InvalidBid(quote.bid));
        }
        if !quote.ask.is_finite() || quote.ask <= 0.0 {
            return Err(ExecutionError::InvalidAsk(quote.ask));
        }
        if quote.bid > quote.ask {
            return Err(ExecutionError::CrossedQuote {
                bid: quote.bid,
                ask: quote.ask,
            });
        }
        Ok(())
    }

    /// Price one fill. Market purposes ignore `requested_price`; all other
    /// purposes require it.
    pub fn price(
        &self,
        purpose: FillPurpose,
        side: Side,
        quote: &PriceQuote,
        requested_price: Option<f64>,
        pip_size: f64,
    ) -> ExecutionResult<ExecutionFill> {
        Self::validate_quote(quote)?;
        if !pip_size.is_finite() || pip_size <= 0.0 {
            return Err(ExecutionError::InvalidPipSize(pip_size));
        }

        let slippage_pips = self.model.slippage.pips();
        if !slippage_pips.is_finite() {
            return Err(ExecutionError::InvalidSlippage(slippage_pips));
        }

        let requested_price = if purpose.requires_requested_price() {
            let requested =
                requested_price.ok_or(ExecutionError::MissingRequestedPrice(purpose))?;
            if !requested.is_finite() || requested <= 0.0 {
                return Err(ExecutionError::InvalidRequestedPrice(requested));
            }
            Some(requested)
        } else {
            None
        };

        let quote_price = if purpose.is_entry() {
            quote.fill_price(side, self.model.fill_model)
        } else {
            quote.eval_price(side, self.model.fill_model)
        };

        let price = match self.model.convention {
            ExecutionConvention::Legacy => {
                let base = requested_price.unwrap_or(quote_price);
                apply_slippage(base, purpose, side, slippage_pips, pip_size)
            }
            ExecutionConvention::FutureQuoteV1 => {
                let slipped_quote =
                    apply_slippage(quote_price, purpose, side, slippage_pips, pip_size);
                future_quote_price(purpose, side, slipped_quote, requested_price)
            }
        };

        Ok(ExecutionFill {
            purpose,
            side,
            price,
            quote_price,
            requested_price,
            slippage_pips,
        })
    }

    pub fn market_entry(
        &self,
        side: Side,
        quote: &PriceQuote,
        pip_size: f64,
    ) -> ExecutionResult<ExecutionFill> {
        self.price(FillPurpose::MarketEntry, side, quote, None, pip_size)
    }

    pub fn market_exit(
        &self,
        side: Side,
        quote: &PriceQuote,
        pip_size: f64,
    ) -> ExecutionResult<ExecutionFill> {
        self.price(FillPurpose::MarketExit, side, quote, None, pip_size)
    }

    pub fn limit_entry(
        &self,
        side: Side,
        quote: &PriceQuote,
        limit_price: f64,
        pip_size: f64,
    ) -> ExecutionResult<ExecutionFill> {
        self.price(
            FillPurpose::LimitEntry,
            side,
            quote,
            Some(limit_price),
            pip_size,
        )
    }

    pub fn stop_entry(
        &self,
        side: Side,
        quote: &PriceQuote,
        stop_price: f64,
        pip_size: f64,
    ) -> ExecutionResult<ExecutionFill> {
        self.price(
            FillPurpose::StopEntry,
            side,
            quote,
            Some(stop_price),
            pip_size,
        )
    }

    pub fn stop_loss(
        &self,
        side: Side,
        quote: &PriceQuote,
        stop_price: f64,
        pip_size: f64,
    ) -> ExecutionResult<ExecutionFill> {
        self.price(
            FillPurpose::StopLoss,
            side,
            quote,
            Some(stop_price),
            pip_size,
        )
    }

    pub fn take_profit(
        &self,
        side: Side,
        quote: &PriceQuote,
        target_price: f64,
        pip_size: f64,
    ) -> ExecutionResult<ExecutionFill> {
        self.price(
            FillPurpose::TakeProfit,
            side,
            quote,
            Some(target_price),
            pip_size,
        )
    }
}

impl Default for ExecutionPricer {
    fn default() -> Self {
        Self::new(ExecutionModel::default())
    }
}

fn apply_slippage(
    price: f64,
    purpose: FillPurpose,
    side: Side,
    signed_pips: f64,
    pip_size: f64,
) -> f64 {
    // Positive pips always move against the position: entries cost more and
    // exits realize less. A negative value reverses the movement.
    let adverse_sign = match (purpose.is_entry(), side) {
        (true, Side::Buy) | (false, Side::Sell) => 1.0,
        (true, Side::Sell) | (false, Side::Buy) => -1.0,
    };
    price + adverse_sign * signed_pips * pip_size
}

fn future_quote_price(
    purpose: FillPurpose,
    side: Side,
    slipped_quote: f64,
    requested_price: Option<f64>,
) -> f64 {
    let Some(requested) = requested_price else {
        return slipped_quote;
    };

    match (purpose, side) {
        // A limit is never filled worse than its cap, but a better quote is kept.
        (FillPurpose::LimitEntry, Side::Buy) => slipped_quote.min(requested),
        (FillPurpose::LimitEntry, Side::Sell) => slipped_quote.max(requested),

        // Stop orders keep an adverse opening gap and never manufacture a
        // favorable fill beyond the trigger.
        (FillPurpose::StopEntry, Side::Buy) => slipped_quote.max(requested),
        (FillPurpose::StopEntry, Side::Sell) => slipped_quote.min(requested),

        // A protective stop keeps an adverse closing gap.
        (FillPurpose::StopLoss, Side::Buy) => slipped_quote.min(requested),
        (FillPurpose::StopLoss, Side::Sell) => slipped_quote.max(requested),

        // A take-profit behaves as a price-protected exit: keep favorable gap
        // improvement, but never fill worse than the target.
        (FillPurpose::TakeProfit, Side::Buy) => slipped_quote.max(requested),
        (FillPurpose::TakeProfit, Side::Sell) => slipped_quote.min(requested),

        (FillPurpose::MarketEntry | FillPurpose::MarketExit, _) => slipped_quote,
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;
    use crate::types::{FillModel, SlippageModel};

    const PIP: f64 = 0.0001;

    fn quote(bid: f64, ask: f64) -> PriceQuote {
        PriceQuote {
            symbol: "EURUSD".into(),
            ts: NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
            bid,
            ask,
        }
    }

    fn future(fill_model: FillModel) -> ExecutionPricer {
        ExecutionPricer::new(ExecutionModel::future_quote_v1(fill_model))
    }

    fn with_slippage(pips: f64) -> ExecutionPricer {
        ExecutionPricer::new(ExecutionModel::new(
            ExecutionConvention::FutureQuoteV1,
            FillModel::BidAsk,
            SlippageModel::FixedPips { pips },
        ))
    }

    fn assert_price(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn defaults_preserve_legacy_bid_ask_without_slippage() {
        let model = ExecutionModel::default();
        assert_eq!(model.convention, ExecutionConvention::Legacy);
        assert_eq!(model.fill_model, FillModel::BidAsk);
        assert_eq!(model.slippage, SlippageModel::None);
        assert_eq!(ExecutionPricer::default().model(), model);
    }

    #[test]
    fn market_entry_uses_opening_side_for_both_sides() {
        let q = quote(1.1000, 1.1002);
        assert_price(
            future(FillModel::BidAsk)
                .market_entry(Side::Buy, &q, PIP)
                .unwrap()
                .price,
            1.1002,
        );
        assert_price(
            future(FillModel::BidAsk)
                .market_entry(Side::Sell, &q, PIP)
                .unwrap()
                .price,
            1.1000,
        );
    }

    #[test]
    fn market_exit_uses_closing_side_for_both_sides() {
        let q = quote(1.1000, 1.1002);
        assert_price(
            future(FillModel::BidAsk)
                .market_exit(Side::Buy, &q, PIP)
                .unwrap()
                .price,
            1.1000,
        );
        assert_price(
            future(FillModel::BidAsk)
                .market_exit(Side::Sell, &q, PIP)
                .unwrap()
                .price,
            1.1002,
        );
    }

    #[test]
    fn ask_only_and_mid_price_are_honored() {
        let q = quote(1.1000, 1.1004);
        for side in [Side::Buy, Side::Sell] {
            assert_price(
                future(FillModel::AskOnly)
                    .market_exit(side, &q, PIP)
                    .unwrap()
                    .price,
                1.1004,
            );
            assert_price(
                future(FillModel::MidPrice)
                    .market_entry(side, &q, PIP)
                    .unwrap()
                    .price,
                1.1002,
            );
        }
    }

    #[test]
    fn buy_limit_keeps_improvement_and_caps_adverse_price() {
        let pricer = future(FillModel::BidAsk);
        assert_price(
            pricer
                .limit_entry(Side::Buy, &quote(1.0988, 1.0990), 1.1000, PIP)
                .unwrap()
                .price,
            1.0990,
        );
        assert_price(
            pricer
                .limit_entry(Side::Buy, &quote(1.1000, 1.1002), 1.1000, PIP)
                .unwrap()
                .price,
            1.1000,
        );
    }

    #[test]
    fn sell_limit_keeps_improvement_and_caps_adverse_price() {
        let pricer = future(FillModel::BidAsk);
        assert_price(
            pricer
                .limit_entry(Side::Sell, &quote(1.1010, 1.1012), 1.1000, PIP)
                .unwrap()
                .price,
            1.1010,
        );
        assert_price(
            pricer
                .limit_entry(Side::Sell, &quote(1.0998, 1.1000), 1.1000, PIP)
                .unwrap()
                .price,
            1.1000,
        );
    }

    #[test]
    fn stop_entry_keeps_adverse_gaps_for_both_sides() {
        let pricer = future(FillModel::BidAsk);
        assert_price(
            pricer
                .stop_entry(Side::Buy, &quote(1.1008, 1.1010), 1.1000, PIP)
                .unwrap()
                .price,
            1.1010,
        );
        assert_price(
            pricer
                .stop_entry(Side::Sell, &quote(1.0990, 1.0992), 1.1000, PIP)
                .unwrap()
                .price,
            1.0990,
        );
    }

    #[test]
    fn stop_entry_does_not_manufacture_favorable_gap() {
        let pricer = future(FillModel::BidAsk);
        assert_price(
            pricer
                .stop_entry(Side::Buy, &quote(1.0996, 1.0998), 1.1000, PIP)
                .unwrap()
                .price,
            1.1000,
        );
        assert_price(
            pricer
                .stop_entry(Side::Sell, &quote(1.1002, 1.1004), 1.1000, PIP)
                .unwrap()
                .price,
            1.1000,
        );
    }

    #[test]
    fn stop_loss_keeps_adverse_gaps_for_both_sides() {
        let pricer = future(FillModel::BidAsk);
        assert_price(
            pricer
                .stop_loss(Side::Buy, &quote(1.0988, 1.0990), 1.1000, PIP)
                .unwrap()
                .price,
            1.0988,
        );
        assert_price(
            pricer
                .stop_loss(Side::Sell, &quote(1.1010, 1.1012), 1.1000, PIP)
                .unwrap()
                .price,
            1.1012,
        );
    }

    #[test]
    fn take_profit_keeps_favorable_gap_and_target_cap() {
        let pricer = future(FillModel::BidAsk);
        assert_price(
            pricer
                .take_profit(Side::Buy, &quote(1.1020, 1.1022), 1.1000, PIP)
                .unwrap()
                .price,
            1.1020,
        );
        assert_price(
            pricer
                .take_profit(Side::Buy, &quote(1.0998, 1.1000), 1.1000, PIP)
                .unwrap()
                .price,
            1.1000,
        );
        assert_price(
            pricer
                .take_profit(Side::Sell, &quote(1.0988, 1.0990), 1.1000, PIP)
                .unwrap()
                .price,
            1.0990,
        );
        assert_price(
            pricer
                .take_profit(Side::Sell, &quote(1.1000, 1.1002), 1.1000, PIP)
                .unwrap()
                .price,
            1.1000,
        );
    }

    #[test]
    fn legacy_non_market_fills_use_requested_price() {
        let pricer = ExecutionPricer::default();
        let q = quote(1.0988, 1.0990);
        assert_price(
            pricer
                .limit_entry(Side::Buy, &q, 1.1000, PIP)
                .unwrap()
                .price,
            1.1000,
        );
        assert_price(
            pricer
                .stop_entry(Side::Buy, &quote(1.1010, 1.1012), 1.1000, PIP)
                .unwrap()
                .price,
            1.1000,
        );
        assert_price(
            pricer.stop_loss(Side::Buy, &q, 1.1000, PIP).unwrap().price,
            1.1000,
        );
        assert_price(
            pricer
                .take_profit(Side::Buy, &quote(1.1020, 1.1022), 1.1000, PIP)
                .unwrap()
                .price,
            1.1000,
        );
    }

    #[test]
    fn adverse_slippage_moves_entries_against_both_sides() {
        let q = quote(1.1000, 1.1002);
        let pricer = with_slippage(2.0);
        assert_price(
            pricer.market_entry(Side::Buy, &q, PIP).unwrap().price,
            1.1004,
        );
        assert_price(
            pricer.market_entry(Side::Sell, &q, PIP).unwrap().price,
            1.0998,
        );
    }

    #[test]
    fn adverse_slippage_moves_exits_against_both_sides() {
        let q = quote(1.1000, 1.1002);
        let pricer = with_slippage(2.0);
        assert_price(
            pricer.market_exit(Side::Buy, &q, PIP).unwrap().price,
            1.0998,
        );
        assert_price(
            pricer.market_exit(Side::Sell, &q, PIP).unwrap().price,
            1.1004,
        );
    }

    #[test]
    fn favorable_slippage_reverses_the_adverse_direction() {
        let q = quote(1.1000, 1.1002);
        let pricer = with_slippage(-2.0);
        assert_price(
            pricer.market_entry(Side::Buy, &q, PIP).unwrap().price,
            1.1000,
        );
        assert_price(
            pricer.market_entry(Side::Sell, &q, PIP).unwrap().price,
            1.1002,
        );
        assert_price(
            pricer.market_exit(Side::Buy, &q, PIP).unwrap().price,
            1.1002,
        );
        assert_price(
            pricer.market_exit(Side::Sell, &q, PIP).unwrap().price,
            1.1000,
        );
    }

    #[test]
    fn limit_cap_is_preserved_after_adverse_slippage() {
        let q = quote(1.0998, 1.1000);
        let pricer = with_slippage(5.0);
        assert_price(
            pricer
                .limit_entry(Side::Buy, &q, 1.1002, PIP)
                .unwrap()
                .price,
            1.1002,
        );
    }

    #[test]
    fn execution_fill_reports_inputs_and_selected_quote() {
        let fill = with_slippage(1.5)
            .stop_loss(Side::Buy, &quote(1.0990, 1.0992), 1.1000, PIP)
            .unwrap();
        assert_eq!(fill.purpose, FillPurpose::StopLoss);
        assert_eq!(fill.side, Side::Buy);
        assert_eq!(fill.requested_price, Some(1.1000));
        assert_eq!(fill.quote_price, 1.0990);
        assert_eq!(fill.slippage_pips, 1.5);
    }

    #[test]
    fn rejects_non_finite_non_positive_and_crossed_quotes() {
        assert!(matches!(
            ExecutionPricer::validate_quote(&quote(f64::NAN, 1.0)),
            Err(ExecutionError::InvalidBid(_))
        ));
        assert_eq!(
            ExecutionPricer::validate_quote(&quote(1.0, 0.0)),
            Err(ExecutionError::InvalidAsk(0.0))
        );
        assert_eq!(
            ExecutionPricer::validate_quote(&quote(1.1, 1.0)),
            Err(ExecutionError::CrossedQuote { bid: 1.1, ask: 1.0 })
        );
    }

    #[test]
    fn rejects_invalid_pip_size_and_slippage() {
        let q = quote(1.0, 1.1);
        assert_eq!(
            future(FillModel::BidAsk).market_entry(Side::Buy, &q, 0.0),
            Err(ExecutionError::InvalidPipSize(0.0))
        );
        let pricer = with_slippage(f64::INFINITY);
        assert!(matches!(
            pricer.market_entry(Side::Buy, &q, PIP),
            Err(ExecutionError::InvalidSlippage(_))
        ));
    }

    #[test]
    fn validates_requested_price_only_when_required() {
        let q = quote(1.0, 1.1);
        let pricer = future(FillModel::BidAsk);
        assert_eq!(
            pricer.price(FillPurpose::LimitEntry, Side::Buy, &q, None, PIP),
            Err(ExecutionError::MissingRequestedPrice(
                FillPurpose::LimitEntry
            ))
        );
        assert_eq!(
            pricer.limit_entry(Side::Buy, &q, 0.0, PIP),
            Err(ExecutionError::InvalidRequestedPrice(0.0))
        );
        assert!(
            pricer
                .price(FillPurpose::MarketEntry, Side::Buy, &q, Some(f64::NAN), PIP)
                .is_ok()
        );
    }
}
