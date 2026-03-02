//! Strategy trait for strategy-driven backtests.
//!
//! Implement [`Strategy`] to define custom trading logic that reacts to
//! market events and produces [`Action`]s for the engine to process.

use qs_core::types::Action;

use crate::data_feed::MarketEvent;

/// A trading strategy that reacts to market events.
///
/// The backtest runner calls [`on_event`](Strategy::on_event) for every
/// market event (tick or bar) in the data feed.  The strategy inspects the
/// event and returns zero or more [`Action`]s that the engine will process.
///
/// # Example
///
/// ```ignore
/// use qs_backtest::{Strategy, MarketEvent};
/// use qs_core::types::{Action, OrderType, Side};
///
/// struct BuyAndHold { entered: bool }
///
/// impl Strategy for BuyAndHold {
///     fn on_event(&mut self, event: &MarketEvent) -> Vec<Action> {
///         if self.entered { return vec![]; }
///         if let MarketEvent::Tick { symbol, ask, .. } = event {
///             self.entered = true;
///             return vec![Action::Open {
///                 symbol: symbol.clone(),
///                 side: Side::Buy,
///                 order_type: OrderType::Market,
///                 price: Some(*ask),
///                 size: 1.0,
///                 stoploss: None,
///                 targets: vec![],
///                 rules: vec![],
///                 group: None,
///             }];
///         }
///         vec![]
///     }
///
///     fn on_finished(&mut self) -> Vec<Action> {
///         vec![Action::CloseAll]
///     }
/// }
/// ```
pub trait Strategy {
    /// Called for every market event in the data feed.
    ///
    /// Return an empty `Vec` to take no action on this event.
    fn on_event(&mut self, event: &MarketEvent) -> Vec<Action>;

    /// Called once after the data feed is exhausted.
    ///
    /// Use this to emit final actions such as closing all remaining
    /// positions.  The default implementation does nothing.
    fn on_finished(&mut self) -> Vec<Action> {
        vec![]
    }
}
