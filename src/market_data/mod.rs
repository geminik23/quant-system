use crate::core::Id;

pub mod ctrader_market;
pub mod market_handler;
pub mod market_manager;
pub mod price_alert;
pub mod utils;

pub type AlertId = Id;

// Re-export commonly used types
pub use market_manager::ConnectionState;
