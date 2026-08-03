//! Source-neutral event contracts and Telegram signal parsing for the quant-system workspace.
//!
//! The [`ingestion`] module defines bounded source facts before parsing. Existing Telegram
//! parsers, runners, artifacts, and direct normalized-signal inputs remain separate.

pub mod config;
pub mod error;
pub mod handler;
pub mod ingestion;
pub mod offline;
pub mod parser;
pub mod pipeline;
pub mod registry;
pub mod template;
pub mod types;

#[cfg(feature = "online")]
pub mod online;

pub use config::load_parsers;
pub use error::SignalParserError;
pub use handler::{LoggingHandler, NoopHandler, SignalContext, SignalHandler};
pub use offline::{OfflineArgs, OfflineRunner};
pub use parser::ChannelParser;
pub use pipeline::{
    parse_messages, parse_messages_v2, parse_messages_with_context, parse_messages_with_context_v2,
};
pub use qs_core::{PositionRef, RawSignal};
pub use registry::ParserRegistry;
pub use template::TemplateParser;
pub use types::{
    LlmClient, MarketQuote, MessageParseOutcome, ParseBatchResult, ParseContext, ParseFailure,
    ParsedAction, RawTgMessage, SkipReason,
};

#[cfg(feature = "online")]
pub use online::OnlineServer;
