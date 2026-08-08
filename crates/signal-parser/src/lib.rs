//! Source-neutral event, normalization, and durable ingestion-state contracts with Telegram compatibility parsing.
//!
//! The [`ingestion`] module defines bounded source facts before parsing, [`normalization`] provides deterministic routing and typed stateless pipelines, [`state`] owns restart-safe application, committed lifecycle, checkpoints, and publication outbox state, and [`adapters`] contains source-specific adaptation outside those neutral contracts. Existing Telegram runners, artifacts, and direct normalized-signal inputs remain separate compatibility surfaces.

pub mod adapters;
pub mod config;
pub mod error;
pub mod handler;
pub mod ingestion;
pub mod normalization;
pub mod offline;
pub mod parser;
pub mod pipeline;
pub mod registry;
pub mod state;
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
