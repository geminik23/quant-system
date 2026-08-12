//! Source-neutral event, normalization, and durable ingestion-state contracts with Telegram compatibility parsing.
//!
//! The [`ingestion`] module defines bounded source facts before parsing, [`normalization`] provides deterministic routing and typed stateless pipelines, [`state`] owns restart-safe application, committed lifecycle, checkpoints, and publication outbox state, and [`adapters`] contains source-specific adaptation outside those neutral contracts. Existing Telegram runners, artifacts, and direct normalized-signal inputs remain separate compatibility surfaces.

pub mod adapters;
pub mod ingestion;
pub mod normalization;
pub mod state;

#[cfg(feature = "neutral-runner")]
pub mod runner;

#[cfg(feature = "telegram-compat")]
pub mod config;
#[cfg(feature = "telegram-compat")]
pub mod error;
#[cfg(feature = "telegram-compat")]
pub mod handler;
#[cfg(feature = "telegram-compat")]
pub mod offline;
#[cfg(feature = "telegram-compat")]
pub mod parser;
#[cfg(feature = "telegram-compat")]
pub mod pipeline;
#[cfg(feature = "telegram-compat")]
pub mod registry;
#[cfg(feature = "telegram-compat")]
pub mod template;
#[cfg(feature = "telegram-compat")]
pub mod types;

#[cfg(feature = "online")]
pub mod online;

#[cfg(feature = "telegram-compat")]
pub use config::load_parsers;
#[cfg(feature = "telegram-compat")]
pub use error::SignalParserError;
#[cfg(feature = "telegram-compat")]
pub use handler::{LoggingHandler, NoopHandler, SignalContext, SignalHandler};
#[cfg(feature = "telegram-compat")]
pub use offline::{OfflineArgs, OfflineRunner};
#[cfg(feature = "telegram-compat")]
pub use parser::ChannelParser;
#[cfg(feature = "telegram-compat")]
pub use pipeline::{
    parse_messages, parse_messages_v2, parse_messages_with_context, parse_messages_with_context_v2,
};
pub use qs_core::{PositionRef, RawSignal};
#[cfg(feature = "telegram-compat")]
pub use registry::ParserRegistry;
#[cfg(feature = "telegram-compat")]
pub use template::TemplateParser;
#[cfg(feature = "telegram-compat")]
pub use types::{
    LlmClient, MarketQuote, MessageParseOutcome, ParseBatchResult, ParseContext, ParseFailure,
    ParsedAction, RawTgMessage, SkipReason,
};

#[cfg(feature = "online")]
pub use online::OnlineServer;
