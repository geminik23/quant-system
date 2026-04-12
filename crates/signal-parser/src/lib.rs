//! Per-channel Telegram signal parser for the quant-system workspace.

pub mod config;
pub mod error;
pub mod parser;
pub mod pipeline;
pub mod registry;
pub mod template;
pub mod types;

pub use config::load_parsers;
pub use error::SignalParserError;
pub use parser::ChannelParser;
pub use pipeline::{parse_messages, parse_messages_with_context};
pub use registry::ParserRegistry;
pub use template::TemplateParser;
pub use types::{LlmClient, MarketQuote, ParseContext, ParsedAction, RawTgMessage};
