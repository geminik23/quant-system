//! Source-specific adapters that terminate before neutral runner ownership.

#[cfg(feature = "adapter-jsonl")]
pub mod structured_json;
#[cfg(feature = "telegram-compat")]
pub mod telegram;
pub mod webhook;
