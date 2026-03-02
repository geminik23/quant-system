//! Symbol normalization utilities.
//!
//! This module re-exports [`qs_symbols::SymbolRegistry`] and provides thin
//! wrappers around it for backward compatibility. The hardcoded
//! `convert_symbol()` / `symbol_info()` functions are replaced by registry
//! lookups.

pub use qs_symbols::{SymbolRegistry, SymbolSpec};

/// Strip `/`, `-`, `_`, and spaces then lowercase — basic normalization
/// without alias resolution.
pub fn convert_symbol_to_norm(mut symbol: String) -> String {
    symbol = symbol.to_lowercase();
    symbol.retain(|c| !matches!(c, '/' | '-' | '_' | ' '));
    symbol
}

/// Normalize a symbol string using a [`SymbolRegistry`], falling back to
/// basic stripping when no registry is available.
pub fn convert_symbol_with_registry(symbol: &str, registry: Option<&SymbolRegistry>) -> String {
    match registry {
        Some(reg) => reg.normalize_or_passthrough(symbol),
        None => convert_symbol_to_norm(symbol.to_string()),
    }
}

/// Legacy `convert_symbol` — kept for call-sites that don't yet have access
/// to a registry instance. Handles the small set of aliases that were
/// previously hardcoded.
pub fn convert_symbol(symbol: &str) -> String {
    let mut symbol = symbol.to_lowercase();
    symbol = match symbol.trim() {
        "spx500" => "us500",
        "nas100" => "us100",
        "ger30" => "de40",
        "ger40" => "de40",
        "de30" => "de40",
        "nasdaq" => "us100",
        "gold" => "xauusd",
        "silver" => "xagusd",
        "oil" | "usoil" => "xtiusd",
        sym => sym,
    }
    .to_string();
    symbol
}
