//! Error types for the symbol registry.

/// All errors that can occur when loading or querying the symbol registry.
#[derive(Debug, thiserror::Error)]
pub enum SymbolError {
    #[error("Failed to read symbol file: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to parse symbol TOML: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("Duplicate alias '{alias}' — maps to both '{existing}' and '{new}'")]
    DuplicateAlias {
        alias: String,
        existing: String,
        new: String,
    },

    #[error("Duplicate canonical name: '{0}'")]
    DuplicateCanonical(String),

    #[error("Symbol not found: '{0}'")]
    NotFound(String),

    #[error("Invalid lot spec for '{symbol}': lot_step_units ({step}) must be > 0 and <= lot_base_units ({base})")]
    InvalidLotSpec {
        symbol: String,
        step: i64,
        base: i64,
    },

    #[error("Invalid digits for '{symbol}': pip_position ({pip}) must be <= digits ({digits})")]
    InvalidDigits {
        symbol: String,
        pip: u16,
        digits: u16,
    },
}
