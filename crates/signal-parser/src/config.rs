use serde::Deserialize;

use crate::error::SignalParserError;
use crate::registry::ParserRegistry;
use crate::template::TemplateParser;

/// Top-level TOML config structure.
#[derive(Debug, Deserialize)]
struct ParsersConfig {
    parser: Vec<ParserDef>,
}

/// A single parser definition from TOML.
#[derive(Debug, Deserialize)]
struct ParserDef {
    name: String,
    #[serde(rename = "type")]
    parser_type: String,
    channel_ids: Vec<i64>,
    #[serde(default = "default_risk_multiplier")]
    default_risk_multiplier: f64,
    /// Optional group prefix override; defaults to `tg_{name}` when absent.
    group_prefix: Option<String>,
}

fn default_risk_multiplier() -> f64 {
    1.0
}

/// Load parser registry from a TOML config file.
pub fn load_parsers(path: &str) -> Result<ParserRegistry, SignalParserError> {
    let content = std::fs::read_to_string(path).map_err(SignalParserError::Io)?;
    load_parsers_from_str(&content)
}

/// Load parser registry from a TOML string (useful for testing).
pub fn load_parsers_from_str(toml_str: &str) -> Result<ParserRegistry, SignalParserError> {
    let config: ParsersConfig =
        toml::from_str(toml_str).map_err(|e| SignalParserError::Config(e.to_string()))?;

    let mut registry = ParserRegistry::new();

    for def in config.parser {
        if !def.default_risk_multiplier.is_finite() || def.default_risk_multiplier <= 0.0 {
            return Err(SignalParserError::Config(format!(
                "parser '{}': default_risk_multiplier must be finite and greater than zero, got {}",
                def.name, def.default_risk_multiplier
            )));
        }

        match def.parser_type.as_str() {
            "template" => {
                let parser = TemplateParser::new(
                    def.name,
                    def.channel_ids,
                    def.default_risk_multiplier,
                    def.group_prefix,
                );
                registry.register(Box::new(parser));
            }
            other => {
                return Err(SignalParserError::Config(format!(
                    "unknown parser type: '{other}'"
                )));
            }
        }
    }

    Ok(registry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ParseContext, ParsedAction};
    use qs_core::RawSignal;

    fn parsed_risk(registry: &ParserRegistry, channel_id: i64) -> f64 {
        let ts = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap();
        let action = registry.get(channel_id).unwrap().parse_root(
            "EURUSD BUY NOW SL 1.08 TP 1.09",
            ts,
            &ParseContext::empty(),
        );
        match action {
            ParsedAction::Signals(signals) => match &signals[0] {
                RawSignal::Entry {
                    risk_multiplier, ..
                } => *risk_multiplier,
                _ => panic!("expected entry"),
            },
            _ => panic!("expected signals"),
        }
    }

    #[test]
    fn load_parsers_from_toml_string() {
        let toml = r#"
[[parser]]
name = "wave-trader"
type = "template"
channel_ids = [2331249584]
default_risk_multiplier = 0.5

[[parser]]
name = "gold-signals"
type = "template"
channel_ids = [1890843109, 1735292110]
default_risk_multiplier = 1.25
"#;

        let registry = load_parsers_from_str(toml).expect("should parse config");
        assert!(registry.has_parser(2331249584));
        assert!(registry.has_parser(1890843109));
        assert!(registry.has_parser(1735292110));

        let mut names = registry.names();
        names.sort();
        assert!(names.contains(&"wave-trader"));
        assert!(names.contains(&"gold-signals"));

        let gold_ids = registry.ids_for_name("gold-signals").unwrap();
        assert_eq!(gold_ids.len(), 2);
        assert_eq!(parsed_risk(&registry, 2331249584), 0.5);
        assert_eq!(parsed_risk(&registry, 1890843109), 1.25);
    }

    #[test]
    fn omitted_risk_multiplier_defaults_to_one() {
        let toml = r#"
[[parser]]
name = "default-risk"
type = "template"
channel_ids = [7]
"#;
        let registry = load_parsers_from_str(toml).unwrap();
        assert_eq!(parsed_risk(&registry, 7), 1.0);
    }

    #[test]
    fn invalid_default_risk_multiplier_errors() {
        for value in ["0.0", "-1.0", "nan", "inf"] {
            let toml = format!(
                r#"
[[parser]]
name = "invalid-risk"
type = "template"
channel_ids = [8]
default_risk_multiplier = {value}
"#
            );
            let error = load_parsers_from_str(&toml).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("must be finite and greater than zero")
            );
        }
    }

    #[test]
    fn unknown_parser_type_errors() {
        let toml = r#"
[[parser]]
name = "bad"
type = "unknown_type"
channel_ids = [123]
"#;
        let result = load_parsers_from_str(toml);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown parser type"));
    }

    #[test]
    fn custom_group_prefix() {
        let toml = r#"
[[parser]]
name = "custom"
type = "template"
channel_ids = [42]
default_risk_multiplier = 0.75
group_prefix = "my_group"
"#;
        let registry = load_parsers_from_str(toml).expect("should parse config");
        assert!(registry.has_parser(42));
    }
}
