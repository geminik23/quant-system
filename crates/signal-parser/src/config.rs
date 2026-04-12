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
    #[serde(default = "default_size")]
    default_size: f64,
    /// Optional group prefix override; defaults to `tg_{name}` when absent.
    group_prefix: Option<String>,
}

fn default_size() -> f64 {
    0.01
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
        match def.parser_type.as_str() {
            "template" => {
                let parser = TemplateParser::new(
                    def.name,
                    def.channel_ids,
                    def.default_size,
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

    #[test]
    fn load_parsers_from_toml_string() {
        let toml = r#"
[[parser]]
name = "wave-trader"
type = "template"
channel_ids = [2331249584]
default_size = 0.01

[[parser]]
name = "gold-signals"
type = "template"
channel_ids = [1890843109, 1735292110]
default_size = 0.05
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
default_size = 0.02
group_prefix = "my_group"
"#;
        let registry = load_parsers_from_str(toml).expect("should parse config");
        assert!(registry.has_parser(42));
    }
}
