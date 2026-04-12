use std::collections::HashMap;
use std::sync::Arc;

use crate::parser::ChannelParser;

/// Registry of channel parsers, keyed by channel_id.
pub struct ParserRegistry {
    parsers: HashMap<i64, Arc<dyn ChannelParser>>,
    names: HashMap<String, Vec<i64>>,
}

impl ParserRegistry {
    pub fn new() -> Self {
        Self {
            parsers: HashMap::new(),
            names: HashMap::new(),
        }
    }

    /// Register a parser for all its declared channel IDs.
    pub fn register(&mut self, parser: Box<dyn ChannelParser>) {
        let name = parser.name().to_string();
        let ids = parser.channel_ids().to_vec();
        let shared: Arc<dyn ChannelParser> = Arc::from(parser);
        self.names.insert(name, ids.clone());
        for id in ids {
            self.parsers.insert(id, Arc::clone(&shared));
        }
    }

    /// Look up the parser for a given channel ID.
    pub fn get(&self, channel_id: i64) -> Option<&dyn ChannelParser> {
        self.parsers.get(&channel_id).map(|arc| arc.as_ref())
    }

    /// Return channel IDs associated with a parser name.
    pub fn ids_for_name(&self, name: &str) -> Option<&[i64]> {
        self.names.get(name).map(|v| v.as_slice())
    }

    /// List all registered parser names.
    pub fn names(&self) -> Vec<&str> {
        self.names.keys().map(|s| s.as_str()).collect()
    }

    /// Check whether a parser exists for the given channel ID.
    pub fn has_parser(&self, channel_id: i64) -> bool {
        self.parsers.contains_key(&channel_id)
    }
}

impl std::fmt::Debug for ParserRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParserRegistry")
            .field("channels", &self.parsers.keys().collect::<Vec<_>>())
            .field("names", &self.names.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Default for ParserRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template::TemplateParser;

    fn make_parser(name: &str, ids: Vec<i64>) -> Box<dyn ChannelParser> {
        Box::new(TemplateParser::new(name.to_string(), ids, 0.01, None))
    }

    #[test]
    fn registry_lookup_by_channel() {
        let mut reg = ParserRegistry::new();
        reg.register(make_parser("wave-trader", vec![100, 200]));
        assert!(reg.get(100).is_some());
        assert!(reg.get(200).is_some());
        assert_eq!(reg.get(100).unwrap().name(), "wave-trader");
    }

    #[test]
    fn registry_lookup_unknown_channel() {
        let reg = ParserRegistry::new();
        assert!(reg.get(999).is_none());
        assert!(!reg.has_parser(999));
    }

    #[test]
    fn registry_names() {
        let mut reg = ParserRegistry::new();
        reg.register(make_parser("alpha", vec![1]));
        reg.register(make_parser("beta", vec![2]));
        let mut names = reg.names();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }
}
