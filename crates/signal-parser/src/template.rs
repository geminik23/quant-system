use chrono::NaiveDateTime;
use qs_backtest::RawSignalEntry;
use qs_core::{OrderType, Side};

use crate::parser::ChannelParser;
use crate::types::{ParseContext, ParsedAction, RawTgMessage};

/// Built-in parser for channels that use a simple text template format.
/// Pattern: `{SYMBOL} {BUY|SELL} {NOW|MARKET|LIMIT|STOP} [PRICE] SL {SL} [TP {TP}]+`
pub struct TemplateParser {
    name: String,
    channel_ids: Vec<i64>,
    default_size: f64,
    group_prefix: String,
}

impl TemplateParser {
    pub fn new(
        name: impl Into<String>,
        channel_ids: Vec<i64>,
        default_size: f64,
        group_prefix: Option<String>,
    ) -> Self {
        let name = name.into();
        let group_prefix = group_prefix.unwrap_or_else(|| format!("tg_{}", name));
        Self {
            name,
            channel_ids,
            default_size,
            group_prefix,
        }
    }

    /// Try to parse a float from a token, stripping trailing commas/periods.
    fn parse_price(token: &str) -> Option<f64> {
        token.trim_end_matches(',').parse::<f64>().ok()
    }

    /// Core template parsing logic shared by root messages.
    fn parse_template(&self, message: &str, ts: NaiveDateTime) -> ParsedAction {
        // Normalize: uppercase, collapse whitespace.
        let normalized: String = message.to_uppercase();
        let tokens: Vec<&str> = normalized.split_whitespace().collect();

        if tokens.len() < 4 {
            return ParsedAction::Skip;
        }

        let mut idx = 0;

        // 1. Symbol (first token, lowercased for output).
        let symbol = tokens[idx].to_lowercase();
        idx += 1;

        // 2. Side: BUY/LONG → Buy, SELL/SHORT → Sell.
        let side = match tokens[idx] {
            "BUY" | "LONG" => Side::Buy,
            "SELL" | "SHORT" => Side::Sell,
            _ => return ParsedAction::Skip,
        };
        idx += 1;

        // 3. Order type + optional entry price.
        let (order_type, price) = if idx >= tokens.len() {
            return ParsedAction::Skip;
        } else if let Some(p) = Self::parse_price(tokens[idx]) {
            // Bare number after side → Limit with that price.
            idx += 1;
            (OrderType::Limit, Some(p))
        } else {
            match tokens[idx] {
                "NOW" | "MARKET" => {
                    idx += 1;
                    (OrderType::Market, None)
                }
                "LIMIT" => {
                    idx += 1;
                    let p = if idx < tokens.len() {
                        if let Some(v) = Self::parse_price(tokens[idx]) {
                            idx += 1;
                            Some(v)
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    (OrderType::Limit, p)
                }
                "STOP" => {
                    idx += 1;
                    let p = if idx < tokens.len() {
                        if let Some(v) = Self::parse_price(tokens[idx]) {
                            idx += 1;
                            Some(v)
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    (OrderType::Stop, p)
                }
                _ => return ParsedAction::Skip,
            }
        };

        // 4. Scan remaining tokens for SL/STOPLOSS and TP/TP1/TP2...
        let mut stoploss: Option<f64> = None;
        let mut targets: Vec<f64> = Vec::new();

        while idx < tokens.len() {
            let tok = tokens[idx];
            if tok == "SL" || tok == "STOPLOSS" {
                idx += 1;
                if idx < tokens.len() {
                    if let Some(v) = Self::parse_price(tokens[idx]) {
                        stoploss = Some(v);
                        idx += 1;
                    }
                }
            } else if tok == "TP" || tok.starts_with("TP") && tok.len() <= 4 {
                // Matches TP, TP1, TP2, etc.
                idx += 1;
                if idx < tokens.len() {
                    if let Some(v) = Self::parse_price(tokens[idx]) {
                        targets.push(v);
                        idx += 1;
                    }
                }
            } else {
                idx += 1;
            }
        }

        // 5. Require at least SL or TP to consider this a valid signal.
        if stoploss.is_none() && targets.is_empty() {
            return ParsedAction::Skip;
        }

        ParsedAction::Entries(vec![RawSignalEntry {
            ts,
            symbol,
            side,
            order_type,
            price,
            size: self.default_size,
            stoploss,
            targets,
            group: Some(self.group_prefix.clone()),
        }])
    }
}

impl ChannelParser for TemplateParser {
    fn name(&self) -> &str {
        &self.name
    }

    fn channel_ids(&self) -> &[i64] {
        &self.channel_ids
    }

    // Template parser is stateless — no history needed.
    fn max_history(&self) -> usize {
        0
    }

    /// Root messages are parsed against the text template.
    fn parse_root(&self, message: &str, ts: NaiveDateTime, _ctx: &ParseContext) -> ParsedAction {
        self.parse_template(message, ts)
    }

    /// Template parser ignores reply messages — override in custom parsers.
    fn parse_reply(
        &self,
        _message: &str,
        _ts: NaiveDateTime,
        _parent: Option<&RawTgMessage>,
        _ctx: &ParseContext,
    ) -> ParsedAction {
        ParsedAction::Skip
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts() -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap()
    }

    fn parser() -> TemplateParser {
        TemplateParser::new("test-chan", vec![123], 0.01, None)
    }

    fn entries(action: ParsedAction) -> Vec<RawSignalEntry> {
        match action {
            ParsedAction::Entries(v) => v,
            ParsedAction::Skip => panic!("expected Entries, got Skip"),
        }
    }

    #[test]
    fn template_parse_buy_now() {
        let ctx = ParseContext::empty();
        let result = parser().parse_root("EURUSD BUY NOW SL 1.0800 TP 1.0900", ts(), &ctx);
        let e = entries(result);
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].symbol, "eurusd");
        assert_eq!(e[0].side, Side::Buy);
        assert_eq!(e[0].order_type, OrderType::Market);
        assert_eq!(e[0].price, None);
        assert!((e[0].stoploss.unwrap() - 1.08).abs() < 1e-9);
        assert_eq!(e[0].targets, vec![1.09]);
    }

    #[test]
    fn template_parse_sell_limit() {
        let ctx = ParseContext::empty();
        let result = parser().parse_root("XAUUSD SELL LIMIT 2650 SL 2680 TP 2620", ts(), &ctx);
        let e = entries(result);
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].symbol, "xauusd");
        assert_eq!(e[0].side, Side::Sell);
        assert_eq!(e[0].order_type, OrderType::Limit);
        assert!((e[0].price.unwrap() - 2650.0).abs() < 1e-9);
        assert!((e[0].stoploss.unwrap() - 2680.0).abs() < 1e-9);
        assert_eq!(e[0].targets, vec![2620.0]);
    }

    #[test]
    fn template_parse_multiple_targets() {
        let ctx = ParseContext::empty();
        let result = parser().parse_root(
            "XAUUSD SELL LIMIT 2650 SL 2680 TP1 2620 TP2 2600",
            ts(),
            &ctx,
        );
        let e = entries(result);
        assert_eq!(e[0].targets, vec![2620.0, 2600.0]);
    }

    #[test]
    fn template_parse_stop_order() {
        let ctx = ParseContext::empty();
        let result = parser().parse_root("GBPUSD BUY STOP 1.3000 SL 1.2950 TP 1.3100", ts(), &ctx);
        let e = entries(result);
        assert_eq!(e[0].order_type, OrderType::Stop);
        assert!((e[0].price.unwrap() - 1.3).abs() < 1e-9);
    }

    #[test]
    fn template_skip_non_signal() {
        let ctx = ParseContext::empty();
        let result = parser().parse_root("Good morning traders", ts(), &ctx);
        assert!(matches!(result, ParsedAction::Skip));
    }

    #[test]
    fn template_skip_partial() {
        let ctx = ParseContext::empty();
        let result = parser().parse_root("EURUSD is looking bullish", ts(), &ctx);
        assert!(matches!(result, ParsedAction::Skip));
    }

    #[test]
    fn template_case_insensitive() {
        let ctx = ParseContext::empty();
        let result = parser().parse_root("eurusd buy now sl 1.0800 tp 1.0900", ts(), &ctx);
        let e = entries(result);
        assert_eq!(e[0].symbol, "eurusd");
        assert_eq!(e[0].side, Side::Buy);
        assert_eq!(e[0].order_type, OrderType::Market);
    }

    #[test]
    fn template_no_entry_price_market() {
        let ctx = ParseContext::empty();
        let result = parser().parse_root("EURUSD BUY MARKET SL 1.0800 TP 1.0900", ts(), &ctx);
        let e = entries(result);
        assert_eq!(e[0].order_type, OrderType::Market);
        assert_eq!(e[0].price, None);
    }

    #[test]
    fn template_reply_is_skipped() {
        let ctx = ParseContext::empty();
        let result = parser().parse_reply("close this", ts(), None, &ctx);
        assert!(matches!(result, ParsedAction::Skip));
    }

    #[test]
    fn template_max_history_is_zero() {
        assert_eq!(parser().max_history(), 0);
    }
}
