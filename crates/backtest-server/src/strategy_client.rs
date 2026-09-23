//! TOML run files that describe a configured strategy run or a parameter search for the backtest service.
//!
//! A run file names a bound strategy document, or a strategy template and a space document, by path relative to the run file, together with the data scope, profiles, sizing, and series geometry. Documents stay in their TOML authoring form on disk and are converted to JSON values only when the request is built, because the service transport is JSON and the service decodes documents strictly.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::rpc_types::{
    BacktestConfigMsg, ConfiguredStrategyRunMsg, ConfiguredStrategyRunSpec, EntryProfileRouteMsg,
    FutureQuoteConfigMsg, ProfileRef, ProviderEvaluationOptionsMsg, ResultDeliveryMsg,
    RunConfiguredStrategyRequest, SearchRunSpec, SearchWindowsMsg, SourceBindingMsg,
    SubmitConfiguredStrategyRequest, SubmitSearchRequest,
};

/// A request built from a run file, ready to submit.
#[derive(Debug, Clone)]
pub enum StrategyClientRequest {
    Run(SubmitConfiguredStrategyRequest),
    Search(SubmitSearchRequest),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunFile {
    #[serde(default)]
    strategy: Option<PathBuf>,
    #[serde(default)]
    template: Option<PathBuf>,
    #[serde(default)]
    space: Option<PathBuf>,
    exchange: String,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    symbols: Vec<String>,
    data_type: String,
    #[serde(default)]
    timeframe: Option<String>,
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    to: Option<String>,
    account_currency: String,
    #[serde(default)]
    profile: Option<String>,
    #[serde(default)]
    entry_profile_routes: Vec<RouteFile>,
    #[serde(default)]
    series: Vec<SourceBindingMsg>,
    #[serde(default)]
    windows: Option<SearchWindowsMsg>,
    config: BacktestConfigMsg,
    #[serde(default)]
    instance_id: Option<String>,
    #[serde(default)]
    decision_latency_ms: u64,
    #[serde(default)]
    workers: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteFile {
    entry_class: String,
    profile: String,
}

/// Read a run file and the documents it names, and build the request it describes.
pub fn load_run_file(path: &Path) -> Result<StrategyClientRequest, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read run file {}: {error}", path.display()))?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    build_request(&text, base)
}

/// Build a request from run-file text, resolving document paths against `base`.
pub fn build_request(text: &str, base: &Path) -> Result<StrategyClientRequest, String> {
    let file: RunFile =
        toml::from_str(text).map_err(|error| format!("invalid run file: {error}"))?;
    let future = FutureQuoteConfigMsg {
        account_currency: file.account_currency.clone(),
        ..FutureQuoteConfigMsg::default()
    };
    let routes = file
        .entry_profile_routes
        .iter()
        .map(|route| EntryProfileRouteMsg {
            entry_class: route.entry_class.clone(),
            profile: ProfileRef::Named(route.profile.clone()),
        })
        .collect::<Vec<_>>();
    match (&file.strategy, &file.template, &file.space) {
        (Some(strategy), None, None) => {
            let symbol = file
                .symbol
                .clone()
                .ok_or("a strategy run requires symbol")?;
            if !file.symbols.is_empty() || file.windows.is_some() || file.workers.is_some() {
                return Err("symbols, windows, and workers apply only to a search".into());
            }
            if file.series.is_empty() {
                return Err("a strategy run requires at least one [[series]]".into());
            }
            Ok(StrategyClientRequest::Run(
                SubmitConfiguredStrategyRequest {
                    request: RunConfiguredStrategyRequest {
                        request: ConfiguredStrategyRunSpec {
                            symbol,
                            exchange: file.exchange,
                            data_type: file.data_type,
                            timeframe: file.timeframe,
                            from: file.from,
                            to: file.to,
                            strategy: ConfiguredStrategyRunMsg {
                                document: document_value(&base.join(strategy))?,
                                sources: file.series,
                                instance_id: file.instance_id,
                                decision_latency_ms: file.decision_latency_ms,
                            },
                            profile: file.profile,
                            profile_def: None,
                            entry_profile_routes: routes,
                            config: file.config,
                        },
                        future,
                        evaluation: ProviderEvaluationOptionsMsg::default(),
                        result_delivery: ResultDeliveryMsg::Auto,
                    },
                },
            ))
        }
        (None, Some(template), Some(space)) => {
            let mut symbols = file.symbols;
            if let Some(symbol) = file.symbol {
                symbols.push(symbol);
            }
            if symbols.is_empty() {
                return Err("a search requires symbol or symbols".into());
            }
            if !file.series.is_empty() || file.instance_id.is_some() {
                return Err("a search takes its series from the space document".into());
            }
            if file.from.is_some() || file.to.is_some() {
                return Err("a search takes its range from [windows]".into());
            }
            let windows = file.windows.ok_or("a search requires [windows]")?;
            Ok(StrategyClientRequest::Search(SubmitSearchRequest {
                request: SearchRunSpec {
                    template: document_value(&base.join(template))?,
                    space: document_value(&base.join(space))?,
                    symbols,
                    exchange: file.exchange,
                    data_type: file.data_type,
                    timeframe: file.timeframe,
                    windows,
                    config: file.config,
                    profile: file.profile,
                    entry_profile_routes: routes,
                    workers: file.workers,
                    decision_latency_ms: file.decision_latency_ms,
                },
                future,
                evaluation: ProviderEvaluationOptionsMsg::default(),
            }))
        }
        _ => Err("a run file names either strategy, or both template and space".into()),
    }
}

/// Read a TOML document and convert it to the JSON value the service decodes.
fn document_value(path: &Path) -> Result<serde_json::Value, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read document {}: {error}", path.display()))?;
    let value: toml::Value = toml::from_str(&text)
        .map_err(|error| format!("invalid TOML document {}: {error}", path.display()))?;
    serde_json::to_value(value)
        .map_err(|error| format!("cannot convert {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "qs_strategy_client_{}_{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(
            path.join("strategy.toml"),
            "strategy_id = \"alpha\"\n[[states]]\nid = \"flat\"\n",
        )
        .unwrap();
        std::fs::write(path.join("space.toml"), "family_id = \"alpha\"\n").unwrap();
        path
    }

    const COMMON: &str = r#"
exchange = "icmarkets"
data_type = "tick"
account_currency = "USD"

[config]
initial_balance = 10000.0
sizing = { type = "FixedLot", lots = 0.1 }
"#;

    #[test]
    fn a_run_file_builds_a_configured_request_with_its_document_as_json() {
        let base = directory();
        let text = format!(
            "strategy = \"strategy.toml\"\nsymbol = \"EURUSD\"\nfrom = \"2026-01-01\"\nprofile = \"trail\"\n{COMMON}\n[[entry_profile_routes]]\nentry_class = \"trend\"\nprofile = \"trail\"\n\n[[series]]\nsource = \"primary\"\ntimeframe_seconds = 60\nprice_basis = \"mid\"\n"
        );
        let StrategyClientRequest::Run(request) = build_request(&text, &base).unwrap() else {
            panic!("a strategy run file builds a run request");
        };
        let run = &request.request;
        assert_eq!(run.future.account_currency, "USD");
        assert_eq!(run.request.symbol, "EURUSD");
        assert_eq!(run.request.from.as_deref(), Some("2026-01-01"));
        assert_eq!(
            run.request.strategy.document,
            serde_json::json!({ "strategy_id": "alpha", "states": [{ "id": "flat" }] })
        );
        assert_eq!(run.request.strategy.sources[0].source, "primary");
        assert!(matches!(
            &run.request.entry_profile_routes[0].profile,
            ProfileRef::Named(name) if name == "trail"
        ));
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn a_search_file_builds_a_search_request_from_template_space_and_windows() {
        let base = directory();
        let text = format!(
            "template = \"strategy.toml\"\nspace = \"space.toml\"\nsymbols = [\"EURUSD\"]\nsymbol = \"GBPUSD\"\nworkers = 4\n{COMMON}\n[windows]\ntype = \"fixed\"\nin_sample = {{ label = \"is\", from = \"2025-01-01\", to = \"2026-01-01\" }}\nout_of_sample = {{ label = \"oos\", from = \"2026-01-01\", to = \"2026-09-01\" }}\n"
        );
        let StrategyClientRequest::Search(request) = build_request(&text, &base).unwrap() else {
            panic!("a template and space build a search request");
        };
        assert_eq!(request.request.symbols, vec!["EURUSD", "GBPUSD"]);
        assert_eq!(request.request.workers, Some(4));
        assert_eq!(
            request.request.space,
            serde_json::json!({ "family_id": "alpha" })
        );
        assert!(matches!(
            request.request.windows,
            SearchWindowsMsg::Fixed { .. }
        ));
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn contradictory_or_incomplete_run_files_are_rejected() {
        let base = directory();
        let cases = [
            (
                "strategy = \"strategy.toml\"\ntemplate = \"strategy.toml\"\nspace = \"space.toml\"\nsymbol = \"EURUSD\"\n",
                "either strategy",
            ),
            (
                "strategy = \"strategy.toml\"\nsymbol = \"EURUSD\"\n",
                "[[series]]",
            ),
            (
                "template = \"strategy.toml\"\nspace = \"space.toml\"\nsymbol = \"EURUSD\"\n",
                "[windows]",
            ),
            (
                "template = \"strategy.toml\"\nspace = \"space.toml\"\nsymbol = \"EURUSD\"\nfrom = \"2026-01-01\"\n",
                "[windows]",
            ),
            (
                "strategy = \"missing.toml\"\nsymbol = \"EURUSD\"\n[[series]]\nsource = \"primary\"\ntimeframe_seconds = 60\nprice_basis = \"mid\"\n",
                "cannot read document",
            ),
            (
                "strategy = \"strategy.toml\"\nsymbol = \"EURUSD\"\nunknown = 1\n",
                "invalid run file",
            ),
        ];
        for (head, expected) in cases {
            let (before, after) = head
                .split_once("[[series]]")
                .map_or((head, ""), |(before, after)| (before, after));
            let text = if after.is_empty() {
                format!("{before}{COMMON}")
            } else {
                format!("{before}{COMMON}\n[[series]]{after}")
            };
            let error = build_request(&text, &base).unwrap_err();
            assert!(error.contains(expected), "{expected:?} not in {error:?}");
        }
        std::fs::remove_dir_all(base).unwrap();
    }
}
