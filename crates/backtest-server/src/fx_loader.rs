//! Server-side currency planning and conversion tick loading.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use chrono::NaiveDateTime;
#[cfg(test)]
use data_preprocess::models::QueryOpts;
use data_preprocess::{DataError, ParquetScanBounds, ParquetStore, Tick};
use qs_backtest::currency::{ConversionRoute, RunCurrencyPlan, resolve_conversion_route};
#[cfg(test)]
use qs_backtest::data_feed::{SeriesRoles, VecFeed, ticks_to_feed_with_metadata};
use qs_core::types::PriceQuote;
use qs_symbols::SymbolRegistry;

use crate::error::{BacktestServerError, Result};
use crate::market_loader::{MarketSeriesDescription, MarketStreamDescription};

#[cfg(test)]
pub(crate) struct LoadedFutureBundle {
    pub feed: VecFeed,
    pub currency_plan: RunCurrencyPlan,
}

pub(crate) struct LoadedFutureStream {
    pub description: MarketStreamDescription,
    pub currency_plan: RunCurrencyPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TickDataset {
    exchange: String,
    symbol: String,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn describe_future_stream(
    data_dir: &str,
    exchange: &str,
    registry: &SymbolRegistry,
    account_currency: &str,
    primary_symbols: &[String],
    primary_data_type: &str,
    replay_start: Option<NaiveDateTime>,
    mut primary: MarketStreamDescription,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<LoadedFutureStream> {
    ensure_not_cancelled(is_cancelled)?;
    let primary_symbol_set = primary_symbols.iter().cloned().collect::<BTreeSet<_>>();
    if primary_symbol_set.is_empty() {
        return Ok(LoadedFutureStream {
            description: primary,
            currency_plan: RunCurrencyPlan::new(
                account_currency,
                BTreeSet::new(),
                BTreeSet::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                Vec::new(),
            )?,
        });
    }

    let conversion_start = replay_start.or(primary.primary_start()).ok_or_else(|| {
        BacktestServerError::InvalidRequest("primary market-data stream is empty".into())
    })?;
    let conversion_end = primary.conversion_end().ok_or_else(|| {
        BacktestServerError::InvalidRequest("primary market-data stream has no replay end".into())
    })?;
    let pnl_currency_by_primary_symbol = primary_pnl_currencies(registry, &primary_symbol_set)?;
    let store = ParquetStore::open(data_dir)?;
    let datasets = discover_tick_datasets(data_dir, exchange, registry, is_cancelled)?;
    let available_symbols = datasets.keys().cloned().collect::<BTreeSet<_>>();
    let routes = resolve_routes_to_account(
        registry,
        &pnl_currency_by_primary_symbol,
        account_currency,
        exchange,
        &available_symbols,
    )?;
    let conversion_symbols = routes
        .values()
        .flat_map(ConversionRoute::symbols)
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();

    let mut warmup_quotes = Vec::with_capacity(conversion_symbols.len());
    for symbol in &conversion_symbols {
        ensure_not_cancelled(is_cancelled)?;
        let dataset = datasets.get(symbol).ok_or_else(|| {
            BacktestServerError::InvalidRequest(format!(
                "conversion route symbol '{symbol}' has no tick dataset on exchange '{exchange}'"
            ))
        })?;
        let tick = store
            .latest_valid_tick_before_cancellable(
                &dataset.exchange,
                &dataset.symbol,
                conversion_start,
                &mut *is_cancelled,
            )
            .map_err(map_data_error)?
            .ok_or_else(|| {
                BacktestServerError::InvalidRequest(format!(
                    "no valid conversion tick for '{symbol}' strictly before {conversion_start}"
                ))
            })?;
        warmup_quotes.push(tick_to_quote(tick, symbol)?);
    }

    let shared_tick_symbols = if primary_data_type.eq_ignore_ascii_case("tick") {
        primary_symbol_set
            .intersection(&conversion_symbols)
            .cloned()
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    primary.mark_shared_conversion_symbols(&shared_tick_symbols);
    let primary_rank_count = primary.primary_series_count();
    let conversion_bounds = ParquetScanBounds::new(Some(conversion_start), Some(conversion_end));
    for (route_index, symbol) in conversion_symbols.iter().enumerate() {
        ensure_not_cancelled(is_cancelled)?;
        if shared_tick_symbols.contains(symbol) {
            primary.push_series(MarketSeriesDescription::empty_conversion(
                data_dir,
                symbol.clone(),
            ));
            continue;
        }
        let dataset = datasets
            .get(symbol)
            .expect("route symbols were checked against discovered datasets");
        let expected_rank = primary_rank_count.saturating_add(route_index);
        if primary.primary_series_count() != expected_rank {
            return Err(BacktestServerError::InvalidRequest(
                "conversion stream rank construction is inconsistent".into(),
            ));
        }
        if conversion_start <= conversion_end {
            primary.push_series(MarketSeriesDescription::conversion_tick(
                data_dir,
                dataset.exchange.clone(),
                dataset.symbol.clone(),
                symbol.clone(),
                conversion_bounds,
            ));
        } else {
            primary.push_series(MarketSeriesDescription::empty_conversion(
                data_dir,
                symbol.clone(),
            ));
        }
    }

    let currency_plan = RunCurrencyPlan::new(
        account_currency,
        primary_symbol_set,
        conversion_symbols,
        pnl_currency_by_primary_symbol,
        routes,
        warmup_quotes,
    )?;
    Ok(LoadedFutureStream {
        description: primary,
        currency_plan,
    })
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) fn load_materialized_future_fixture(
    data_dir: &str,
    exchange: &str,
    registry: &SymbolRegistry,
    account_currency: &str,
    primary_symbols: &[String],
    primary_data_type: &str,
    replay_start: Option<NaiveDateTime>,
    requested_to: Option<NaiveDateTime>,
    mut primary_feed: VecFeed,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<LoadedFutureBundle> {
    ensure_not_cancelled(is_cancelled)?;
    let primary_symbol_set = primary_symbols.iter().cloned().collect::<BTreeSet<_>>();
    if primary_symbol_set.is_empty() {
        return Ok(LoadedFutureBundle {
            feed: primary_feed,
            currency_plan: RunCurrencyPlan::new(
                account_currency,
                BTreeSet::new(),
                BTreeSet::new(),
                BTreeMap::new(),
                BTreeMap::new(),
                Vec::new(),
            )?,
        });
    }

    let mut primary_events = Vec::with_capacity(primary_feed.total());
    while let Some(event) = primary_feed.next_feed_event() {
        ensure_not_cancelled(is_cancelled)?;
        primary_events.push(event);
    }
    let primary_start = primary_events.iter().map(|event| event.event.ts()).min();
    let conversion_start = replay_start.or(primary_start).ok_or_else(|| {
        BacktestServerError::InvalidRequest("primary market-data feed is empty".into())
    })?;
    let primary_eod = primary_events
        .iter()
        .map(|event| event.event.ts())
        .max()
        .or(requested_to)
        .ok_or_else(|| {
            BacktestServerError::InvalidRequest("primary market-data feed is empty".into())
        })?;

    let pnl_currency_by_primary_symbol = primary_pnl_currencies(registry, &primary_symbol_set)?;
    let store = ParquetStore::open(data_dir)?;
    let datasets = discover_tick_datasets(data_dir, exchange, registry, is_cancelled)?;
    let available_symbols = datasets.keys().cloned().collect::<BTreeSet<_>>();
    let routes = resolve_routes_to_account(
        registry,
        &pnl_currency_by_primary_symbol,
        account_currency,
        exchange,
        &available_symbols,
    )?;
    let conversion_symbols = routes
        .values()
        .flat_map(ConversionRoute::symbols)
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();

    let mut warmup_quotes = Vec::with_capacity(conversion_symbols.len());
    for symbol in &conversion_symbols {
        ensure_not_cancelled(is_cancelled)?;
        let dataset = datasets.get(symbol).ok_or_else(|| {
            BacktestServerError::InvalidRequest(format!(
                "conversion route symbol '{symbol}' has no tick dataset on exchange '{exchange}'"
            ))
        })?;
        let tick = store
            .latest_valid_tick_before_cancellable(
                &dataset.exchange,
                &dataset.symbol,
                conversion_start,
                &mut *is_cancelled,
            )
            .map_err(map_data_error)?
            .ok_or_else(|| {
                BacktestServerError::InvalidRequest(format!(
                    "no valid conversion tick for '{symbol}' strictly before {conversion_start}"
                ))
            })?;
        warmup_quotes.push(tick_to_quote(tick, symbol)?);
    }

    let shared_tick_symbols = if primary_data_type.eq_ignore_ascii_case("tick") {
        primary_symbol_set
            .intersection(&conversion_symbols)
            .cloned()
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    for event in &mut primary_events {
        if shared_tick_symbols.contains(event.event.symbol()) {
            event.metadata.roles = SeriesRoles::PRIMARY_AND_CONVERSION;
        }
    }

    let primary_rank_count = primary_symbols.len().min(u32::MAX as usize) as u32;
    let mut merged_events = primary_events;
    for (route_index, symbol) in conversion_symbols.iter().enumerate() {
        if shared_tick_symbols.contains(symbol) {
            continue;
        }
        ensure_not_cancelled(is_cancelled)?;
        let dataset = datasets
            .get(symbol)
            .expect("route symbols were checked against discovered datasets");
        let mut ticks = if conversion_start <= primary_eod {
            let opts = QueryOpts {
                exchange: dataset.exchange.clone(),
                symbol: dataset.symbol.clone(),
                from: Some(conversion_start),
                to: Some(primary_eod),
                limit: 0,
                tail: false,
                descending: false,
            };
            store
                .query_ticks_cancellable(&opts, &mut *is_cancelled)
                .map_err(map_data_error)?
                .0
        } else {
            Vec::new()
        };
        for tick in &mut ticks {
            tick.symbol.clone_from(symbol);
        }
        let route_rank = route_index.min(u32::MAX as usize) as u32;
        let series_rank = primary_rank_count.saturating_add(route_rank);
        let mut conversion_feed =
            ticks_to_feed_with_metadata(ticks, SeriesRoles::CONVERSION, series_rank);
        while let Some(event) = conversion_feed.next_feed_event() {
            ensure_not_cancelled(is_cancelled)?;
            merged_events.push(event);
        }
    }

    ensure_not_cancelled(is_cancelled)?;
    let currency_plan = RunCurrencyPlan::new(
        account_currency,
        primary_symbol_set,
        conversion_symbols,
        pnl_currency_by_primary_symbol,
        routes,
        warmup_quotes,
    )?;

    Ok(LoadedFutureBundle {
        feed: VecFeed::from_feed_events(merged_events),
        currency_plan,
    })
}

fn primary_pnl_currencies(
    registry: &SymbolRegistry,
    primary_symbols: &BTreeSet<String>,
) -> Result<BTreeMap<String, String>> {
    primary_symbols
        .iter()
        .map(|symbol| {
            let metadata = registry.currency_metadata(symbol).ok_or_else(|| {
                BacktestServerError::InvalidRequest(format!(
                    "primary symbol '{symbol}' has no explicit currency metadata"
                ))
            })?;
            if metadata.pnl_currency.is_empty() {
                return Err(BacktestServerError::InvalidRequest(format!(
                    "primary symbol '{symbol}' has no explicit P&L currency"
                )));
            }
            Ok((symbol.clone(), metadata.pnl_currency.clone()))
        })
        .collect()
}

fn resolve_routes_to_account(
    registry: &SymbolRegistry,
    pnl_currency_by_primary_symbol: &BTreeMap<String, String>,
    account_currency: &str,
    exchange: &str,
    available_symbols: &BTreeSet<String>,
) -> Result<BTreeMap<String, ConversionRoute>> {
    pnl_currency_by_primary_symbol
        .values()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|source_currency| {
            let route = resolve_conversion_route(
                registry,
                &source_currency,
                account_currency,
                available_symbols,
            )
            .map_err(|error| {
                BacktestServerError::InvalidRequest(format!(
                    "cannot resolve {source_currency} to {account_currency} on exchange '{exchange}': {error}"
                ))
            })?;
            Ok((source_currency, route))
        })
        .collect()
}

fn discover_tick_datasets(
    data_dir: &str,
    exchange: &str,
    registry: &SymbolRegistry,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<BTreeMap<String, TickDataset>> {
    let disk_exchange = resolve_tick_exchange(data_dir, exchange, is_cancelled)?;
    let exchange_dir = Path::new(data_dir)
        .join("ticks")
        .join(format!("exchange={disk_exchange}"));
    let mut discovered = Vec::new();
    ensure_not_cancelled(is_cancelled)?;
    if let Ok(entries) = std::fs::read_dir(exchange_dir) {
        for entry in entries.flatten() {
            ensure_not_cancelled(is_cancelled)?;
            if !entry.path().is_dir() || !contains_parquet_file(&entry.path(), is_cancelled)? {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(symbol) = name.strip_prefix("symbol=") else {
                continue;
            };
            let canonical = registry.normalize_or_passthrough(symbol);
            discovered.push((canonical, symbol.to_owned()));
        }
    }
    discovered.sort();

    let mut datasets = BTreeMap::new();
    for (canonical, symbol) in discovered {
        datasets.entry(canonical).or_insert_with(|| TickDataset {
            exchange: disk_exchange.clone(),
            symbol,
        });
    }
    Ok(datasets)
}

fn contains_parquet_file(directory: &Path, is_cancelled: &mut dyn FnMut() -> bool) -> Result<bool> {
    ensure_not_cancelled(is_cancelled)?;
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Ok(false);
    };
    for entry in entries.flatten() {
        ensure_not_cancelled(is_cancelled)?;
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn resolve_tick_exchange(
    data_dir: &str,
    requested: &str,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<String> {
    let ticks_dir = Path::new(data_dir).join("ticks");
    let mut candidates = Vec::new();
    ensure_not_cancelled(is_cancelled)?;
    if let Ok(entries) = std::fs::read_dir(ticks_dir) {
        for entry in entries.flatten() {
            ensure_not_cancelled(is_cancelled)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(exchange) = name.strip_prefix("exchange=")
                && exchange.eq_ignore_ascii_case(requested)
            {
                candidates.push(exchange.to_owned());
            }
        }
    }
    candidates.sort();
    Ok(candidates
        .iter()
        .find(|candidate| candidate.as_str() == requested)
        .cloned()
        .or_else(|| candidates.into_iter().next())
        .unwrap_or_else(|| requested.to_owned()))
}

fn tick_to_quote(tick: Tick, canonical_symbol: &str) -> Result<PriceQuote> {
    let bid = tick.bid.ok_or_else(|| {
        BacktestServerError::InvalidRequest(format!(
            "conversion tick for '{canonical_symbol}' has no bid"
        ))
    })?;
    let ask = tick.ask.ok_or_else(|| {
        BacktestServerError::InvalidRequest(format!(
            "conversion tick for '{canonical_symbol}' has no ask"
        ))
    })?;
    Ok(PriceQuote {
        symbol: canonical_symbol.to_owned(),
        ts: tick.ts,
        bid,
        ask,
    })
}

fn ensure_not_cancelled(is_cancelled: &mut dyn FnMut() -> bool) -> Result<()> {
    if is_cancelled() {
        Err(BacktestServerError::Cancelled)
    } else {
        Ok(())
    }
}

fn map_data_error(error: DataError) -> BacktestServerError {
    match error {
        DataError::Cancelled => BacktestServerError::Cancelled,
        other => BacktestServerError::Database(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use data_preprocess::Tick;
    use qs_backtest::data_feed::{DataFeed, FallibleBatchFeed, MarketEvent};

    fn ts(hour: u32, minute: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 1, 2)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
    }

    fn registry() -> SymbolRegistry {
        SymbolRegistry::from_toml(
            r#"
[[symbol]]
canonical = "eurjpy"
aliases = []
pip_position = 2
digits = 3
category = "forex"
base_currency = "EUR"
quote_currency = "JPY"
pnl_currency = "JPY"
lot_base_units = 100000
lot_step_units = 1000

[[symbol]]
canonical = "usdjpy"
aliases = []
pip_position = 2
digits = 3
category = "forex"
base_currency = "USD"
quote_currency = "JPY"
pnl_currency = "JPY"
lot_base_units = 100000
lot_step_units = 1000

[[symbol]]
canonical = "usdchf"
aliases = []
pip_position = 4
digits = 5
category = "forex"
base_currency = "USD"
quote_currency = "CHF"
pnl_currency = "CHF"
lot_base_units = 100000
lot_step_units = 1000
"#,
        )
        .unwrap()
    }

    fn temp_data_dir() -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("qs-fx-loader-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn tick(symbol: &str, timestamp: NaiveDateTime, bid: f64, ask: f64) -> Tick {
        Tick {
            exchange: "ctrader".into(),
            symbol: symbol.into(),
            ts: timestamp,
            bid: Some(bid),
            ask: Some(ask),
            last: None,
            volume: None,
            flags: None,
        }
    }

    #[test]
    fn empty_primary_universe_builds_idle_plan_without_storage() {
        let mut never_cancelled = || false;
        let bundle = load_materialized_future_fixture(
            "/path/that/does/not/exist",
            "ctrader",
            &registry(),
            "USD",
            &[],
            "tick",
            None,
            None,
            VecFeed::new(Vec::new()),
            &mut never_cancelled,
        )
        .unwrap();

        assert_eq!(bundle.feed.total(), 0);
        assert!(bundle.currency_plan.primary_symbols().is_empty());
        assert!(bundle.currency_plan.conversion_symbols().is_empty());
    }

    #[test]
    fn resolves_identity_direct_inverse_and_two_leg_routes() {
        let registry = registry();
        let route = |source: &str, destination: &str, available: &[&str]| {
            let pnl = BTreeMap::from([("primary".to_owned(), source.to_owned())]);
            resolve_routes_to_account(
                &registry,
                &pnl,
                destination,
                "ctrader",
                &available
                    .iter()
                    .map(|symbol| (*symbol).to_owned())
                    .collect(),
            )
            .unwrap()
            .remove(source)
            .unwrap()
        };

        assert!(matches!(
            route("JPY", "JPY", &[]),
            ConversionRoute::Identity { .. }
        ));
        assert!(matches!(
            route("USD", "JPY", &["usdjpy"]),
            ConversionRoute::Direct { .. }
        ));
        assert!(matches!(
            route("JPY", "USD", &["usdjpy"]),
            ConversionRoute::Inverse { .. }
        ));
        assert!(matches!(
            route("JPY", "CHF", &["usdjpy", "usdchf"]),
            ConversionRoute::TwoLeg { pivot_currency, .. } if pivot_currency == "USD"
        ));
    }

    #[test]
    fn loads_inverse_route_warmup_and_ticks_only_through_primary_eod() {
        let data_dir = temp_data_dir();
        let store = ParquetStore::open(&data_dir).unwrap();
        store
            .insert_ticks(&[
                tick("USDJPY", ts(9, 59), 149.0, 149.1),
                tick("USDJPY", ts(10, 0), 149.1, 149.2),
                tick("USDJPY", ts(10, 1), 149.2, 149.3),
                tick("USDJPY", ts(10, 2), 149.3, 149.4),
            ])
            .unwrap();
        let primary_feed = VecFeed::new(vec![
            MarketEvent::Tick {
                symbol: "eurjpy".into(),
                ts: ts(10, 0),
                bid: 160.0,
                ask: 160.1,
            },
            MarketEvent::Tick {
                symbol: "eurjpy".into(),
                ts: ts(10, 1),
                bid: 160.1,
                ask: 160.2,
            },
        ]);
        let mut never_cancelled = || false;
        let mut bundle = load_materialized_future_fixture(
            data_dir.to_str().unwrap(),
            "ctrader",
            &registry(),
            "USD",
            &["eurjpy".to_owned()],
            "tick",
            Some(ts(10, 0)),
            None,
            primary_feed,
            &mut never_cancelled,
        )
        .unwrap();
        assert!(matches!(
            bundle
                .currency_plan
                .conversion_route_by_source_currency()
                .get("JPY"),
            Some(ConversionRoute::Inverse { pair }) if pair.symbol == "usdjpy"
        ));
        assert_eq!(bundle.currency_plan.strict_before_warmup_quotes().len(), 1);
        assert_eq!(
            bundle.currency_plan.strict_before_warmup_quotes()[0].ts,
            ts(9, 59)
        );
        assert_eq!(bundle.feed.total(), 4);
        let mut conversion_count = 0;
        while let Some(event) = bundle.feed.next_feed_event() {
            assert!(event.event.ts() <= ts(10, 1));
            if event.event.symbol() == "usdjpy" {
                conversion_count += 1;
                assert!(!event.metadata.roles.primary);
                assert!(event.metadata.roles.conversion);
                assert_eq!(event.metadata.series_rank, 1);
            }
        }
        assert_eq!(conversion_count, 2);

        std::fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn streaming_shared_primary_conversion_reopens_without_duplicates() {
        let data_dir = temp_data_dir();
        let store = ParquetStore::open(&data_dir).unwrap();
        store
            .insert_ticks(&[
                tick("USDJPY", ts(9, 59), 149.0, 149.1),
                tick("USDJPY", ts(10, 0), 149.1, 149.2),
                tick("USDJPY", ts(10, 1), 149.2, 149.3),
            ])
            .unwrap();
        let mut never_cancelled = || false;
        let primary = crate::market_loader::describe_primary_market_stream(
            data_dir.to_str().unwrap(),
            "ctrader",
            &["usdjpy".into()],
            "tick",
            None,
            Some(ts(10, 0)),
            Some(ts(10, 1)),
            &mut never_cancelled,
            &mut |_| {},
        )
        .unwrap();
        let bundle = describe_future_stream(
            data_dir.to_str().unwrap(),
            "ctrader",
            &registry(),
            "USD",
            &["usdjpy".into()],
            "tick",
            Some(ts(10, 0)),
            primary,
            &mut never_cancelled,
        )
        .unwrap();

        assert_eq!(bundle.currency_plan.strict_before_warmup_quotes().len(), 1);
        assert_eq!(
            bundle.currency_plan.strict_before_warmup_quotes()[0].ts,
            ts(9, 59)
        );
        let mut stream = bundle
            .description
            .open(std::sync::Arc::new(|| false))
            .unwrap();
        assert_eq!(
            stream.series_count(),
            2,
            "shared FX rank must remain reserved"
        );
        let mut count = 0;
        while let Some(batch) = FallibleBatchFeed::next_batch(&mut stream).unwrap() {
            assert_eq!(batch.events.len(), 1);
            let event = &batch.events[0];
            assert_eq!(event.metadata.roles, SeriesRoles::PRIMARY_AND_CONVERSION);
            assert_eq!(event.metadata.series_rank, 0);
            count += 1;
        }
        assert_eq!(count, 2);

        std::fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn shared_primary_conversion_ticks_are_not_duplicated() {
        let data_dir = temp_data_dir();
        let store = ParquetStore::open(&data_dir).unwrap();
        store
            .insert_ticks(&[
                tick("USDJPY", ts(9, 59), 149.0, 149.1),
                tick("USDJPY", ts(10, 0), 149.1, 149.2),
                tick("USDJPY", ts(10, 1), 149.2, 149.3),
            ])
            .unwrap();
        let primary_feed = VecFeed::new(vec![
            MarketEvent::Tick {
                symbol: "usdjpy".into(),
                ts: ts(10, 0),
                bid: 149.1,
                ask: 149.2,
            },
            MarketEvent::Tick {
                symbol: "usdjpy".into(),
                ts: ts(10, 1),
                bid: 149.2,
                ask: 149.3,
            },
        ]);
        let mut never_cancelled = || false;
        let mut bundle = load_materialized_future_fixture(
            data_dir.to_str().unwrap(),
            "ctrader",
            &registry(),
            "USD",
            &["usdjpy".to_owned()],
            "tick",
            Some(ts(10, 0)),
            None,
            primary_feed,
            &mut never_cancelled,
        )
        .unwrap();
        assert_eq!(bundle.feed.total(), 2);
        assert_eq!(bundle.currency_plan.conversion_symbols().len(), 1);
        while let Some(batch) = bundle.feed.next_batch() {
            assert_eq!(batch.events.len(), 1);
            assert!(batch.events[0].metadata.roles.primary);
            assert!(batch.events[0].metadata.roles.conversion);
            assert_eq!(batch.events[0].metadata.series_rank, 0);
        }

        std::fs::remove_dir_all(data_dir).unwrap();
    }
}
