use std::collections::BTreeMap;
use std::path::Path;

use data_preprocess::DataError;
use qs_symbols::SymbolRegistry;

use crate::{MarketLoadError, Result, ensure_not_cancelled_mut, resolve_partition_value};

/// Resolves source-specific names without changing canonical replay identity.
#[derive(Clone, Copy)]
pub struct SymbolPartitionResolver<'a> {
    registry: &'a SymbolRegistry,
    source_symbols: Option<&'a BTreeMap<String, String>>,
}

impl<'a> SymbolPartitionResolver<'a> {
    pub fn new(registry: &'a SymbolRegistry) -> Self {
        Self {
            registry,
            source_symbols: None,
        }
    }

    pub fn with_source_symbols(
        registry: &'a SymbolRegistry,
        source_symbols: &'a BTreeMap<String, String>,
    ) -> Self {
        Self {
            registry,
            source_symbols: Some(source_symbols),
        }
    }

    /// Resolve physical tick coordinates before opening an exact-coordinate cursor.
    pub fn resolve_tick_source(
        &self,
        data_dir: &str,
        exchange: &str,
        symbol: &str,
        is_cancelled: &mut dyn FnMut() -> bool,
    ) -> Result<(String, String)> {
        let exchange =
            resolve_partition_value(data_dir, "ticks", "exchange", exchange, "", is_cancelled)?;
        let physical = self.resolve(data_dir, "ticks", &exchange, symbol, is_cancelled)?;
        Ok((exchange, physical))
    }

    pub fn canonical_source_symbol(&self, source: &str) -> String {
        if let Some(bindings) = self.source_symbols
            && let Some((canonical, _)) = bindings
                .iter()
                .find(|(_, physical)| physical.eq_ignore_ascii_case(source))
        {
            return canonical.clone();
        }
        self.registry.normalize_or_passthrough(source)
    }

    pub fn resolve(
        &self,
        data_dir: &str,
        subdir: &str,
        exchange: &str,
        symbol: &str,
        is_cancelled: &mut dyn FnMut() -> bool,
    ) -> Result<String> {
        let candidates = partition_names(data_dir, subdir, exchange, is_cancelled)?;
        self.choose(
            symbol,
            &candidates,
            exchange,
            match subdir {
                "ticks" => "tick",
                "bars" => "bar",
                other => other,
            },
        )
    }

    pub fn choose(
        &self,
        symbol: &str,
        candidates: &[String],
        exchange: &str,
        data_type: &str,
    ) -> Result<String> {
        let canonical = self.registry.normalize_or_passthrough(symbol);
        let explicit = self
            .source_symbols
            .and_then(|bindings| bindings.get(&canonical));
        let matches = candidates
            .iter()
            .filter(|physical| {
                explicit.map_or_else(
                    || self.canonical_source_symbol(physical) == canonical,
                    |selected| physical.eq_ignore_ascii_case(selected),
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [physical] => Ok(physical.clone()),
            [] if explicit.is_some() => Err(MarketLoadError::InvalidSeries(format!(
                "configured source symbol '{}' for '{canonical}' is absent on '{exchange}'",
                explicit.unwrap()
            ))),
            [] => Err(MarketLoadError::NoDataFound {
                symbol: canonical,
                exchange: exchange.into(),
                data_type: data_type.into(),
            }),
            _ => Err(MarketLoadError::AmbiguousSymbol {
                symbol: canonical,
                candidates: matches,
            }),
        }
    }
}

pub(crate) fn partition_names(
    data_dir: &str,
    subdir: &str,
    exchange: &str,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<Vec<String>> {
    ensure_not_cancelled_mut(is_cancelled)?;
    let directory = Path::new(data_dir)
        .join(subdir)
        .join(format!("exchange={exchange}"));
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(MarketLoadError::Data(DataError::Io(error))),
    };
    let mut names = Vec::new();
    for entry in entries {
        ensure_not_cancelled_mut(is_cancelled)?;
        let entry = entry.map_err(DataError::Io)?;
        if !entry.file_type().map_err(DataError::Io)?.is_dir() {
            continue;
        }
        if let Some(name) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("symbol="))
        {
            names.push(name.to_owned());
        }
    }
    names.sort();
    Ok(names)
}

/// Discover physical tick candidates, leaving ambiguity decisions to the required instrument.
pub fn discover_tick_partitions(
    data_dir: &str,
    exchange: &str,
    resolver: &SymbolPartitionResolver<'_>,
    is_cancelled: &mut dyn FnMut() -> bool,
) -> Result<BTreeMap<String, Vec<(String, String)>>> {
    let disk_exchange =
        resolve_partition_value(data_dir, "ticks", "exchange", exchange, "", is_cancelled)?;
    let candidates = partition_names(data_dir, "ticks", &disk_exchange, is_cancelled)?;
    let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for physical in &candidates {
        ensure_not_cancelled_mut(is_cancelled)?;
        let directory = Path::new(data_dir)
            .join("ticks")
            .join(format!("exchange={disk_exchange}"))
            .join(format!("symbol={physical}"));
        let mut has_parquet = false;
        for entry in std::fs::read_dir(directory).map_err(DataError::Io)? {
            ensure_not_cancelled_mut(is_cancelled)?;
            let entry = entry.map_err(DataError::Io)?;
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                has_parquet = true;
                break;
            }
        }
        if has_parquet {
            grouped
                .entry(resolver.canonical_source_symbol(physical))
                .or_default()
                .push(physical.clone());
        }
    }
    if let Some(bindings) = resolver.source_symbols {
        for canonical in bindings.keys() {
            grouped.entry(canonical.clone()).or_default();
        }
    }
    let mut result = BTreeMap::new();
    for (canonical, physicals) in grouped {
        let selected = match resolver.choose(&canonical, &physicals, &disk_exchange, "tick") {
            Ok(selected) => vec![selected],
            Err(MarketLoadError::AmbiguousSymbol { candidates, .. }) => candidates,
            Err(MarketLoadError::InvalidSeries(_)) => Vec::new(),
            Err(error) => return Err(error),
        };
        result.insert(
            canonical,
            selected
                .into_iter()
                .map(|symbol| (disk_exchange.clone(), symbol))
                .collect(),
        );
    }
    Ok(result)
}
