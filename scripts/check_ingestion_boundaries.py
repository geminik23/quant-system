#!/usr/bin/env python3
"""Check Telegram compatibility and generic ingestion source boundaries."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PARSER_CRATE = (ROOT / "crates/signal-parser").resolve()
GENERIC_INGESTION = PARSER_CRATE / "src/ingestion"
TARGETS = (
    ROOT / "crates/core",
    ROOT / "crates/backtest",
    ROOT / "crates/backtest-api",
    ROOT / "crates/service",
    ROOT / "crates/market-data-api",
)
FORBIDDEN_PACKAGE = "qs-signal-parser"
FORBIDDEN_SYMBOLS = (
    "RawTgMessage",
    "MessageParseOutcome",
    "ParseBatchResult",
    "ChannelParser",
    "ParserRegistry",
    "SignalHandler",
    "SignalContext",
)
GENERIC_FORBIDDEN_SYMBOLS = (
    "RawTgMessage",
    "MessageParseOutcome",
    "ParseBatchResult",
    "ParseContext",
    "ParseFailure",
    "ParsedAction",
    "SkipReason",
    "ChannelParser",
    "TemplateParser",
    "ParserRegistry",
    "SignalParserError",
    "SignalHandler",
    "SignalContext",
    "LoggingHandler",
    "NoopHandler",
    "LlmClient",
    "MarketQuote",
    "OfflineArgs",
    "OfflineRunner",
    "OnlineServer",
    "load_parsers",
    "parse_messages",
    "crate::config",
    "crate::handler",
    "crate::offline",
    "crate::online",
    "crate::parser",
    "crate::pipeline",
    "crate::registry",
    "crate::template",
    "crate::types",
    "SourceState",
    "SourceHistory",
    "RevisionPolicy",
    "DuplicatePolicy",
    "ConflictPolicy",
    "SourceCheckpoint",
    "ContentHash",
    "RawSignal",
    "TradeIntent",
    "InstrumentId",
    "ExecutionVenue",
    "StrategyRuntime",
    "qs_core",
    "qs_symbols",
    "qs_backtest",
    "qs_service",
    "qs_market_data",
    "tokio",
    "axum",
    "tower",
    "tower_http",
    "xrpc",
    "clap",
    "std::io",
    "std::net",
    "std::path",
    "polars",
    "duckdb",
    "ctrader_fix",
    "std::fs",
)


def load_metadata() -> dict:
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "cargo metadata failed")
    return json.loads(result.stdout)


def check_dependencies(metadata: dict) -> list[str]:
    target_manifests = {
        (crate / "Cargo.toml").resolve(): crate.relative_to(ROOT) for crate in TARGETS
    }
    errors: list[str] = []

    for package in metadata.get("packages", []):
        manifest = Path(package["manifest_path"]).resolve()
        crate = target_manifests.get(manifest)
        if crate is None:
            continue

        for dependency in package.get("dependencies", []):
            dependency_path = dependency.get("path")
            resolves_to_parser = (
                dependency_path is not None
                and Path(dependency_path).resolve() == PARSER_CRATE
            )
            if dependency.get("name") == FORBIDDEN_PACKAGE or resolves_to_parser:
                visible_name = dependency.get("rename") or dependency.get("name")
                errors.append(
                    f"{crate}/Cargo.toml: forbidden dependency {visible_name}"
                )
    return errors


def check_source_tree(
    root: Path,
    symbols: tuple[str, ...],
    description: str,
) -> list[str]:
    errors: list[str] = []
    for source in sorted(root.rglob("*.rs")):
        text = source.read_text(encoding="utf-8")
        for symbol in symbols:
            if symbol in text:
                try:
                    display_path = source.relative_to(ROOT)
                except ValueError:
                    display_path = source
                errors.append(f"{display_path}: forbidden {description} symbol {symbol}")
    return errors


def check_sources(crate: Path) -> list[str]:
    return check_source_tree(
        crate / "src",
        FORBIDDEN_SYMBOLS,
        "Telegram parser",
    )


def check_generic_ingestion_sources(root: Path = GENERIC_INGESTION) -> list[str]:
    return check_source_tree(
        root,
        GENERIC_FORBIDDEN_SYMBOLS,
        "generic ingestion",
    )


def main() -> int:
    try:
        metadata = load_metadata()
    except (RuntimeError, json.JSONDecodeError) as error:
        print(f"unable to inspect Cargo dependencies: {error}", file=sys.stderr)
        return 1

    errors = check_dependencies(metadata)
    for crate in TARGETS:
        errors.extend(check_sources(crate))
    errors.extend(check_generic_ingestion_sources())
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("ingestion boundary check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
