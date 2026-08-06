import importlib.util
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "check_ingestion_boundaries.py"
SPEC = importlib.util.spec_from_file_location("check_ingestion_boundaries", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"unable to load {SCRIPT_PATH}")
BOUNDARIES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BOUNDARIES)


class IngestionBoundaryTests(unittest.TestCase):
    def package(self, crate: Path, dependencies: list[dict]) -> dict:
        return {
            "manifest_path": str(crate / "Cargo.toml"),
            "dependencies": dependencies,
        }

    def target(self) -> Path:
        return BOUNDARIES.TARGETS[0]

    def test_rejects_direct_package_dependency(self):
        metadata = {
            "packages": [
                self.package(
                    self.target(),
                    [{"name": "qs-signal-parser", "rename": None, "path": None}],
                )
            ]
        }

        errors = BOUNDARIES.check_dependencies(metadata)

        self.assertEqual(len(errors), 1)
        self.assertIn("qs-signal-parser", errors[0])

    def test_rejects_renamed_package_dependency(self):
        metadata = {
            "packages": [
                self.package(
                    self.target(),
                    [{"name": "qs-signal-parser", "rename": "parser", "path": None}],
                )
            ]
        }

        errors = BOUNDARIES.check_dependencies(metadata)

        self.assertEqual(len(errors), 1)
        self.assertIn("parser", errors[0])

    def test_rejects_dependency_that_resolves_to_parser_path(self):
        metadata = {
            "packages": [
                self.package(
                    self.target(),
                    [
                        {
                            "name": "unexpected-name",
                            "rename": "local-parser",
                            "path": str(BOUNDARIES.PARSER_CRATE),
                        }
                    ],
                )
            ]
        }

        errors = BOUNDARIES.check_dependencies(metadata)

        self.assertEqual(len(errors), 1)
        self.assertIn("local-parser", errors[0])

    def test_accepts_unrelated_dependency(self):
        metadata = {
            "packages": [
                self.package(
                    self.target(),
                    [{"name": "serde", "rename": None, "path": None}],
                )
            ]
        }

        self.assertEqual(BOUNDARIES.check_dependencies(metadata), [])

    def test_rejects_trading_types_in_generic_ingestion(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.rs").write_text("use qs_core::RawSignal;", encoding="utf-8")

            errors = BOUNDARIES.check_generic_ingestion_sources(root)

        self.assertEqual(len(errors), 2)
        self.assertTrue(any("qs_core" in error for error in errors))
        self.assertTrue(any("RawSignal" in error for error in errors))

    def test_rejects_storage_and_instrument_types_in_generic_ingestion(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "payload.rs").write_text(
                "use qs_symbols::InstrumentId; use std::fs;",
                encoding="utf-8",
            )

            errors = BOUNDARIES.check_generic_ingestion_sources(root)

        self.assertEqual(len(errors), 3)
        self.assertTrue(any("qs_symbols" in error for error in errors))
        self.assertTrue(any("InstrumentId" in error for error in errors))
        self.assertTrue(any("std::fs" in error for error in errors))

    def test_rejects_parser_and_service_runtime_in_generic_ingestion(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.rs").write_text(
                "use crate::TemplateParser; use tower::Service;",
                encoding="utf-8",
            )

            errors = BOUNDARIES.check_generic_ingestion_sources(root)

        self.assertEqual(len(errors), 2)
        self.assertTrue(any("TemplateParser" in error for error in errors))
        self.assertTrue(any("tower" in error for error in errors))

    def test_accepts_source_only_generic_ingestion(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "source.rs").write_text("pub struct SourceEvent;", encoding="utf-8")

            errors = BOUNDARIES.check_generic_ingestion_sources(root)

        self.assertEqual(errors, [])

    def test_accepts_core_signal_only_in_normalization_allowlist(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "signal.rs").write_text("use qs_core::RawSignal;", encoding="utf-8")
            (root / "raw_signals_v1.rs").write_text(
                "use qs_core::RawSignal;",
                encoding="utf-8",
            )

            errors = BOUNDARIES.check_normalization_sources(root)

        self.assertEqual(errors, [])

    def test_rejects_core_signal_outside_normalization_allowlist(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "graph.rs").write_text("use qs_core::RawSignal;", encoding="utf-8")

            errors = BOUNDARIES.check_normalization_sources(root)

        self.assertEqual(len(errors), 1)
        self.assertTrue(any("qs_core" in error for error in errors))

    def test_rejects_runtime_and_telegram_in_normalization(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "context.rs").write_text(
                "use tokio::task; use crate::types::RawTgMessage;",
                encoding="utf-8",
            )

            errors = BOUNDARIES.check_normalization_sources(root)

        self.assertEqual(len(errors), 3)
        self.assertTrue(any("tokio" in error for error in errors))
        self.assertTrue(any("crate::types" in error for error in errors))
        self.assertTrue(any("RawTgMessage" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
