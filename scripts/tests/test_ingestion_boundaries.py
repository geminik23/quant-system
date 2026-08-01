import importlib.util
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
