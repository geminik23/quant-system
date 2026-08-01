import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "extract_signals.py"
PRIMARY_CHANNEL = 9001
OTHER_CHANNEL = 9002


class ExtractSignalsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = Path(cls._temp_dir.name) / "signals.sqlite"

        rows = [
            (PRIMARY_CHANNEL, 101, 1_704_067_200_123, "first version", None, 0),
            (PRIMARY_CHANNEL, 102, 1_704_070_800_000, "second message", 101, 0),
            (OTHER_CHANNEL, 201, 1_704_074_400_000, "other channel", None, 0),
            (PRIMARY_CHANNEL, 103, 1_704_078_000_000, "removed message", None, 1),
            (PRIMARY_CHANNEL, 104, 1_704_081_600_000, None, None, 0),
            (PRIMARY_CHANNEL, 101, 1_704_153_600_000, "latest version", None, 0),
            (PRIMARY_CHANNEL, 105, 1_704_240_000_456, sqlite3.Binary(b"prefix\xffsuffix"), 102, 0),
        ]

        with sqlite3.connect(cls.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE tg_messages (
                    chat_id INTEGER NOT NULL,
                    msg_id INTEGER NOT NULL,
                    ts INTEGER NOT NULL,
                    message BLOB,
                    reply_to INTEGER,
                    removed INTEGER NOT NULL
                )
                """
            )
            connection.executemany(
                "INSERT INTO tg_messages (chat_id, msg_id, ts, message, reply_to, removed) VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )

    @classmethod
    def tearDownClass(cls):
        cls._temp_dir.cleanup()

    def run_extract(self, *extra_args, channel=PRIMARY_CHANNEL):
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--db",
                str(self.db_path),
                "--channel",
                str(channel),
                *extra_args,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return [json.loads(line) for line in result.stdout.splitlines()]

    def test_excludes_removed_rows_and_null_messages(self):
        rows = self.run_extract("--include-edits")

        self.assertEqual([row["msg_id"] for row in rows], [101, 102, 101, 105])
        self.assertNotIn(103, {row["msg_id"] for row in rows})
        self.assertNotIn(104, {row["msg_id"] for row in rows})

    def test_filters_by_channel_and_date_range(self):
        primary_rows = self.run_extract(
            "--from",
            "2024-01-02",
            "--to",
            "2024-01-03",
            "--include-edits",
        )
        other_rows = self.run_extract("--include-edits", channel=OTHER_CHANNEL)

        self.assertEqual(
            [(row["chat_id"], row["msg_id"], row["message"]) for row in primary_rows],
            [(PRIMARY_CHANNEL, 101, "latest version")],
        )
        self.assertEqual(
            [(row["chat_id"], row["msg_id"], row["message"]) for row in other_rows],
            [(OTHER_CHANNEL, 201, "other channel")],
        )

    def test_converts_epoch_milliseconds_to_utc_iso_strings(self):
        rows = self.run_extract("--include-edits")

        self.assertEqual(
            [row["ts"] for row in rows],
            [
                "2024-01-01T00:00:00Z",
                "2024-01-01T01:00:00Z",
                "2024-01-02T00:00:00Z",
                "2024-01-03T00:00:00Z",
            ],
        )

    def test_default_selects_latest_version_in_first_key_order(self):
        rows = self.run_extract()

        self.assertEqual(
            [(row["msg_id"], row["message"]) for row in rows],
            [
                (101, "latest version"),
                (102, "second message"),
                (105, "prefix\ufffdsuffix"),
            ],
        )

    def test_include_edits_retains_versions_in_sql_timestamp_order(self):
        rows = self.run_extract("--include-edits")

        self.assertEqual(
            [(row["msg_id"], row["message"]) for row in rows],
            [
                (101, "first version"),
                (102, "second message"),
                (101, "latest version"),
                (105, "prefix\ufffdsuffix"),
            ],
        )

    def test_decodes_blob_with_utf8_replacement(self):
        rows = self.run_extract()
        blob_row = next(row for row in rows if row["msg_id"] == 105)

        self.assertEqual(blob_row["message"], "prefix\ufffdsuffix")
        self.assertEqual(blob_row["reply_to"], 102)


if __name__ == "__main__":
    unittest.main()
