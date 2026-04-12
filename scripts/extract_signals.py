#!/usr/bin/env python3
"""Extract raw Telegram messages from SQLite for signal parsing."""

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone


def parse_ts(value: str) -> int:
    """Parse ISO date or datetime string to epoch milliseconds."""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"invalid date/datetime: {value!r}")


def epoch_ms_to_iso(ms: int) -> str:
    """Convert epoch milliseconds to ISO 8601 UTC string."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_query(args: argparse.Namespace) -> tuple[str, list]:
    """Build the SELECT query and parameter list from CLI args."""
    clauses = ["chat_id = ?", "removed = 0", "message IS NOT NULL"]
    params: list = [args.channel]

    if args.from_date is not None:
        clauses.append("ts >= ?")
        params.append(args.from_date)
    if args.to_date is not None:
        clauses.append("ts < ?")
        params.append(args.to_date)

    sql = f"SELECT chat_id, msg_id, ts, message, reply_to FROM tg_messages WHERE {' AND '.join(clauses)} ORDER BY ts ASC"
    return sql, params


def dedup_latest(rows: list[dict]) -> list[dict]:
    """Keep only the last version per msg_id (last-write-wins by ts order)."""
    seen: dict[int, dict] = {}
    for row in rows:
        seen[row["msg_id"]] = row
    return list(seen.values())


def decode_message(raw) -> str:
    """Decode BLOB message field to string, handling bytes or str."""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Telegram messages from SQLite to JSONL.")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--channel", required=True, type=int, help="Telegram chat_id to filter")
    parser.add_argument("--from", dest="from_date", type=parse_ts, default=None, help="Start date (inclusive, ISO format)")
    parser.add_argument("--to", dest="to_date", type=parse_ts, default=None, help="End date (exclusive, ISO format)")
    parser.add_argument("--output", default=None, help="Output JSONL file path (default: stdout)")
    parser.add_argument("--include-edits", action="store_true", help="Include all edit versions, not just latest")
    args = parser.parse_args()

    # Query the database
    sql, params = build_query(args)
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    rows = [
        {
            "chat_id": r["chat_id"],
            "msg_id": r["msg_id"],
            "ts": epoch_ms_to_iso(r["ts"]),
            "message": decode_message(r["message"]),
            "reply_to": r["reply_to"],
        }
        for r in con.execute(sql, params)
    ]
    con.close()

    # Dedup to latest version per msg_id unless edits requested
    if not args.include_edits:
        rows = dedup_latest(rows)

    # Write JSONL output
    out = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout
    try:
        for row in rows:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
    finally:
        if out is not sys.stdout:
            out.close()


if __name__ == "__main__":
    main()