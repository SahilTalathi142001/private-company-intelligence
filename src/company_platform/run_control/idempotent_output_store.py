"""SQLite idempotency ledger for task outputs.

Use a deterministic output_key (for example run_id/component_id/business_date).
The ledger's insert is atomic: retries return the original committed payload.
For file/object-store sinks, write to a deterministic staging path and publish
atomically before recording the ledger entry; this class alone cannot make an
external filesystem and SQLite one distributed transaction.
"""
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone


class IdempotentOutputStore:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._db() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS committed_outputs (
                output_key TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                committed_at_utc TEXT NOT NULL
            )""")

    def _db(self):
        c = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        c.execute("PRAGMA busy_timeout=30000")
        return c

    def commit_once(self, output_key, task_id, payload):
        if not output_key or not task_id:
            raise ValueError("output_key and task_id are required")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        now = datetime.now(timezone.utc).isoformat()
        with self._db() as c:
            c.execute("BEGIN IMMEDIATE")
            cur = c.execute("INSERT OR IGNORE INTO committed_outputs VALUES(?,?,?,?)",
                            (str(output_key), str(task_id), encoded, now))
            row = c.execute("SELECT task_id,payload_json,committed_at_utc FROM committed_outputs WHERE output_key=?",
                            (str(output_key),)).fetchone()
            c.commit()
        return {"created": cur.rowcount == 1, "task_id": row[0],
                "payload": json.loads(row[1]), "committed_at_utc": row[2]}

    def get(self, output_key):
        with self._db() as c:
            row = c.execute("SELECT task_id,payload_json,committed_at_utc FROM committed_outputs WHERE output_key=?",
                            (str(output_key),)).fetchone()
        if row is None:
            return None
        return {"task_id": row[0], "payload": json.loads(row[1]), "committed_at_utc": row[2]}
