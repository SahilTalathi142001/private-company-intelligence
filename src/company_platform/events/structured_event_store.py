from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import hashlib
import json
import sqlite3
import time
from typing import Any


STRUCTURED_EVENT_STATUSES = {
    "PERSISTED",
    "PROCESSED",
    "FAILED",
    "SUPERSEDED",
}

SOURCE_ENVIRONMENTS = {"PRODUCTION", "TEST", "UNKNOWN"}


def classify_source_environment(
    document_id: str,
    source_document_path: str | None,
) -> str:
    document = str(document_id).strip().upper()
    path = str(source_document_path or "").replace("\\", "/").lower()
    path_parts = {part for part in path.split("/") if part}
    if document.startswith(("TEST-", "TEST_", "E2E-")) or "test" in path_parts:
        return "TEST"
    if "/data/bronze/sec/documents/" in f"/{path.strip('/')}":
        return "PRODUCTION"
    return "UNKNOWN"


class StructuredEventStore:
    """
    Durable store for derived business events produced by document understanding.

    This store is intentionally separate from EventStore:
    - EventStore = inbound platform events / orchestration queue
    - StructuredEventStore = derived business events
    """

    def __init__(self, path=None):
        root = Path(__file__).resolve().parents[3]

        self.path = (
            Path(path)
            if path is not None
            else root / "data" / "silver" / "platform" / "structured_events.db"
        )

        self.path.parent.mkdir(parents=True, exist_ok=True)

        with self._db() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS structured_events (
                    event_id TEXT PRIMARY KEY,

                    event_type TEXT NOT NULL,
                    event_version TEXT NOT NULL,

                    status TEXT NOT NULL,

                    company_id TEXT NOT NULL,
                    cik TEXT NOT NULL,

                    document_id TEXT NOT NULL,
                    document_class TEXT,

                    source_document_path TEXT,
                    source_document_sha256 TEXT,
                    source_environment TEXT NOT NULL DEFAULT 'UNKNOWN',

                    source_event_id TEXT,
                    source_run_id TEXT,

                    event_confidence REAL,

                    facts_json TEXT NOT NULL,

                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS
                    ux_structured_event_business_identity
                ON structured_events (
                    company_id,
                    document_id,
                    event_type
                );

                CREATE INDEX IF NOT EXISTS
                    ix_structured_events_company
                ON structured_events(company_id);

                CREATE INDEX IF NOT EXISTS
                    ix_structured_events_document
                ON structured_events(document_id);

                CREATE INDEX IF NOT EXISTS
                    ix_structured_events_status
                ON structured_events(status);
                """
            )
            columns = {
                row["name"]
                for row in conn.execute(
                    "PRAGMA table_info(structured_events)"
                )
            }
            if "source_environment" not in columns:
                conn.execute(
                    "ALTER TABLE structured_events "
                    "ADD COLUMN source_environment TEXT NOT NULL DEFAULT 'UNKNOWN'"
                )
            conn.execute(
                """
                UPDATE structured_events
                SET source_environment = CASE
                    WHEN upper(document_id) LIKE 'TEST-%'
                      OR upper(document_id) LIKE 'TEST\\_%' ESCAPE '\\'
                      OR upper(document_id) LIKE 'E2E-%'
                      OR lower(replace(source_document_path, '\\', '/'))
                         LIKE '%/data/test/%'
                    THEN 'TEST'
                    WHEN lower(replace(source_document_path, '\\', '/'))
                         LIKE '%/data/bronze/sec/documents/%'
                    THEN 'PRODUCTION'
                    ELSE 'UNKNOWN'
                END
                WHERE source_environment = 'UNKNOWN'
                """
            )

    @contextmanager
    def _db(self):
        conn = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")

        try:
            yield conn
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def build_event_id(
        *,
        company_id: str,
        document_id: str,
        event_type: str,
        source_document_sha256: str,
    ) -> str:
        """
        Deterministic event identity.

        Same company + document + event type + source content
        produces the same event ID.
        """

        identity = "|".join(
            [
                str(company_id),
                str(document_id),
                str(event_type),
                str(source_document_sha256),
            ]
        )

        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    def persist(
        self,
        *,
        event_type: str,
        company_id: str,
        cik: str,
        document_id: str,
        document_class: str,
        source_document_path: str | None,
        source_document_sha256: str,
        source_event_id: str | None,
        source_run_id: str | None,
        event_confidence: float,
        facts: dict[str, Any],
        source_environment: str | None = None,
        event_version: str = "1.0",
    ) -> dict:
        environment = (
            source_environment.strip().upper()
            if source_environment is not None
            else classify_source_environment(
                document_id,
                source_document_path,
            )
        )
        if environment not in SOURCE_ENVIRONMENTS:
            raise ValueError(
                f"Invalid source environment: {source_environment!r}"
            )

        event_id = self.build_event_id(
            company_id=company_id,
            document_id=document_id,
            event_type=event_type,
            source_document_sha256=source_document_sha256,
        )

        facts_json = json.dumps(
            facts,
            sort_keys=True,
            separators=(",", ":"),
        )

        now = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(),
        )

        row = {
            "event_id": event_id,
            "event_type": event_type,
            "event_version": event_version,
            "status": "PERSISTED",
            "company_id": company_id,
            "cik": str(cik).zfill(10),
            "document_id": document_id,
            "document_class": document_class,
            "source_document_path": source_document_path,
            "source_document_sha256": source_document_sha256,
            "source_environment": environment,
            "source_event_id": source_event_id,
            "source_run_id": source_run_id,
            "event_confidence": float(event_confidence),
            "facts_json": facts_json,
            "created_at_utc": now,
            "updated_at_utc": now,
        }

        with self._db() as conn:
            existing = conn.execute(
                """
                SELECT *
                FROM structured_events
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()

            if existing:
                existing = dict(existing)

                if (
                    existing["company_id"] != company_id
                    or existing["document_id"] != document_id
                    or existing["event_type"] != event_type
                    or existing["source_document_sha256"]
                    != source_document_sha256
                ):
                    raise ValueError(
                        f"Structured event ID collision: {event_id}"
                    )

                return {
                    **existing,
                    "duplicate": True,
                }

            conn.execute(
                """
                INSERT INTO structured_events (
                    event_id,
                    event_type,
                    event_version,
                    status,
                    company_id,
                    cik,
                    document_id,
                    document_class,
                    source_document_path,
                    source_document_sha256,
                    source_environment,
                    source_event_id,
                    source_run_id,
                    event_confidence,
                    facts_json,
                    created_at_utc,
                    updated_at_utc
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    row["event_id"],
                    row["event_type"],
                    row["event_version"],
                    row["status"],
                    row["company_id"],
                    row["cik"],
                    row["document_id"],
                    row["document_class"],
                    row["source_document_path"],
                    row["source_document_sha256"],
                    row["source_environment"],
                    row["source_event_id"],
                    row["source_run_id"],
                    row["event_confidence"],
                    row["facts_json"],
                    row["created_at_utc"],
                    row["updated_at_utc"],
                ),
            )

        return {
            **row,
            "duplicate": False,
        }

    def update_facts(self, event_id: str, facts: dict[str, Any]) -> bool:
        """Replace facts on an existing event without creating a second row."""
        facts_json = json.dumps(
            facts,
            sort_keys=True,
            separators=(",", ":"),
        )
        now = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(),
        )
        with self._db() as conn:
            cursor = conn.execute(
                """
                UPDATE structured_events
                SET facts_json = ?, updated_at_utc = ?
                WHERE event_id = ?
                """,
                (facts_json, now, str(event_id)),
            )
            return cursor.rowcount == 1

    def get(self, event_id: str) -> dict | None:
        with self._db() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM structured_events
                WHERE event_id = ?
                """,
                (str(event_id),),
            ).fetchone()

        return None if row is None else dict(row)

    def get_by_business_identity(
        self,
        *,
        company_id: str,
        document_id: str,
        event_type: str,
    ) -> dict | None:
        with self._db() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM structured_events
                WHERE company_id = ?
                  AND document_id = ?
                  AND event_type = ?
                """,
                (
                    company_id,
                    document_id,
                    event_type,
                ),
            ).fetchone()

        return None if row is None else dict(row)

    def list_events(
        self,
        *,
        company_id: str | None = None,
        status: str | None = None,
        source_environment: str | None = None,
    ):
        import pandas as pd

        if status is not None and status not in STRUCTURED_EVENT_STATUSES:
            raise ValueError(f"Invalid structured event status: {status}")
        if (
            source_environment is not None
            and source_environment not in SOURCE_ENVIRONMENTS
        ):
            raise ValueError(
                f"Invalid source environment: {source_environment}"
            )

        query = "SELECT * FROM structured_events"
        params = []
        conditions = []

        if company_id is not None:
            conditions.append("company_id = ?")
            params.append(company_id)

        if status is not None:
            conditions.append("status = ?")
            params.append(status)

        if source_environment is not None:
            conditions.append("source_environment = ?")
            params.append(source_environment)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY created_at_utc, event_id"

        with self._db() as conn:
            rows = conn.execute(query, params).fetchall()

        return pd.DataFrame(
            [dict(row) for row in rows]
        )

    def update_status(
        self,
        event_id: str,
        status: str,
    ) -> dict:
        if status not in STRUCTURED_EVENT_STATUSES:
            raise ValueError(
                f"Invalid structured event status: {status}"
            )

        now = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(),
        )

        with self._db() as conn:
            cursor = conn.execute(
                """
                UPDATE structured_events
                SET status = ?,
                    updated_at_utc = ?
                WHERE event_id = ?
                """,
                (
                    status,
                    now,
                    str(event_id),
                ),
            )

            if cursor.rowcount != 1:
                raise KeyError(
                    f"Structured event not found: {event_id}"
                )

        return self.get(event_id)