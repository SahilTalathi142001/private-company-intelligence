from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import json
import sqlite3
import time


COMPANY_STATE_STATUSES = {
    "ACTIVE",
    "NO_DATA",
}


class CompanyStateStore:
    """
    Durable current-state store.

    Structured events remain the immutable source of truth.
    This store contains the latest deterministic projection.
    """

    def __init__(self, path=None):
        root = Path(__file__).resolve().parents[3]

        self.path = (
            Path(path)
            if path is not None
            else root
            / "data"
            / "silver"
            / "platform"
            / "company_intelligence_state.db"
        )

        self.path.parent.mkdir(parents=True, exist_ok=True)

        with self._db() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS company_state (
                    company_id TEXT PRIMARY KEY,
                    cik TEXT NOT NULL,

                    state_version INTEGER NOT NULL,

                    status TEXT NOT NULL,

                    funding_event_count INTEGER NOT NULL DEFAULT 0,

                    latest_funding_event_id TEXT,
                    latest_funding_event_type TEXT,
                    latest_funding_date TEXT,

                    latest_issuer_name TEXT,
                    latest_total_offering_amount REAL,
                    latest_total_amount_sold REAL,

                    cumulative_offering_amount REAL NOT NULL DEFAULT 0,
                    cumulative_amount_sold REAL NOT NULL DEFAULT 0,

                    last_source_event_id TEXT,
                    last_source_run_id TEXT,
                    last_source_document_id TEXT,
                    last_source_document_sha256 TEXT,

                    state_updated_at_utc TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    ix_company_state_cik
                ON company_state(cik);
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

    def get(self, company_id: str) -> dict | None:
        with self._db() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM company_state
                WHERE company_id = ?
                """,
                (company_id,),
            ).fetchone()

        return None if row is None else dict(row)

    def upsert(
        self,
        *,
        company_id: str,
        cik: str,
        state_version: int,
        status: str,
        funding_event_count: int,
        latest_funding_event_id: str | None,
        latest_funding_event_type: str | None,
        latest_funding_date: str | None,
        latest_issuer_name: str | None,
        latest_total_offering_amount: float | None,
        latest_total_amount_sold: float | None,
        cumulative_offering_amount: float | None,
        cumulative_amount_sold: float | None,
        last_source_event_id: str | None,
        last_source_run_id: str | None,
        last_source_document_id: str | None,
        last_source_document_sha256: str | None,
    ) -> dict:

        if status not in COMPANY_STATE_STATUSES:
            raise ValueError(
                f"Invalid company state status: {status}"
            )

        now = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(),
        )

        with self._db() as conn:
            existing = conn.execute(
                """
                SELECT state_version
                FROM company_state
                WHERE company_id = ?
                """,
                (company_id,),
            ).fetchone()

            if existing is not None:
                current_version = int(existing["state_version"])

                if state_version < current_version:
                    raise ValueError(
                        f"State version regression for {company_id}: "
                        f"{state_version} < {current_version}"
                    )

            conn.execute(
                """
                INSERT INTO company_state (
                    company_id,
                    cik,
                    state_version,
                    status,
                    funding_event_count,
                    latest_funding_event_id,
                    latest_funding_event_type,
                    latest_funding_date,
                    latest_issuer_name,
                    latest_total_offering_amount,
                    latest_total_amount_sold,
                    cumulative_offering_amount,
                    cumulative_amount_sold,
                    last_source_event_id,
                    last_source_run_id,
                    last_source_document_id,
                    last_source_document_sha256,
                    state_updated_at_utc
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(company_id)
                DO UPDATE SET
                    cik = excluded.cik,
                    state_version = excluded.state_version,
                    status = excluded.status,
                    funding_event_count = excluded.funding_event_count,
                    latest_funding_event_id = excluded.latest_funding_event_id,
                    latest_funding_event_type = excluded.latest_funding_event_type,
                    latest_funding_date = excluded.latest_funding_date,
                    latest_issuer_name = excluded.latest_issuer_name,
                    latest_total_offering_amount =
                        excluded.latest_total_offering_amount,
                    latest_total_amount_sold =
                        excluded.latest_total_amount_sold,
                    cumulative_offering_amount =
                        excluded.cumulative_offering_amount,
                    cumulative_amount_sold =
                        excluded.cumulative_amount_sold,
                    last_source_event_id =
                        excluded.last_source_event_id,
                    last_source_run_id =
                        excluded.last_source_run_id,
                    last_source_document_id =
                        excluded.last_source_document_id,
                    last_source_document_sha256 =
                        excluded.last_source_document_sha256,
                    state_updated_at_utc =
                        excluded.state_updated_at_utc
                """,
                (
                    company_id,
                    str(cik).zfill(10),
                    state_version,
                    status,
                    funding_event_count,
                    latest_funding_event_id,
                    latest_funding_event_type,
                    latest_funding_date,
                    latest_issuer_name,
                    latest_total_offering_amount,
                    latest_total_amount_sold,
                    cumulative_offering_amount,
                    cumulative_amount_sold,
                    last_source_event_id,
                    last_source_run_id,
                    last_source_document_id,
                    last_source_document_sha256,
                    now,
                ),
            )

        return self.get(company_id)

    def record_filing_evidence(
        self,
        *,
        company_id: str,
        cik: str,
        source_event_id: str | None,
        source_run_id: str | None,
        source_document_id: str | None,
        source_document_sha256: str | None,
    ) -> dict:
        """Remember a production filing without inventing funding amounts.

        Form 4, Form 144, and other SEC filing events are evidence that a
        document was processed. They are not Form D funding observations.
        """
        previous = self.get(company_id)
        state_version = (
            int(previous["state_version"]) + 1
            if previous and previous.get("state_version") is not None
            else 1
        )
        return self.upsert(
            company_id=company_id,
            cik=cik,
            state_version=state_version,
            status="NO_DATA",
            funding_event_count=0,
            latest_funding_event_id=None,
            latest_funding_event_type=None,
            latest_funding_date=None,
            latest_issuer_name=None,
            latest_total_offering_amount=None,
            latest_total_amount_sold=None,
            cumulative_offering_amount=0.0,
            cumulative_amount_sold=0.0,
            last_source_event_id=source_event_id,
            last_source_run_id=source_run_id,
            last_source_document_id=source_document_id,
            last_source_document_sha256=source_document_sha256,
        )

    def mark_no_data(self, company_id: str, cik: str) -> dict:
        previous = self.get(company_id)
        if previous is None:
            return {
                "company_id": company_id,
                "cik": str(cik).zfill(10),
                "status": "NO_DATA",
                "state_version": None,
            }

        return self.upsert(
            company_id=company_id,
            cik=cik,
            state_version=int(previous["state_version"]) + 1,
            status="NO_DATA",
            funding_event_count=0,
            latest_funding_event_id=None,
            latest_funding_event_type=None,
            latest_funding_date=None,
            latest_issuer_name=None,
            latest_total_offering_amount=None,
            latest_total_amount_sold=None,
            cumulative_offering_amount=0.0,
            cumulative_amount_sold=0.0,
            last_source_event_id=None,
            last_source_run_id=None,
            last_source_document_id=None,
            last_source_document_sha256=None,
        )