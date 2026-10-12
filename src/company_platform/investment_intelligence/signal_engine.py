from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sqlite3
from typing import Any

from company_platform.company_intelligence.company_state_store import (
    CompanyStateStore,
)


SIGNAL_VERSION = "1.0.0"


@dataclass(frozen=True)
class InvestmentSignal:
    signal_id: str
    company_id: str
    cik: str

    signal_type: str
    signal_value: str
    signal_status: str
    confidence: str

    source_scope: str
    source_state_version: int

    source_event_id: str | None
    source_run_id: str | None
    source_document_id: str | None
    source_document_sha256: str | None

    reason: str
    created_at_utc: str


class InvestmentSignalStore:
    """
    Durable store for deterministic investment signals.

    Signals are derived artifacts.
    Structured business events remain the immutable source of truth.
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
            / "investment_signals.db"
        )

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS investment_signals (
                    signal_id TEXT PRIMARY KEY,

                    company_id TEXT NOT NULL,
                    cik TEXT NOT NULL,

                    signal_type TEXT NOT NULL,
                    signal_value TEXT NOT NULL,
                    signal_status TEXT NOT NULL,
                    confidence TEXT NOT NULL,

                    source_scope TEXT NOT NULL,
                    source_state_version INTEGER NOT NULL,

                    source_event_id TEXT,
                    source_run_id TEXT,
                    source_document_id TEXT,
                    source_document_sha256 TEXT,

                    reason TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS
                    ux_investment_signal_business_identity
                ON investment_signals(
                    company_id,
                    source_state_version,
                    signal_type
                );

                CREATE INDEX IF NOT EXISTS
                    ix_investment_signals_company
                ON investment_signals(company_id);
                """
            )

    def _connect(self):
        conn = sqlite3.connect(
            self.path,
            timeout=30,
        )

        conn.row_factory = sqlite3.Row

        conn.execute(
            "PRAGMA busy_timeout=30000"
        )

        conn.execute(
            "PRAGMA journal_mode=WAL"
        )

        return conn

    @staticmethod
    def _signal_id(
        company_id: str,
        state_version: int,
        signal_type: str,
    ) -> str:
        raw = (
            f"{company_id}|"
            f"{state_version}|"
            f"{signal_type}"
        )

        return hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()

    def persist(
        self,
        *,
        company_id: str,
        cik: str,
        state_version: int,
        signal_type: str,
        signal_value: str,
        signal_status: str,
        confidence: str,
        source_scope: str,
        source_event_id: str | None,
        source_run_id: str | None,
        source_document_id: str | None,
        source_document_sha256: str | None,
        reason: str,
    ) -> dict[str, Any]:

        signal_id = self._signal_id(
            company_id,
            state_version,
            signal_type,
        )

        created_at = datetime.now(
            timezone.utc
        ).isoformat()

        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT *
                FROM investment_signals
                WHERE signal_id = ?
                """,
                (signal_id,),
            ).fetchone()

            if existing is not None:
                return {
                    "signal_id": existing["signal_id"],
                    "duplicate": True,
                    "created": False,
                }

            conn.execute(
                """
                INSERT INTO investment_signals (
                    signal_id,
                    company_id,
                    cik,
                    signal_type,
                    signal_value,
                    signal_status,
                    confidence,
                    source_scope,
                    source_state_version,
                    source_event_id,
                    source_run_id,
                    source_document_id,
                    source_document_sha256,
                    reason,
                    created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    company_id,
                    str(cik).zfill(10),
                    signal_type,
                    signal_value,
                    signal_status,
                    confidence,
                    source_scope,
                    state_version,
                    source_event_id,
                    source_run_id,
                    source_document_id,
                    source_document_sha256,
                    reason,
                    created_at,
                ),
            )

        return {
            "signal_id": signal_id,
            "duplicate": False,
            "created": True,
        }

    def list_for_company(
        self,
        company_id: str,
    ) -> list[dict[str, Any]]:

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM investment_signals
                WHERE company_id = ?
                ORDER BY source_state_version, signal_type
                """,
                (company_id,),
            ).fetchall()

        return [
            dict(row)
            for row in rows
        ]


class InvestmentSignalEngine:
    """
    Converts company intelligence state into deterministic
    investment-relevant signals.

    No recommendation is produced here.
    """

    def __init__(
        self,
        *,
        state_store=None,
        signal_store=None,
    ):
        self.state_store = (
            state_store
            or CompanyStateStore()
        )

        self.signal_store = (
            signal_store
            or InvestmentSignalStore()
        )

    def generate(
        self,
        company_id: str,
    ) -> dict[str, Any]:

        state = self.state_store.get(
            company_id
        )

        if state is None:
            return {
                "company_id": company_id,
                "status": "NO_DATA",
                "signals_created": 0,
                "signals": [],
            }

        if state["status"] != "ACTIVE":
            return {
                "company_id": company_id,
                "status": "NO_DATA",
                "signals_created": 0,
                "signals": [],
            }

        signals = []

        signals.append(
            self._create_signal(
                state,
                signal_type="CAPITAL_ACTIVITY_DETECTED",
                signal_value="TRUE",
                signal_status="ACTIVE",
                confidence="HIGH",
                reason=(
                    "Company has at least one observed "
                    "SEC Form D funding event."
                ),
            )
        )

        if state["funding_event_count"] > 1:
            signals.append(
                self._create_signal(
                    state,
                    signal_type="RECURRING_CAPITAL_ACTIVITY",
                    signal_value="TRUE",
                    signal_status="ACTIVE",
                    confidence="HIGH",
                    reason=(
                        "Company has multiple observed "
                        "funding events."
                    ),
                )
            )

        sold = float(
            state["cumulative_amount_sold"] or 0
        )

        offering = float(
            state["cumulative_offering_amount"] or 0
        )

        if offering > 0:
            sold_ratio = sold / offering

            if sold_ratio > 0:
                signals.append(
                    self._create_signal(
                        state,
                        signal_type="CAPITAL_RAISE_EXECUTION",
                        signal_value=f"{sold_ratio:.6f}",
                        signal_status="ACTIVE",
                        confidence="HIGH",
                        reason=(
                            "Observed amount sold is "
                            "greater than zero relative "
                            "to observed offering amount."
                        ),
                    )
                )

        return {
            "company_id": company_id,
            "cik": state["cik"],
            "status": "ACTIVE",
            "signals_created": len(signals),
            "signals": signals,
        }

    def _create_signal(
        self,
        state: dict[str, Any],
        *,
        signal_type: str,
        signal_value: str,
        signal_status: str,
        confidence: str,
        reason: str,
    ) -> dict[str, Any]:

        result = self.signal_store.persist(
            company_id=state["company_id"],
            cik=state["cik"],
            state_version=int(
                state["state_version"]
            ),
            signal_type=signal_type,
            signal_value=signal_value,
            signal_status=signal_status,
            confidence=confidence,
            source_scope="COMPANY_INTELLIGENCE_STATE",
            source_event_id=state[
                "last_source_event_id"
            ],
            source_run_id=state[
                "last_source_run_id"
            ],
            source_document_id=state[
                "last_source_document_id"
            ],
            source_document_sha256=state[
                "last_source_document_sha256"
            ],
            reason=reason,
        )

        return result