from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import hashlib
import sqlite3
from typing import Any


DECISION_VERSION = "1.0.0"


class DecisionSignalStore:
    """
    Durable store for derived investment decision signals.

    These are not raw facts. They are deterministic interpretations
    of previously generated investment signals.
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
            / "decision_signals.db"
        )

        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS decision_signals (
                    decision_id TEXT PRIMARY KEY,

                    company_id TEXT NOT NULL,
                    cik TEXT NOT NULL,

                    decision_type TEXT NOT NULL,
                    decision_value TEXT NOT NULL,
                    decision_status TEXT NOT NULL,

                    confidence TEXT NOT NULL,

                    source_signal_count INTEGER NOT NULL,

                    source_state_version INTEGER NOT NULL,

                    source_event_id TEXT,
                    source_run_id TEXT,
                    source_document_id TEXT,
                    source_document_sha256 TEXT,

                    rationale TEXT NOT NULL,

                    created_at_utc TEXT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS
                    ux_decision_business_identity
                ON decision_signals(
                    company_id,
                    source_state_version,
                    decision_type
                );

                CREATE INDEX IF NOT EXISTS
                    ix_decision_company
                ON decision_signals(company_id);
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
    def _decision_id(
        company_id: str,
        state_version: int,
        decision_type: str,
    ) -> str:

        raw = (
            f"{company_id}|"
            f"{state_version}|"
            f"{decision_type}"
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
        decision_type: str,
        decision_value: str,
        decision_status: str,
        confidence: str,
        source_signal_count: int,
        source_event_id: str | None,
        source_run_id: str | None,
        source_document_id: str | None,
        source_document_sha256: str | None,
        rationale: str,
    ) -> dict[str, Any]:

        decision_id = self._decision_id(
            company_id,
            state_version,
            decision_type,
        )

        created_at = datetime.now(
            timezone.utc
        ).isoformat()

        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT *
                FROM decision_signals
                WHERE decision_id = ?
                """,
                (decision_id,),
            ).fetchone()

            if existing is not None:
                return {
                    "decision_id": existing[
                        "decision_id"
                    ],
                    "duplicate": True,
                    "created": False,
                }

            conn.execute(
                """
                INSERT INTO decision_signals (
                    decision_id,
                    company_id,
                    cik,
                    decision_type,
                    decision_value,
                    decision_status,
                    confidence,
                    source_signal_count,
                    source_state_version,
                    source_event_id,
                    source_run_id,
                    source_document_id,
                    source_document_sha256,
                    rationale,
                    created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id,
                    company_id,
                    str(cik).zfill(10),
                    decision_type,
                    decision_value,
                    decision_status,
                    confidence,
                    source_signal_count,
                    state_version,
                    source_event_id,
                    source_run_id,
                    source_document_id,
                    source_document_sha256,
                    rationale,
                    created_at,
                ),
            )

        return {
            "decision_id": decision_id,
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
                FROM decision_signals
                WHERE company_id = ?
                ORDER BY source_state_version
                """,
                (company_id,),
            ).fetchall()

        return [
            dict(row)
            for row in rows
        ]


class DecisionSignalEngine:
    """
    Converts investment signals into a deterministic decision posture.

    This layer deliberately does not claim BUY / SELL / INVEST.
    """

    def __init__(
        self,
        *,
        state_store,
        signal_store,
        decision_store=None,
    ):
        self.state_store = state_store
        self.signal_store = signal_store
        self.decision_store = (
            decision_store
            or DecisionSignalStore()
        )

    def evaluate(
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
                "decision": "INSUFFICIENT_DATA",
                "created": False,
            }

        signals = self.signal_store.list_for_company(
            company_id
        )

        current_version = int(
            state["state_version"]
        )

        signals = [
            signal
            for signal in signals
            if int(
                signal["source_state_version"]
            ) == current_version
        ]

        signal_types = {
            signal["signal_type"]
            for signal in signals
        }

        if not signals:
            decision = "INSUFFICIENT_DATA"
            confidence = "LOW"
            rationale = (
                "No current investment signals "
                "are available."
            )

        elif (
            "CAPITAL_ACTIVITY_DETECTED"
            in signal_types
            and
            "RECURRING_CAPITAL_ACTIVITY"
            in signal_types
        ):
            decision = "POSITIVE"
            confidence = "MEDIUM"
            rationale = (
                "Observed capital activity is "
                "supported by multiple funding events. "
                "This is a positive activity signal, "
                "not an investment recommendation."
            )

        elif (
            "CAPITAL_ACTIVITY_DETECTED"
            in signal_types
        ):
            decision = "MONITOR"
            confidence = "MEDIUM"
            rationale = (
                "Observed capital activity exists, "
                "but the current evidence is limited "
                "to capital-event activity."
            )

        else:
            decision = "INSUFFICIENT_DATA"
            confidence = "LOW"
            rationale = (
                "Available signals are insufficient "
                "for a stronger decision posture."
            )

        result = self.decision_store.persist(
            company_id=company_id,
            cik=state["cik"],
            state_version=current_version,
            decision_type="INVESTMENT_POSTURE",
            decision_value=decision,
            decision_status="ACTIVE",
            confidence=confidence,
            source_signal_count=len(signals),
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
            rationale=rationale,
        )

        return {
            "company_id": company_id,
            "cik": state["cik"],
            "status": "ACTIVE",
            "decision": decision,
            "confidence": confidence,
            "source_signal_count": len(signals),
            "decision_id": result[
                "decision_id"
            ],
            "duplicate": result[
                "duplicate"
            ],
        }