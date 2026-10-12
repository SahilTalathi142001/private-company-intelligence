from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json

import pandas as pd

from company_platform.company_intelligence.company_state_store import (
    CompanyStateStore,
)

from company_platform.investment_intelligence.signal_engine import (
    InvestmentSignalEngine,
    InvestmentSignalStore,
)

from company_platform.investment_intelligence.decision_engine import (
    DecisionSignalEngine,
    DecisionSignalStore,
)


class UnifiedInvestmentIntelligence:
    """
    Builds a company-level unified investment intelligence artifact.

    Inputs:
        Company Intelligence State
        Investment Signals
        Decision Signals

    Output:
        One current intelligence record per company.
    """

    def __init__(
        self,
        *,
        state_store=None,
        signal_store=None,
        decision_store=None,
    ):
        self.state_store = (
            state_store
            or CompanyStateStore()
        )

        self.signal_store = (
            signal_store
            or InvestmentSignalStore()
        )

        self.decision_store = (
            decision_store
            or DecisionSignalStore()
        )

        self.signal_engine = (
            InvestmentSignalEngine(
                state_store=self.state_store,
                signal_store=self.signal_store,
            )
        )

        self.decision_engine = (
            DecisionSignalEngine(
                state_store=self.state_store,
                signal_store=self.signal_store,
                decision_store=self.decision_store,
            )
        )

    def project_company(
        self,
        company_id: str,
    ) -> dict:

        state = self.state_store.get(
            company_id
        )

        if state is None:
            return {
                "company_id": company_id,
                "status": "NO_DATA",
            }

        if state["status"] != "ACTIVE":
            return {
                "company_id": company_id,
                "cik": state["cik"],
                "status": "NO_DATA",
                "funding_event_count": 0,
                "investment_signal_count": 0,
                "investment_signal_types": [],
                "investment_posture": "INSUFFICIENT_DATA",
                "source_event_id": None,
                "source_run_id": None,
                "source_document_id": None,
                "source_document_sha256": None,
            }

        signal_result = (
            self.signal_engine.generate(
                company_id
            )
        )

        decision_result = (
            self.decision_engine.evaluate(
                company_id
            )
        )

        signals = (
            self.signal_store.list_for_company(
                company_id
            )
        )

        current_version = int(
            state["state_version"]
        )

        current_signals = [
            signal
            for signal in signals
            if int(
                signal["source_state_version"]
            ) == current_version
        ]

        signal_types = [
            signal["signal_type"]
            for signal in current_signals
        ]

        return {
            "company_id": company_id,
            "cik": state["cik"],

            "state_version": current_version,

            "funding_status": state[
                "status"
            ],

            "funding_event_count": state[
                "funding_event_count"
            ],

            "latest_funding_date": state[
                "latest_funding_date"
            ],

            "latest_total_offering_amount": state[
                "latest_total_offering_amount"
            ],

            "latest_total_amount_sold": state[
                "latest_total_amount_sold"
            ],

            "cumulative_offering_amount": state[
                "cumulative_offering_amount"
            ],

            "cumulative_amount_sold": state[
                "cumulative_amount_sold"
            ],

            "investment_signal_count": len(
                current_signals
            ),

            "investment_signal_types": signal_types,

            "investment_posture": decision_result[
                "decision"
            ],

            "investment_posture_confidence": (
                decision_result["confidence"]
            ),

            "source_event_id": state[
                "last_source_event_id"
            ],

            "source_run_id": state[
                "last_source_run_id"
            ],

            "source_document_id": state[
                "last_source_document_id"
            ],

            "source_document_sha256": state[
                "last_source_document_sha256"
            ],

            "generated_at_utc": (
                datetime.now(
                    timezone.utc
                ).isoformat()
            ),
        }

    def build_dataframe(
        self,
        company_ids: list[str],
    ) -> pd.DataFrame:

        rows = []

        for company_id in company_ids:
            result = self.project_company(
                company_id
            )

            if result.get("status") != "NO_DATA":
                rows.append(result)

        return pd.DataFrame(rows)

    def write_company_artifact(
        self,
        company_id: str,
    ) -> Path | None:

        result = self.project_company(
            company_id
        )

        if result.get("status") == "NO_DATA":
            return None

        root = Path(
            __file__
        ).resolve().parents[3]

        output_dir = (
            root
            / "data"
            / "gold"
            / "company_investment_intelligence"
            / f"cik={result['cik']}"
        )

        output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path = (
            output_dir
            / "unified_investment_intelligence.parquet"
        )

        frame = pd.DataFrame([result])

        frame["investment_signal_types"] = (
            frame[
                "investment_signal_types"
            ].map(
                lambda value: json.dumps(
                    value,
                    sort_keys=True,
                )
            )
        )

        frame.to_parquet(
            output_path,
            index=False,
        )

        return output_path