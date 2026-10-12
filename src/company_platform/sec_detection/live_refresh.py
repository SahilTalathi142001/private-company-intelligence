"""Production SEC live-refresh controller.

Step 36 / Audit #13:
- polls SEC submissions
- maintains durable per-company refresh state
- emits deterministic NEW_SEC_FILING events
- writes refresh audit and coverage artifacts
- delegates event persistence to the existing EventStore
- does NOT execute pipeline runs or schedule production jobs
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import pandas as pd
import requests
from dotenv import load_dotenv

from company_platform.events.event_store import EventStore
from company_platform.events.models import PlatformEvent
from company_platform.registry.company_registry import list_companies


SEC_SUBMISSIONS = "https://data.sec.gov/submissions/CIK{cik}.json"
VERSION = "36.0.0"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _root() -> Path:
    return Path(__file__).resolve().parents[3]


class SECLiveRefresh:
    """Durable and auditable SEC submissions refresh."""

    def __init__(
        self,
        *,
        event_store: EventStore | None = None,
        state_path: str | Path | None = None,
        audit_path: str | Path | None = None,
        coverage_path: str | Path | None = None,
        user_agent: str | None = None,
        timeout: int = 30,
    ):
        root = _root()

        load_dotenv(root / ".env")

        self.root = root

        # Existing event persistence mechanism.
        self.store = event_store or EventStore()

        # New Step 36 state.
        self.state_path = Path(state_path) if state_path else (
            root
            / "data"
            / "silver"
            / "platform"
            / "sec_live_refresh_state.json"
        )

        # New Step 36 operational artifacts.
        self.audit_path = Path(audit_path) if audit_path else (
            root
            / "data"
            / "gold"
            / "sec_live_refresh"
            / "refresh_audit.parquet"
        )

        self.coverage_path = Path(coverage_path) if coverage_path else (
            root
            / "data"
            / "gold"
            / "sec_live_refresh"
            / "refresh_coverage.parquet"
        )

        for path in (
            self.state_path,
            self.audit_path,
            self.coverage_path,
        ):
            path.parent.mkdir(parents=True, exist_ok=True)

        self.user_agent = user_agent or os.getenv("SEC_USER_AGENT")

        if not self.user_agent or "@" not in self.user_agent:
            raise ValueError(
                "Set SEC_USER_AGENT to a descriptive app name "
                "and valid contact email"
            )

        self.timeout = timeout

        self.session = requests.Session()

        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept-Encoding": "gzip, deflate",
                "Host": "data.sec.gov",
            }
        )

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def _load_state(self) -> dict[str, Any]:
        """Load Step 36 state and migrate the original detector state."""

        if not self.state_path.exists():
            return {
                "version": VERSION,
                "companies": {},
            }

        data = json.loads(
            self.state_path.read_text(encoding="utf-8")
        )

        # Backward compatibility with the original detector:
        #
        # {
        #     "seen_accessions": [
        #         "0000320193:0000320193-26-000001"
        #     ]
        # }
        #
        if "companies" not in data:

            companies: dict[str, Any] = {}

            for value in data.get("seen_accessions", []):

                try:
                    cik, accession = str(value).split(":", 1)
                except ValueError:
                    continue

                cik = cik.zfill(10)

                company_state = companies.setdefault(
                    cik,
                    {
                        "seen_accessions": [],
                        "last_success_utc": None,
                        "last_error": None,
                    },
                )

                company_state["seen_accessions"].append(
                    accession
                )

            data = {
                "version": VERSION,
                "companies": companies,
                "migrated_from_detector": True,
            }

        data.setdefault("version", VERSION)
        data.setdefault("companies", {})

        return data

    def _save_state(self, state: dict[str, Any]) -> None:
        """Atomically persist refresh state."""

        state["version"] = VERSION

        tmp_path = self.state_path.with_suffix(".tmp")

        tmp_path.write_text(
            json.dumps(
                state,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        tmp_path.replace(self.state_path)

    # ------------------------------------------------------------------
    # SEC
    # ------------------------------------------------------------------

    def _recent(self, cik: str) -> list[dict[str, Any]]:
        """Retrieve recent SEC submissions for one company."""

        response = self.session.get(
            SEC_SUBMISSIONS.format(cik=cik),
            timeout=self.timeout,
        )

        response.raise_for_status()

        payload = response.json()

        recent = payload.get("filings", {}).get(
            "recent",
            {},
        )

        accession_numbers = recent.get(
            "accessionNumber",
            [],
        )

        count = len(accession_numbers)

        fields = [
            "accessionNumber",
            "filingDate",
            "reportDate",
            "form",
            "primaryDocument",
            "primaryDocDescription",
        ]

        return [
            {
                field: recent.get(
                    field,
                    [None] * count,
                )[index]
                for field in fields
            }
            for index in range(count)
        ]

    # ------------------------------------------------------------------
    # Event identity
    # ------------------------------------------------------------------

    @staticmethod
    def _event_id(
        cik: str,
        accession: str,
    ) -> str:
        """Generate the existing deterministic SEC event identity."""

        value = (
            f"SEC_NEW_FILING:"
            f"{cik}:"
            f"{accession}"
        )

        return hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()

    # ------------------------------------------------------------------
    # Audit
    # ------------------------------------------------------------------

    def _append_audit(
        self,
        rows: list[dict[str, Any]],
    ) -> None:

        if not rows:
            return

        incoming = pd.DataFrame(rows)

        if self.audit_path.exists():

            existing = pd.read_parquet(
                self.audit_path
            )

            combined = pd.concat(
                [
                    existing,
                    incoming,
                ],
                ignore_index=True,
            )

            dedupe_columns = [
                "refresh_id",
                "cik",
                "accession_number",
                "action",
            ]

            combined = combined.drop_duplicates(
                subset=dedupe_columns,
                keep="last",
            )

        else:
            combined = incoming

        combined.to_parquet(
            self.audit_path,
            index=False,
        )

    def _write_coverage(
        self,
        rows: list[dict[str, Any]],
    ) -> None:

        pd.DataFrame(rows).to_parquet(
            self.coverage_path,
            index=False,
        )

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    def refresh_once(
        self,
        *,
        forms: list[str] | None = None,
        max_filings_per_company: int = 20,
        sleep_seconds: float = 0.12,
        ciks: list[str] | None = None,
    ) -> dict[str, Any]:
        """Execute one complete SEC refresh."""

        refresh_id = hashlib.sha256(
            f"{VERSION}:{_utc_now()}".encode(
                "utf-8"
            )
        ).hexdigest()[:20]

        started_at = _utc_now()

        state = self._load_state()

        companies = list_companies()

        if companies.empty:

            result = {
                "refresh_id": refresh_id,
                "version": VERSION,
                "status": "NO_COMPANIES",
                "checked": 0,
                "emitted": 0,
                "duplicates": 0,
                "filtered": 0,
                "failed": 0,
            }

            self._write_coverage([])

            return result

        form_set = {
            value.strip().upper()
            for value in (forms or [])
            if value.strip()
        }
        allowed_ciks = (
            {str(cik).zfill(10) for cik in ciks}
            if ciks is not None
            else None
        )

        audit_rows: list[dict[str, Any]] = []

        coverage_rows: list[dict[str, Any]] = []

        emitted_event_ids: list[str] = []

        totals = {
            "checked": 0,
            "emitted": 0,
            "duplicates": 0,
            "filtered": 0,
            "failed": 0,
        }

        # --------------------------------------------------------------
        # Company loop
        # --------------------------------------------------------------

        for _, company in companies.iterrows():

            status = str(
                company.get(
                    "status",
                    "ACTIVE",
                )
            ).upper()

            ingestion_enabled = bool(
                company.get(
                    "ingestion_enabled",
                    True,
                )
            )

            if status != "ACTIVE":
                continue

            if not ingestion_enabled:
                continue

            cik = str(
                company["cik"]
            ).zfill(10)

            if allowed_ciks is not None and cik not in allowed_ciks:
                continue

            company_id = str(
                company["company_id"]
            )

            totals["checked"] += 1

            company_state = state[
                "companies"
            ].setdefault(
                cik,
                {
                    "seen_accessions": [],
                    "last_success_utc": None,
                    "last_error": None,
                },
            )

            seen = set(
                company_state.get(
                    "seen_accessions",
                    [],
                )
            )

            company_emitted = 0
            company_duplicates = 0
            company_filtered = 0

            error = None

            # ----------------------------------------------------------
            # SEC request
            # ----------------------------------------------------------

            try:

                filings = self._recent(cik)

                filings = filings[
                    :max_filings_per_company
                ]

                for filing in filings:

                    accession = str(
                        filing.get(
                            "accessionNumber"
                        )
                        or ""
                    ).strip()

                    if not accession:
                        continue

                    form = str(
                        filing.get(
                            "form"
                        )
                        or ""
                    ).strip().upper()

                    # --------------------------------------------------
                    # Form filtering
                    # --------------------------------------------------

                    if (
                        form_set
                        and form not in form_set
                    ):

                        company_filtered += 1
                        totals["filtered"] += 1

                        # IMPORTANT:
                        # Do NOT add this accession to seen state.
                        #
                        # If the user later changes the form filter,
                        # the filing can still be discovered.
                        continue

                    # --------------------------------------------------
                    # Duplicate protection
                    # --------------------------------------------------

                    if accession in seen:

                        company_duplicates += 1
                        totals["duplicates"] += 1

                        continue

                    # --------------------------------------------------
                    # Deterministic event
                    # --------------------------------------------------

                    event_id = self._event_id(
                        cik,
                        accession,
                    )

                    event = PlatformEvent(
                        event_type="NEW_SEC_FILING",
                        company_id=company_id,
                        source_id="SEC",
                        event_id=event_id,
                        payload={
                            "cik": cik,
                            "accession_number": accession,
                            "form": form,
                            "filing_date": filing.get(
                                "filingDate"
                            ),
                            "report_date": filing.get(
                                "reportDate"
                            ),
                            "primary_document": filing.get(
                                "primaryDocument"
                            ),
                            "primary_doc_description": filing.get(
                                "primaryDocDescription"
                            ),
                            "refresh_id": refresh_id,
                            "detector_version": VERSION,
                        },
                    )

                    # Existing EventStore remains authoritative.
                    self.store.append(event)

                    seen.add(accession)

                    emitted_event_ids.append(
                        event_id
                    )

                    company_emitted += 1
                    totals["emitted"] += 1

                    audit_rows.append(
                        {
                            "refresh_id": refresh_id,
                            "refresh_version": VERSION,
                            "refreshed_at_utc": started_at,
                            "cik": cik,
                            "company_id": company_id,
                            "accession_number": accession,
                            "form": form,
                            "event_id": event_id,
                            "action": "EMITTED",
                            "error": None,
                        }
                    )

                # ------------------------------------------------------
                # Company success
                # ------------------------------------------------------

                company_state[
                    "seen_accessions"
                ] = sorted(seen)

                company_state[
                    "last_success_utc"
                ] = _utc_now()

                company_state[
                    "last_error"
                ] = None

            except Exception as exc:

                error = str(exc)

                totals["failed"] += 1

                company_state[
                    "last_error"
                ] = error

                audit_rows.append(
                    {
                        "refresh_id": refresh_id,
                        "refresh_version": VERSION,
                        "refreshed_at_utc": started_at,
                        "cik": cik,
                        "company_id": company_id,
                        "accession_number": None,
                        "form": None,
                        "event_id": None,
                        "action": "FAILED",
                        "error": error,
                    }
                )

            # ----------------------------------------------------------
            # Company coverage
            # ----------------------------------------------------------

            coverage_rows.append(
                {
                    "refresh_id": refresh_id,
                    "refresh_version": VERSION,
                    "refreshed_at_utc": started_at,
                    "cik": cik,
                    "company_id": company_id,
                    "status": (
                        "FAILED"
                        if error
                        else "SUCCESS"
                    ),
                    "filings_emitted": company_emitted,
                    "duplicates": company_duplicates,
                    "filtered": company_filtered,
                    "last_success_utc": company_state.get(
                        "last_success_utc"
                    ),
                    "last_error": error,
                }
            )

            # SEC-friendly spacing between companies.
            time.sleep(
                max(
                    0.0,
                    sleep_seconds,
                )
            )

        # --------------------------------------------------------------
        # Persist everything only after refresh state is assembled.
        # --------------------------------------------------------------

        self._save_state(state)

        self._append_audit(
            audit_rows
        )

        self._write_coverage(
            coverage_rows
        )

        status = (
            "SUCCESS"
            if totals["failed"] == 0
            else "PARTIAL"
        )

        return {
            "refresh_id": refresh_id,
            "version": VERSION,
            "status": status,
            **totals,
            "events": emitted_event_ids,
            "started_at_utc": started_at,
            "completed_at_utc": _utc_now(),
            "audit_path": str(
                self.audit_path
            ),
            "coverage_path": str(
                self.coverage_path
            ),
            "state_path": str(
                self.state_path
            ),
        }


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


def main() -> None:

    parser = argparse.ArgumentParser(
        description=(
            "SEC live refresh controller "
            "(Step 36 / Audit #13)"
        )
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help=(
            "Run one refresh. "
            "Without --once, repeat at the configured interval."
        ),
    )

    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=900,
        help="Polling interval in seconds.",
    )

    parser.add_argument(
        "--forms",
        default="",
        help=(
            "Optional comma-separated SEC forms. "
            "Example: 10-K,10-Q,8-K,4"
        ),
    )

    parser.add_argument(
        "--max-filings-per-company",
        type=int,
        default=20,
    )

    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.12,
    )

    args = parser.parse_args()

    controller = SECLiveRefresh()

    forms = [
        value.strip()
        for value in args.forms.split(",")
        if value.strip()
    ] or None

    while True:

        result = controller.refresh_once(
            forms=forms,
            max_filings_per_company=(
                args.max_filings_per_company
            ),
            sleep_seconds=args.sleep_seconds,
        )

        print(
            json.dumps(
                result,
                indent=2,
            )
        )

        if args.once:
            break

        # Step #13 deliberately does not become
        # the production scheduler. Step #16 owns that.
        time.sleep(
            max(
                60,
                args.interval_seconds,
            )
        )


if __name__ == "__main__":
    main()