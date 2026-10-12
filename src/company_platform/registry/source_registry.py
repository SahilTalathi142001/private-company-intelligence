from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
REGISTRY_DIR = ROOT / "data" / "silver" / "platform"

SOURCE_PATH = REGISTRY_DIR / "source_registry.parquet"
COMPANY_SOURCE_PATH = REGISTRY_DIR / "company_source_registry.parquet"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_sources() -> pd.DataFrame:
    now = _utc_now()

    return pd.DataFrame(
        [
            {
                "source_id": "SEC",
                "source_name": "SEC EDGAR",
                "source_type": "API",
                "provider": "SEC",
                "enabled": True,
                "supports_onboarding": True,
                "supports_incremental": True,
                "description": "SEC company submissions and XBRL facts",
                "created_at_utc": now,
                "updated_at_utc": now,
            },
            {
                "source_id": "SEC_FORM_D",
                "source_name": "SEC Form D",
                "source_type": "DATASET",
                "provider": "SEC",
                "enabled": True,
                "supports_onboarding": False,
                "supports_incremental": True,
                "description": "SEC Form D exempt offering dataset",
                "created_at_utc": now,
                "updated_at_utc": now,
            },
            {
                "source_id": "DOCUMENT",
                "source_name": "Company Documents",
                "source_type": "FILE",
                "provider": "PLATFORM",
                "enabled": True,
                "supports_onboarding": True,
                "supports_incremental": True,
                "description": "Uploaded company documents",
                "created_at_utc": now,
                "updated_at_utc": now,
            },
        ]
    )


def bootstrap_sources() -> pd.DataFrame:
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)

    if SOURCE_PATH.exists():
        return pd.read_parquet(SOURCE_PATH)

    sources = _default_sources()
    sources.to_parquet(SOURCE_PATH, index=False)

    return sources


def load_sources() -> pd.DataFrame:
    if not SOURCE_PATH.exists():
        return bootstrap_sources()

    return pd.read_parquet(SOURCE_PATH)


def enable_company_sources(company_id: str) -> pd.DataFrame:
    sources = load_sources()
    now = _utc_now()

    enabled = sources[
        sources["enabled"].astype(bool)
        & sources["supports_onboarding"].astype(bool)
    ].copy()

    records = []

    for _, row in enabled.iterrows():
        records.append(
            {
                "company_id": company_id,
                "source_id": row["source_id"],
                "enabled": True,
                "priority": 1,
                "ingestion_mode": (
                    "INCREMENTAL"
                    if row["supports_incremental"]
                    else "FULL"
                ),
                "configuration_status": "READY",
                "created_at_utc": now,
                "updated_at_utc": now,
                "last_successful_ingestion_utc": None,
                "last_error": None,
            }
        )

    result = pd.DataFrame(records)

    if COMPANY_SOURCE_PATH.exists():
        existing = pd.read_parquet(COMPANY_SOURCE_PATH)

        existing = existing[
            existing["company_id"].astype(str) != str(company_id)
        ]

        result = pd.concat(
            [existing, result],
            ignore_index=True,
        )

    result.to_parquet(COMPANY_SOURCE_PATH, index=False)

    return result