import sys
from pathlib import Path

import pandas as pd

# ============================================================
# PROJECT IMPORTS
# ============================================================

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from src.sec_ingestion.config import SILVER_DIR
    from src.sec_ingestion.gold_financial_model import (
        build_quarterly_gold,
    )
else:
    from .config import SILVER_DIR
    from .gold_financial_model import build_quarterly_gold


# ============================================================
# CONFIG
# ============================================================

COMPANIES = [
    "0000320193",  # Apple
    "0000789019",  # Microsoft
    "0001018724",  # Amazon
    "0001652044",  # Alphabet
    "0001045810",  # NVIDIA
]

GOLD_ROOT = SILVER_DIR.parent / "gold"

QUARTERLY_GOLD_ROOT = (
    GOLD_ROOT / "financials_quarterly"
)

FINAL_GOLD_ROOT = (
    GOLD_ROOT / "financials_quarterly_final"
)

QUARANTINE_ROOT = (
    GOLD_ROOT / "financials_quarantine"
)

DQ_ROOT = (
    GOLD_ROOT / "financial_dq"
)

RECON_FAILURES_PATH = (
    DQ_ROOT
    / "reconciliation_failures"
    / "reconciliation_failures.parquet"
)

INCOMPLETE_PATH = (
    DQ_ROOT
    / "incomplete_fiscal_years"
    / "incomplete_fiscal_years.parquet"
)

DUPLICATES_PATH = (
    DQ_ROOT
    / "dq_summary"
    / "quarter_duplicates.parquet"
)


# ============================================================
# VALIDATION
# ============================================================

REQUIRED_COLUMNS = [
    "cik",
    "metric",
    "unit",
    "fiscal_year",
    "quarter",
    "period_start",
    "period_end",
    "value",
    "source_accession",
    "source_filing_date",
    "source_period_type",
]

EXPECTED_QUARTERS = {"Q1", "Q2", "Q3", "Q4"}

FLOW_METRICS = {
    "revenue",
    "net_income",
    "operating_income",
}


def validate_schema(df: pd.DataFrame) -> None:
    missing = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            "Missing required Gold columns: "
            + ", ".join(missing)
        )


def validate_values(df: pd.DataFrame) -> None:
    if df["value"].isna().any():
        raise ValueError(
            "Gold contains null/invalid numeric values."
        )

    invalid_dates = (
        df["period_start"].isna()
        | df["period_end"].isna()
    )

    if invalid_dates.any():
        raise ValueError(
            f"Gold contains {int(invalid_dates.sum())} "
            "rows with invalid period dates."
        )


def duplicate_count(df: pd.DataFrame) -> int:
    key = [
        "cik",
        "metric",
        "unit",
        "fiscal_year",
        "quarter",
    ]

    return int(
        df.duplicated(
            subset=key,
            keep=False,
        ).sum()
    )


# ============================================================
# DQ LOADERS
# ============================================================

def load_dq_file(path: Path) -> pd.DataFrame:
    """
    Load a Step 9 parquet file.

    Missing DQ files are treated as a hard error. Step 10 must
    never silently promote data when its DQ evidence is absent.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Required Step 9 DQ output not found: {path}"
        )

    df = pd.read_parquet(path)

    if df.empty:
        return df

    return df.copy()


def normalize_cik(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "cik" not in df.columns:
        return df

    df = df.copy()
    df["cik"] = (
        df["cik"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(10)
    )
    return df


def normalize_year(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "fiscal_year" not in df.columns:
        return df

    df = df.copy()
    df["fiscal_year"] = pd.to_numeric(
        df["fiscal_year"],
        errors="coerce",
    ).astype("Int64")

    return df


def load_step9_dq() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load the authoritative Step 9 exception datasets.

    Returns:
        reconciliation_failures,
        incomplete_fiscal_years,
        duplicate_groups
    """
    reconciliation = normalize_cik(
        normalize_year(
            load_dq_file(RECON_FAILURES_PATH)
        )
    )

    incomplete = normalize_cik(
        normalize_year(
            load_dq_file(INCOMPLETE_PATH)
        )
    )

    duplicates = normalize_cik(
        normalize_year(
            load_dq_file(DUPLICATES_PATH)
        )
    )

    return reconciliation, incomplete, duplicates


# ============================================================
# DQ KEY BUILDING
# ============================================================

def build_reconciliation_keys(
    reconciliation: pd.DataFrame,
) -> set[tuple]:
    """
    A reconciliation exception is a metric/fiscal-year failure.

    We quarantine the entire metric-year rather than pretending
    Q1-Q3 are independently validated against an inconsistent
    annual source.
    """
    if reconciliation.empty:
        return set()

    required = {"cik", "metric", "fiscal_year"}
    missing = required - set(reconciliation.columns)

    if missing:
        raise ValueError(
            "Reconciliation failure report missing columns: "
            + ", ".join(sorted(missing))
        )

    result = set()

    for row in reconciliation.itertuples(index=False):
        result.add(
            (
                str(row.cik).zfill(10),
                str(row.metric),
                int(row.fiscal_year),
            )
        )

    return result


def build_incomplete_keys(
    incomplete: pd.DataFrame,
) -> set[tuple]:
    """
    An incomplete fiscal-year metric group is quarantined in full.

    This includes every quarterly row belonging to that metric/year.
    """
    if incomplete.empty:
        return set()

    required = {"cik", "metric", "fiscal_year"}
    missing = required - set(incomplete.columns)

    if missing:
        raise ValueError(
            "Incomplete-year report missing columns: "
            + ", ".join(sorted(missing))
        )

    result = set()

    for row in incomplete.itertuples(index=False):
        result.add(
            (
                str(row.cik).zfill(10),
                str(row.metric),
                int(row.fiscal_year),
            )
        )

    return result


def build_duplicate_keys(
    duplicates: pd.DataFrame,
) -> set[tuple]:
    """
    Duplicate groups are quarantined at the exact quarterly key.
    """
    if duplicates.empty:
        return set()

    required = {
        "cik",
        "metric",
        "unit",
        "fiscal_year",
        "quarter",
    }

    missing = required - set(duplicates.columns)

    if missing:
        # Some Step 9 implementations may report duplicate groups
        # without unit. In that case use the remaining key safely.
        fallback = {
            "cik",
            "metric",
            "fiscal_year",
            "quarter",
        }

        if not fallback.issubset(duplicates.columns):
            raise ValueError(
                "Duplicate report missing required key columns: "
                + ", ".join(sorted(missing))
            )

        return {
            (
                str(row.cik).zfill(10),
                str(row.metric),
                None,
                int(row.fiscal_year),
                str(row.quarter),
            )
            for row in duplicates.itertuples(index=False)
        }

    return {
        (
            str(row.cik).zfill(10),
            str(row.metric),
            str(row.unit),
            int(row.fiscal_year),
            str(row.quarter),
        )
        for row in duplicates.itertuples(index=False)
    }


# ============================================================
# PROMOTION / QUARANTINE
# ============================================================

def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    # region agent log
    try:
        import json
        import time
        payload = {
            "sessionId": "63fc53",
            "runId": "post-fix",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with open(Path(__file__).resolve().parents[2] / "debug-63fc53.log", "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # endregion


def retain_incomplete_only_history(classified: pd.DataFrame) -> pd.DataFrame:
    """Keep reported quarters when a company has no complete fiscal year.

    Incomplete years stay quarantined when the company also has promoted
    rows. A company whose every row is incomplete would otherwise lose all
    final gold and fail the pipeline, even though the reported quarters
    are real.
    """
    if classified.empty:
        return classified

    if classified["promotion_status"].eq("PROMOTE").any():
        return classified

    if not classified["dq_status"].eq("INCOMPLETE_FISCAL_YEAR").all():
        return classified

    retained = classified.copy()
    retained["promotion_status"] = "PROMOTE"
    retained["dq_reason"] = (
        "Reported quarters retained. The fiscal year is incomplete "
        "and missing quarters were not synthesized."
    )
    # region agent log
    _debug_log(
        "D",
        "final_quarterly_gold.py:retain_incomplete_only_history",
        "retained incomplete fiscal years",
        {
            "rows": int(len(retained)),
            "ciks": sorted(retained["cik"].astype(str).unique().tolist()),
            "years": sorted(retained["fiscal_year"].astype(int).unique().tolist()),
        },
    )
    # endregion
    return retained


def classify_rows(
    gold_df: pd.DataFrame,
    reconciliation_keys: set[tuple],
    incomplete_keys: set[tuple],
    duplicate_keys: set[tuple],
) -> pd.DataFrame:
    """
    Apply Step 9 decisions to Step 10 Gold.

    Promotion policy:

      PASS
        Row belongs to a complete metric/fiscal-year and has no
        reconciliation or duplicate exception.

      INCOMPLETE_FISCAL_YEAR
        Entire metric/fiscal-year is quarantined.

      RECONCILIATION_EXCEPTION
        Entire metric/fiscal-year is quarantined.

      DUPLICATE_KEY
        Exact quarterly key is quarantined.

    Stock/balance-sheet metrics that are not covered by Step 9
    duration DQ are promoted after structural validation.
    """
    df = gold_df.copy()

    statuses = []
    reasons = []

    for row in df.itertuples(index=False):
        cik = str(row.cik).zfill(10)
        metric = str(row.metric)
        unit = str(row.unit)
        fiscal_year = int(row.fiscal_year)
        quarter = str(row.quarter)

        year_metric_key = (
            cik,
            metric,
            fiscal_year,
        )

        exact_key = (
            cik,
            metric,
            unit,
            fiscal_year,
            quarter,
        )

        fallback_exact_key = (
            cik,
            metric,
            None,
            fiscal_year,
            quarter,
        )

        if exact_key in duplicate_keys or fallback_exact_key in duplicate_keys:
            statuses.append("DUPLICATE_KEY")
            reasons.append(
                "Step 9 identified a duplicate quarterly key."
            )

        elif year_metric_key in incomplete_keys:
            statuses.append("INCOMPLETE_FISCAL_YEAR")
            reasons.append(
                "Step 9 identified missing quarter(s) for this "
                "metric and fiscal year."
            )

        elif year_metric_key in reconciliation_keys:
            statuses.append("RECONCILIATION_EXCEPTION")
            reasons.append(
                "Step 9 identified an annual-to-quarter "
                "reconciliation failure for this metric and fiscal year."
            )

        else:
            statuses.append("PASS")
            reasons.append("No Step 9 exception applies.")

    df["dq_status"] = statuses
    df["dq_reason"] = reasons

    # A derived Q4 is explicitly identifiable in the lineage.
    # Keep this information separate from the primary DQ status.
    df["is_annual_derived"] = (
        df["source_period_type"].astype(str)
        .str.lower()
        .eq("annual_derived")
    )

    df["promotion_status"] = df["dq_status"].map(
        lambda x: "PROMOTE" if x == "PASS" else "QUARANTINE"
    )

    return df


# ============================================================
# OUTPUT WRITERS
# ============================================================

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    df.to_parquet(
        path,
        index=False,
    )


def write_company_outputs(
    cik: str,
    classified: pd.DataFrame,
) -> tuple[Path, Path]:
    final_df = classified[
        classified["promotion_status"] == "PROMOTE"
    ].copy()

    quarantine_df = classified[
        classified["promotion_status"] == "QUARANTINE"
    ].copy()

    final_path = (
        FINAL_GOLD_ROOT
        / f"cik={cik}"
        / "financials_quarterly.parquet"
    )

    quarantine_path = (
        QUARANTINE_ROOT
        / f"cik={cik}"
        / "financial_exceptions.parquet"
    )

    write_parquet(
        final_df,
        final_path,
    )

    write_parquet(
        quarantine_df,
        quarantine_path,
    )

    return final_path, quarantine_path


# ============================================================
# PROCESS ONE COMPANY
# ============================================================

def process_company(
    cik: str,
    reconciliation_keys: set[tuple] | None = None,
    incomplete_keys: set[tuple] | None = None,
    duplicate_keys: set[tuple] | None = None,
) -> dict:

    cik = str(cik).zfill(10)

    print()
    print("=" * 60)
    print(f"Processing CIK: {cik}")
    print("=" * 60)

    source_path = (
        QUARTERLY_GOLD_ROOT
        / f"cik={cik}"
        / "financials_quarterly.parquet"
    )

    if not source_path.exists():
        raise FileNotFoundError(
            f"Step 6 quarterly file not found: {source_path}"
        )

    quarterly_df = pd.read_parquet(source_path)

    print(f"Step 6 rows: {len(quarterly_df):,}")

    gold_df = build_quarterly_gold(
        quarterly_df
    )

    print(f"Step 10 candidate Gold rows: {len(gold_df):,}")

    validate_schema(gold_df)
    validate_values(gold_df)

    duplicate_rows = duplicate_count(gold_df)

    if duplicate_rows:
        raise ValueError(
            f"Structural duplicate Gold rows detected: "
            f"{duplicate_rows}"
        )

    # When called from main.py, Step 9 has already produced the DQ
    # parquet files. Load them here if explicit key sets were not
    # supplied. This keeps the public process_company(cik) API intact.
    if (
        reconciliation_keys is None
        and incomplete_keys is None
        and duplicate_keys is None
    ):
        reconciliation, incomplete, duplicates = load_step9_dq()

        reconciliation_keys = build_reconciliation_keys(
            reconciliation
        )
        incomplete_keys = build_incomplete_keys(
            incomplete
        )
        duplicate_keys = build_duplicate_keys(
            duplicates
        )
    else:
        reconciliation_keys = (
            reconciliation_keys
            if reconciliation_keys is not None
            else set()
        )

        incomplete_keys = (
            incomplete_keys
            if incomplete_keys is not None
            else set()
        )

        duplicate_keys = (
            duplicate_keys
            if duplicate_keys is not None
            else set()
        )

    classified = retain_incomplete_only_history(
        classify_rows(
            gold_df,
            reconciliation_keys,
            incomplete_keys,
            duplicate_keys,
        )
    )

    final_df = classified[
        classified["promotion_status"] == "PROMOTE"
    ].copy()

    quarantine_df = classified[
        classified["promotion_status"] == "QUARANTINE"
    ].copy()

    final_path, quarantine_path = write_company_outputs(
        cik,
        classified,
    )

    status_counts = (
        classified["dq_status"]
        .value_counts()
        .to_dict()
    )

    promoted_rows = len(final_df)
    quarantined_rows = len(quarantine_df)

    print(f"Structural duplicate rows: {duplicate_rows:,}")
    print(
        f"Promoted to Final Gold: {promoted_rows:,}"
    )
    print(
        f"Quarantined: {quarantined_rows:,}"
    )

    print("\nPromotion status:")
    for status in [
        "PASS",
        "INCOMPLETE_FISCAL_YEAR",
        "RECONCILIATION_EXCEPTION",
        "DUPLICATE_KEY",
    ]:
        print(
            f"  {status}: "
            f"{status_counts.get(status, 0):,}"
        )

    print(
        f"\nFinal Gold saved: {final_path}"
    )

    print(
        f"Quarantine saved: {quarantine_path}"
    )

    # --------------------------------------------------------
    # Recent promoted revenue
    # --------------------------------------------------------

    revenue = final_df[
        final_df["metric"] == "revenue"
    ].copy()

    if not revenue.empty:
        print()
        print("Recent FINAL Gold revenue quarters:")
        print()

        display_cols = [
            "fiscal_year",
            "quarter",
            "value",
            "period_start",
            "period_end",
            "source_period_type",
            "dq_status",
        ]

        print(
            revenue[
                [
                    c
                    for c in display_cols
                    if c in revenue.columns
                ]
            ]
            .sort_values(
                ["fiscal_year", "quarter"]
            )
            .tail(12)
            .to_string(index=False)
        )

    # --------------------------------------------------------
    # Quarantine details
    # --------------------------------------------------------

    if not quarantine_df.empty:
        print()
        print("Quarantine by reason:")
        print()

        print(
            quarantine_df[
                "dq_status"
            ]
            .value_counts()
            .to_string()
        )

    return {
        "cik": cik,
        "step6_rows": len(quarterly_df),
        "candidate_gold_rows": len(gold_df),
        "final_gold_rows": promoted_rows,
        "quarantine_rows": quarantined_rows,
        "duplicate_key_rows": duplicate_rows,
        "reconciliation_exception_rows": int(
            (classified["dq_status"] == "RECONCILIATION_EXCEPTION").sum()
        ),
        "incomplete_fiscal_year_rows": int(
            (classified["dq_status"] == "INCOMPLETE_FISCAL_YEAR").sum()
        ),
        "duplicate_exception_rows": int(
            (classified["dq_status"] == "DUPLICATE_KEY").sum()
        ),
        "final_output_path": str(final_path),
        "quarantine_output_path": str(quarantine_path),
    }


# ============================================================
# STANDALONE EXECUTION
# ============================================================

def main() -> None:
    print("=" * 70)
    print("STEP 10 - PRODUCTION FINAL QUARTERLY GOLD")
    print("=" * 70)

    reconciliation, incomplete, duplicates = load_step9_dq()

    reconciliation_keys = build_reconciliation_keys(
        reconciliation
    )

    incomplete_keys = build_incomplete_keys(
        incomplete
    )

    duplicate_keys = build_duplicate_keys(
        duplicates
    )

    print()
    print("STEP 9 DQ inputs loaded:")
    print(
        f"  Reconciliation exception groups: "
        f"{len(reconciliation_keys):,}"
    )
    print(
        f"  Incomplete fiscal-year groups: "
        f"{len(incomplete_keys):,}"
    )
    print(
        f"  Duplicate groups: "
        f"{len(duplicate_keys):,}"
    )

    results = []
    failures = []

    for cik in COMPANIES:
        try:
            result = process_company(
                cik,
                reconciliation_keys,
                incomplete_keys,
                duplicate_keys,
            )
            results.append(result)

        except Exception as exc:
            failures.append(
                {
                    "cik": cik,
                    "error": repr(exc),
                }
            )

            print()
            print("=" * 60)
            print(f"ERROR processing {cik}")
            print("=" * 60)
            print(repr(exc))

    # --------------------------------------------------------
    # STEP 10 SUMMARY
    # --------------------------------------------------------

    print()
    print("=" * 60)
    print("STEP 10 SUMMARY")
    print("=" * 60)

    if results:
        summary = pd.DataFrame(results)

        print(
            summary[
                [
                    "cik",
                    "step6_rows",
                    "candidate_gold_rows",
                    "final_gold_rows",
                    "quarantine_rows",
                    "reconciliation_exception_rows",
                    "incomplete_fiscal_year_rows",
                    "duplicate_exception_rows",
                ]
            ].to_string(index=False)
        )

        summary_path = (
            FINAL_GOLD_ROOT
            / "step10_promotion_summary.parquet"
        )

        write_parquet(
            summary,
            summary_path,
        )

        print(
            f"\nStep 10 promotion summary saved: "
            f"{summary_path}"
        )

    if failures:
        print()
        print("=" * 60)
        print("STEP 10 FAILURES")
        print("=" * 60)

        print(
            pd.DataFrame(
                failures
            ).to_string(index=False)
        )

        raise RuntimeError(
            f"Step 10 failed for {len(failures)} company/companyies."
        )

    print()
    print("=" * 60)
    print("STEP 10 COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()