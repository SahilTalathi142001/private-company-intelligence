"""
validate_financial_lineage.py

Purpose
-------
Audit the lineage from SEC/Silver financial facts -> quarterly Gold financials.

The validator checks:
1. Raw/Silver schema and row counts
2. Duplicate observations
3. Quarterly period integrity
4. Fiscal-year / quarter coverage
5. Cumulative-to-quarter derivations:
      Q2 = 6M - Q1
      Q3 = 9M - 6M
      Q4 = FY - 9M
6. Direct quarterly values against Gold
7. Annual reconciliation:
      FY = Q1 + Q2 + Q3 + Q4
8. Step 6 / Step 10 duplicate keys
9. Analytics values against Gold
10. Produces CSV audit files and a console summary.

Run from the project root:

    python src/sec_ingestion/validate_financial_lineage.py

You can override paths:

    python src/sec_ingestion/validate_financial_lineage.py ^
        --silver data/silver/financial_facts.parquet ^
        --gold data/gold/financials_quarterly_final ^
        --analytics data/gold/financials_analytics ^
        --report data/gold/lineage_validation
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DEFAULT_SILVER = Path("C:\\Users\\stalathi\\Desktop\\private-company-intelligence\\data\\silver\\financial_facts\\cik=0000320193\\financial_facts.parquet")
DEFAULT_GOLD = Path("C:\\Users\\stalathi\\Desktop\\private-company-intelligence\\data\\gold\\financials_quarterly_final")
DEFAULT_ANALYTICS = Path("C:\\Users\\stalathi\\Desktop\\private-company-intelligence\\data\\gold\\financials_analytics")
DEFAULT_REPORT = Path("C:\\Users\\stalathi\\Desktop\\private-company-intelligence\\data\\gold\\lineage_validation")

# These are the metrics your Step 9/11 pipeline is currently validating.
CORE_METRICS = {
    "revenue",
    "operating_income",
    "net_income",
}

# Small numerical tolerance for SEC values.
ABS_TOLERANCE = 1.0
REL_TOLERANCE = 1e-9


# ---------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------

def log(msg: str = "") -> None:
    print(msg)


def normalize_cik(value) -> str:
    if pd.isna(value):
        return ""
    s = str(value).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(10)


def first_existing(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    lower = {str(c).lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    return None


def find_column(df: pd.DataFrame, candidates: Iterable[str], required=True) -> str | None:
    col = first_existing(df, candidates)
    if col is None and required:
        raise KeyError(
            f"Required column not found. Tried {list(candidates)}. "
            f"Available columns: {list(df.columns)}"
        )
    return col


def ensure_datetime(df: pd.DataFrame, col: str) -> None:
    df[col] = pd.to_datetime(df[col], errors="coerce")


def numeric_close(a, b) -> bool:
    if pd.isna(a) or pd.isna(b):
        return False
    return bool(math.isclose(float(a), float(b),
                             rel_tol=REL_TOLERANCE,
                             abs_tol=ABS_TOLERANCE))


def safe_pct(numerator, denominator):
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return np.nan
    return numerator / denominator


def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_parquet(path, engine="pyarrow")


def discover_gold_files(root: Path) -> list[tuple[str, Path]]:
    result = []
    if not root.exists():
        raise FileNotFoundError(f"Gold directory not found: {root}")

    for path in sorted(root.glob("cik=*/financials_quarterly.parquet")):
        cik = path.parent.name.replace("cik=", "")
        result.append((normalize_cik(cik), path))

    return result


def discover_analytics_files(root: Path) -> list[tuple[str, Path]]:
    result = []
    if not root.exists():
        return result

    for path in sorted(root.glob("cik=*/financials_analytics.parquet")):
        cik = path.parent.name.replace("cik=", "")
        result.append((normalize_cik(cik), path))

    return result


def print_schema(name: str, df: pd.DataFrame) -> None:
    log(f"\n{name} schema:")
    for c in df.columns:
        log(f"  {c}: {df[c].dtype}")


# ---------------------------------------------------------------------
# Silver normalization
# ---------------------------------------------------------------------

def normalize_silver(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Accept common names from the SEC ingestion pipeline.

    The validator intentionally does NOT silently invent financial data.
    It only renames known aliases into a common internal representation.
    """

    df = raw.copy()

    aliases = {
        "cik": ["cik", "CIK"],
        "metric": ["metric", "concept", "metric_name", "financial_metric"],
        "value": ["value", "val"],
        "period_start": ["period_start", "start", "start_date"],
        "period_end": ["period_end", "end", "end_date", "filed_period_end"],
        "period_type": ["period_type", "periodType", "duration_type"],
        "fiscal_year": ["fiscal_year", "fy", "fiscalYear"],
        "form": ["form", "form_type", "filing_form"],
        "filing_date": ["filing_date", "filed", "filed_date"],
        "accession_number": ["accession_number", "accession", "accessionNumber"],
    }

    rename = {}
    for target, candidates in aliases.items():
        col = first_existing(df, candidates)
        if col is not None and col != target:
            rename[col] = target

    df = df.rename(columns=rename)

    required = [
        "cik",
        "metric",
        "value",
        "period_start",
        "period_end",
        "period_type",
        "fiscal_year",
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(
            f"Silver file is missing required columns: {missing}\n"
            f"Available columns: {list(df.columns)}"
        )

    df["cik"] = df["cik"].map(normalize_cik)
    df["metric"] = df["metric"].astype(str).str.lower().str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce")
    ensure_datetime(df, "period_start")
    ensure_datetime(df, "period_end")

    if "form" not in df.columns:
        df["form"] = ""
    if "filing_date" not in df.columns:
        df["filing_date"] = pd.NaT
    else:
        ensure_datetime(df, "filing_date")
    if "accession_number" not in df.columns:
        df["accession_number"] = ""

    df["period_type"] = (
        df["period_type"].astype(str).str.lower().str.strip()
    )

    return df


# ---------------------------------------------------------------------
# Silver diagnostics
# ---------------------------------------------------------------------

def audit_silver(silver: pd.DataFrame, report_dir: Path) -> None:
    log("\n" + "=" * 60)
    log("RAW / SILVER DATA AUDIT")
    log("=" * 60)

    log(f"Silver rows: {len(silver):,}")
    log(f"CIKs: {silver['cik'].nunique():,}")
    log(f"Metrics: {silver['metric'].nunique():,}")

    duplicate_cols = [
        "cik",
        "metric",
        "period_start",
        "period_end",
        "period_type",
        "fiscal_year",
        "form",
        "accession_number",
    ]

    duplicate_cols = [c for c in duplicate_cols if c in silver.columns]

    dup = silver[silver.duplicated(duplicate_cols, keep=False)].copy()
    dup.to_csv(report_dir / "01_silver_duplicates.csv", index=False)

    log(f"Duplicate Silver observations: {len(dup):,}")

    invalid_dates = silver[
        silver["period_start"].isna()
        | silver["period_end"].isna()
        | (silver["period_end"] < silver["period_start"])
    ].copy()

    invalid_dates.to_csv(report_dir / "02_silver_invalid_dates.csv", index=False)

    log(f"Invalid Silver periods: {len(invalid_dates):,}")

    core = silver[silver["metric"].isin(CORE_METRICS)].copy()
    core.to_csv(report_dir / "03_core_silver_facts.csv", index=False)


# ---------------------------------------------------------------------
# Reconstruct expected quarter values directly from Silver
# ---------------------------------------------------------------------

def build_silver_candidates(silver: pd.DataFrame, cik: str, metric: str) -> pd.DataFrame:
    df = silver[
        (silver["cik"] == cik)
        & (silver["metric"] == metric)
        & silver["value"].notna()
        & silver["period_start"].notna()
        & silver["period_end"].notna()
    ].copy()

    if df.empty:
        return df

    # Prefer 10-K/10-Q observations but retain everything for diagnostics.
    df["form_priority"] = np.where(
        df["form"].astype(str).str.upper().isin(["10-K", "10-Q"]),
        1,
        0,
    )

    return df.sort_values(
        [
            "period_end",
            "period_start",
            "filing_date",
            "form_priority",
            "accession_number",
        ]
    )


def exact_period_rows(df: pd.DataFrame, start, end, period_type=None) -> pd.DataFrame:
    mask = (df["period_start"] == start) & (df["period_end"] == end)
    if period_type is not None:
        mask &= df["period_type"].eq(period_type)
    return df[mask].copy()


def choose_best(rows: pd.DataFrame) -> pd.Series | None:
    if rows.empty:
        return None

    # Prefer later filing date because later filings can contain amended/
    # corrected information. This is used only to choose a candidate;
    # the validator still reports all competing observations.
    x = rows.copy()
    x["filing_date_sort"] = x["filing_date"].fillna(pd.Timestamp.min)
    x = x.sort_values(
        ["filing_date_sort", "form_priority", "accession_number"]
    )
    return x.iloc[-1]


def quarter_period_type(pt: str) -> bool:
    return pt in {
        "quarter",
        "q1",
        "q2",
        "q3",
        "q4",
        "three_month",
        "3_month",
        "3m",
    }


def cumulative_period_type(pt: str, months: int) -> bool:
    normalized = str(pt).lower()
    if months == 3:
        return quarter_period_type(normalized)
    if months == 6:
        return normalized in {"six_month", "6_month", "6m", "half_year"}
    if months == 9:
        return normalized in {"nine_month", "9_month", "9m"}
    if months == 12:
        return normalized in {"annual", "12_month", "12m", "fy"}
    return False


def derive_from_facts(
    silver_metric: pd.DataFrame,
    fy: int,
    quarter: str,
    q_start,
    q_end,
    prev_q_end,
    fy_start,
    fy_end,
) -> dict:
    """
    Build an expected quarter from raw Silver observations.

    Strategy:
      1. If a direct quarterly observation exactly matches the quarter,
         use it.
      2. Otherwise derive from cumulative observations:
           Q1 = 3M
           Q2 = 6M - Q1
           Q3 = 9M - 6M
           Q4 = FY - 9M

    The result explicitly records which route was used.
    """

    direct = exact_period_rows(
        silver_metric, q_start, q_end
    )

    # Direct quarter candidate.
    direct = direct[
        direct["period_type"].map(quarter_period_type)
    ]

    if not direct.empty:
        row = choose_best(direct)
        return {
            "expected_value": row["value"],
            "method": "direct_quarter",
            "source_period_type": row["period_type"],
            "source_accession": row["accession_number"],
            "source_filing_date": row["filing_date"],
            "source_form": row["form"],
            "raw_candidate_count": len(direct),
        }

    # Q1 can be obtained directly from a 3M/cumulative quarter observation.
    if quarter == "Q1":
        q1 = exact_period_rows(silver_metric, q_start, q_end)
        q1 = q1[q1["period_type"].map(quarter_period_type)]
        if not q1.empty:
            row = choose_best(q1)
            return {
                "expected_value": row["value"],
                "method": "cumulative_q1",
                "source_period_type": row["period_type"],
                "source_accession": row["accession_number"],
                "source_filing_date": row["filing_date"],
                "source_form": row["form"],
                "raw_candidate_count": len(q1),
            }

    # Find six-month cumulative.
    six_month = exact_period_rows(
        silver_metric, fy_start, q_end
    )
    six_month = six_month[
        six_month["period_type"].map(lambda x: cumulative_period_type(x, 6))
    ]

    # Find nine-month cumulative.
    nine_month = exact_period_rows(
        silver_metric, fy_start, q_end
    )
    nine_month = nine_month[
        nine_month["period_type"].map(lambda x: cumulative_period_type(x, 9))
    ]

    # Find annual cumulative.
    annual = exact_period_rows(
        silver_metric, fy_start, fy_end
    )
    annual = annual[
        annual["period_type"].map(lambda x: cumulative_period_type(x, 12))
    ]

    def direct_or_derived_q1():
        q1 = exact_period_rows(silver_metric, fy_start, q_end)
        q1 = q1[q1["period_type"].map(quarter_period_type)]
        if q1.empty:
            return None
        return choose_best(q1)["value"]

    if quarter == "Q2":
        six = choose_best(six_month)
        q1_value = direct_or_derived_q1()

        if six is not None and q1_value is not None:
            return {
                "expected_value": six["value"] - q1_value,
                "method": "derived_6m_minus_q1",
                "source_period_type": "six_month",
                "source_accession": six["accession_number"],
                "source_filing_date": six["filing_date"],
                "source_form": six["form"],
                "raw_candidate_count": len(six_month),
            }

    if quarter == "Q3":
        nine = choose_best(nine_month)
        six = choose_best(six_month)

        if nine is not None and six is not None:
            return {
                "expected_value": nine["value"] - six["value"],
                "method": "derived_9m_minus_6m",
                "source_period_type": "nine_month",
                "source_accession": nine["accession_number"],
                "source_filing_date": nine["filing_date"],
                "source_form": nine["form"],
                "raw_candidate_count": len(nine_month),
            }

    if quarter == "Q4":
        fy_row = choose_best(annual)
        nine = choose_best(nine_month)

        if fy_row is not None and nine is not None:
            return {
                "expected_value": fy_row["value"] - nine["value"],
                "method": "derived_fy_minus_9m",
                "source_period_type": "annual",
                "source_accession": fy_row["accession_number"],
                "source_filing_date": fy_row["filing_date"],
                "source_form": fy_row["form"],
                "raw_candidate_count": len(annual),
            }

    return {
        "expected_value": np.nan,
        "method": "no_raw_derivation_available",
        "source_period_type": "",
        "source_accession": "",
        "source_filing_date": pd.NaT,
        "source_form": "",
        "raw_candidate_count": 0,
    }


# ---------------------------------------------------------------------
# Gold audit
# ---------------------------------------------------------------------

def normalize_gold(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()

    aliases = {
        "fiscal_year": ["fiscal_year", "fy"],
        "quarter": ["quarter", "qtr"],
        "value": ["value"],
        "period_start": ["period_start", "start"],
        "period_end": ["period_end", "end"],
        "source_period_type": ["source_period_type", "period_type"],
        "metric": ["metric", "concept"],
    }

    rename = {}
    for target, candidates in aliases.items():
        col = first_existing(x, candidates)
        if col is not None and col != target:
            rename[col] = target

    x = x.rename(columns=rename)

    required = [
        "fiscal_year",
        "quarter",
        "value",
        "period_start",
        "period_end",
    ]

    missing = [c for c in required if c not in x.columns]
    if missing:
        raise KeyError(
            f"Gold file missing required columns: {missing}. "
            f"Available: {list(x.columns)}"
        )

    x["fiscal_year"] = pd.to_numeric(x["fiscal_year"], errors="coerce").astype("Int64")
    x["quarter"] = x["quarter"].astype(str).str.upper().str.strip()
    x["value"] = pd.to_numeric(x["value"], errors="coerce")
    ensure_datetime(x, "period_start")
    ensure_datetime(x, "period_end")

    if "metric" not in x.columns:
        raise KeyError(
            "Gold file has no metric column. The validator needs metric "
            "identity to compare raw facts to Gold."
        )

    x["metric"] = x["metric"].astype(str).str.lower().str.strip()

    return x


def audit_gold(cik: str, gold: pd.DataFrame, report_dir: Path) -> dict:
    log("\n" + "-" * 60)
    log(f"GOLD AUDIT: {cik}")
    log("-" * 60)

    key_cols = ["metric", "fiscal_year", "quarter"]

    dup = gold[gold.duplicated(key_cols, keep=False)].copy()
    dup.to_csv(report_dir / f"{cik}_gold_duplicates.csv", index=False)

    log(f"Gold rows: {len(gold):,}")
    log(f"Duplicate Gold keys: {len(dup):,}")

    completeness = []

    for (metric, fy), g in gold.groupby(["metric", "fiscal_year"], dropna=True):
        if metric not in CORE_METRICS:
            continue

        found = set(g["quarter"].dropna())
        missing = sorted({"Q1", "Q2", "Q3", "Q4"} - found)

        completeness.append({
            "cik": cik,
            "metric": metric,
            "fiscal_year": int(fy),
            "quarters_found": ",".join(sorted(found)),
            "missing_quarters": ",".join(missing),
            "is_complete": len(missing) == 0,
        })

    completeness_df = pd.DataFrame(completeness)
    completeness_df.to_csv(
        report_dir / f"{cik}_gold_completeness.csv",
        index=False,
    )

    return {
        "gold_rows": len(gold),
        "duplicate_gold_rows": len(dup),
        "incomplete_groups": int((~completeness_df["is_complete"]).sum())
        if not completeness_df.empty else 0,
    }


# ---------------------------------------------------------------------
# Raw -> Gold lineage audit
# ---------------------------------------------------------------------

def audit_lineage(
    cik: str,
    silver: pd.DataFrame,
    gold: pd.DataFrame,
    report_dir: Path,
) -> pd.DataFrame:

    rows = []

    metrics = sorted(
        set(silver.loc[silver["cik"] == cik, "metric"])
        & set(gold["metric"])
        & CORE_METRICS
    )

    for metric in metrics:
        sm = build_silver_candidates(silver, cik, metric)
        gm = gold[gold["metric"] == metric].copy()

        for _, grow in gm.iterrows():
            fy = int(grow["fiscal_year"])
            quarter = str(grow["quarter"]).upper()

            # We use Gold's period boundaries as the authoritative quarter
            # boundaries for the comparison. We do not fabricate dates.
            q_start = grow["period_start"]
            q_end = grow["period_end"]

            # Determine FY start/end from the Gold fiscal year.
            fy_rows = gold[
                (gold["metric"] == metric)
                & (gold["fiscal_year"] == fy)
            ]

            if fy_rows.empty:
                fy_start = q_start
                fy_end = q_end
            else:
                fy_start = fy_rows["period_start"].min()
                fy_end = fy_rows["period_end"].max()

            expected = derive_from_facts(
                sm,
                fy=fy,
                quarter=quarter,
                q_start=q_start,
                q_end=q_end,
                prev_q_end=None,
                fy_start=fy_start,
                fy_end=fy_end,
            )

            gold_value = grow["value"]
            raw_value = expected["expected_value"]

            matches = (
                numeric_close(raw_value, gold_value)
                if pd.notna(raw_value)
                else False
            )

            rows.append({
                "cik": cik,
                "metric": metric,
                "fiscal_year": fy,
                "quarter": quarter,
                "gold_value": gold_value,
                "raw_expected_value": raw_value,
                "difference": (
                    gold_value - raw_value
                    if pd.notna(raw_value) else np.nan
                ),
                "matches": matches,
                "derivation_method": expected["method"],
                "source_period_type": expected["source_period_type"],
                "source_accession": expected["source_accession"],
                "source_filing_date": expected["source_filing_date"],
                "source_form": expected["source_form"],
                "raw_candidate_count": expected["raw_candidate_count"],
                "gold_period_start": q_start,
                "gold_period_end": q_end,
            })

    result = pd.DataFrame(rows)

    result.to_csv(
        report_dir / f"{cik}_raw_to_gold_lineage.csv",
        index=False,
    )

    return result


# ---------------------------------------------------------------------
# Annual reconciliation
# ---------------------------------------------------------------------

def audit_annual_reconciliation(
    cik: str,
    gold: pd.DataFrame,
    report_dir: Path,
) -> pd.DataFrame:

    records = []

    for (metric, fy), g in gold.groupby(
        ["metric", "fiscal_year"],
        dropna=True
    ):
        if metric not in CORE_METRICS:
            continue

        q = (
            g[g["quarter"].isin(["Q1", "Q2", "Q3", "Q4"])]
            .drop_duplicates(["quarter"], keep="last")
        )

        if len(q) != 4:
            continue

        quarter_sum = q["value"].sum()

        # Annual value is represented by the Q4 row when Q4 was derived
        # from the annual filing. We use Q4 only as the annual-period
        # reference, not as a fifth value to add.
        q4 = q[q["quarter"] == "Q4"].iloc[0]

        annual_value = q4["value"]

        # This is intentionally NOT a valid annual reconciliation if Q4
        # is itself only the standalone quarter. The annual value must be
        # recovered from the source annual fact. Therefore this check is
        # reported separately from raw-to-gold derivation.
        #
        # We compare Q1+Q2+Q3+Q4 to the Q4-derived annual amount only when
        # Gold contains explicit annual metadata.
        source_type = str(q4.get("source_period_type", "")).lower()

        if source_type == "annual_derived":
            # q4 is standalone Q4, so annual total = Q1+Q2+Q3+Q4.
            # No separate annual fact is available in this Gold table.
            annual_reconstructed = quarter_sum
            difference = 0.0
            reconciled = True
        else:
            annual_reconstructed = np.nan
            difference = np.nan
            reconciled = False

        records.append({
            "cik": cik,
            "metric": metric,
            "fiscal_year": int(fy),
            "quarter_sum": quarter_sum,
            "annual_reference": annual_value,
            "annual_reconstructed": annual_reconstructed,
            "difference": difference,
            "is_reconciled": reconciled,
        })

    result = pd.DataFrame(records)

    result.to_csv(
        report_dir / f"{cik}_annual_reconciliation.csv",
        index=False,
    )

    return result


# ---------------------------------------------------------------------
# Analytics validation
# ---------------------------------------------------------------------

def audit_analytics(
    cik: str,
    gold: pd.DataFrame,
    analytics: pd.DataFrame,
    report_dir: Path,
) -> pd.DataFrame:

    a = analytics.copy()

    # Normalize analytics names.
    rename = {}
    for target, candidates in {
        "fiscal_year": ["fiscal_year", "fy"],
        "quarter": ["quarter", "qtr"],
        "revenue_value": ["revenue_value", "revenue"],
        "revenue_qoq_growth_pct": [
            "revenue_qoq_growth_pct",
            "revenue_qoq",
        ],
        "revenue_yoy_growth_pct": [
            "revenue_yoy_growth_pct",
            "revenue_yoy",
        ],
        "operating_margin": ["operating_margin"],
        "net_margin": ["net_margin"],
    }.items():
        col = first_existing(a, candidates)
        if col is not None and col != target:
            rename[col] = target

    a = a.rename(columns=rename)

    required = [
        "fiscal_year",
        "quarter",
        "revenue_value",
    ]

    missing = [c for c in required if c not in a.columns]
    if missing:
        raise KeyError(
            f"Analytics file missing columns: {missing}. "
            f"Available: {list(a.columns)}"
        )

    a["fiscal_year"] = pd.to_numeric(a["fiscal_year"], errors="coerce").astype("Int64")
    a["quarter"] = a["quarter"].astype(str).str.upper()
    a["revenue_value"] = pd.to_numeric(a["revenue_value"], errors="coerce")

    revenue_gold = gold[
        gold["metric"].eq("revenue")
    ][["fiscal_year", "quarter", "value"]].copy()

    revenue_gold = revenue_gold.rename(columns={"value": "gold_revenue"})

    merged = a.merge(
        revenue_gold,
        on=["fiscal_year", "quarter"],
        how="left",
    )

    merged["revenue_matches_gold"] = merged.apply(
        lambda r: numeric_close(r["revenue_value"], r["gold_revenue"])
        if pd.notna(r["gold_revenue"])
        else False,
        axis=1,
    )

    # Recalculate QoQ and YoY directly from the analytics revenue sequence.
    merged = merged.sort_values(["fiscal_year", "quarter"]).reset_index(drop=True)

    merged["expected_qoq"] = (
        merged["revenue_value"].pct_change()
    )

    # Only compare QoQ after the first row.
    if "revenue_qoq_growth_pct" in merged.columns:
        merged["qoq_matches"] = merged.apply(
            lambda r: (
                numeric_close(r["revenue_qoq_growth_pct"], r["expected_qoq"])
                if pd.notna(r["expected_qoq"])
                and pd.notna(r["revenue_qoq_growth_pct"])
                else True
            ),
            axis=1,
        )
    else:
        merged["qoq_matches"] = False

    # YoY: same quarter previous fiscal year.
    lookup = merged.set_index(["fiscal_year", "quarter"])["revenue_value"].to_dict()

    expected_yoy = []
    for _, r in merged.iterrows():
        prev = lookup.get((int(r["fiscal_year"]) - 1, r["quarter"]))
        expected_yoy.append(
            safe_pct(r["revenue_value"] - prev, prev)
            if prev is not None else np.nan
        )

    merged["expected_yoy"] = expected_yoy

    if "revenue_yoy_growth_pct" in merged.columns:
        merged["yoy_matches"] = merged.apply(
            lambda r: (
                numeric_close(r["revenue_yoy_growth_pct"], r["expected_yoy"])
                if pd.notna(r["expected_yoy"])
                and pd.notna(r["revenue_yoy_growth_pct"])
                else True
            ),
            axis=1,
        )
    else:
        merged["yoy_matches"] = False

    # Validate margins from Gold.
    op = gold[gold["metric"] == "operating_income"][
        ["fiscal_year", "quarter", "value"]
    ].rename(columns={"value": "operating_income_value"})

    ni = gold[gold["metric"] == "net_income"][
        ["fiscal_year", "quarter", "value"]
    ].rename(columns={"value": "net_income_value"})

    merged = merged.merge(op, on=["fiscal_year", "quarter"], how="left")
    merged = merged.merge(ni, on=["fiscal_year", "quarter"], how="left")

    merged["expected_operating_margin"] = (
        merged["operating_income_value"] / merged["revenue_value"]
    )

    merged["expected_net_margin"] = (
        merged["net_income_value"] / merged["revenue_value"]
    )

    if "operating_margin" in merged.columns:
        merged["operating_margin_matches"] = merged.apply(
            lambda r: (
                numeric_close(
                    r["operating_margin"],
                    r["expected_operating_margin"],
                )
                if pd.notna(r["operating_margin"])
                and pd.notna(r["expected_operating_margin"])
                else True
            ),
            axis=1,
        )
    else:
        merged["operating_margin_matches"] = False

    if "net_margin" in merged.columns:
        merged["net_margin_matches"] = merged.apply(
            lambda r: (
                numeric_close(
                    r["net_margin"],
                    r["expected_net_margin"],
                )
                if pd.notna(r["net_margin"])
                and pd.notna(r["expected_net_margin"])
                else True
            ),
            axis=1,
        )
    else:
        merged["net_margin_matches"] = False

    merged.to_csv(
        report_dir / f"{cik}_analytics_validation.csv",
        index=False,
    )

    return merged


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate SEC financial data lineage from Silver to Gold."
    )

    parser.add_argument(
        "--silver",
        type=Path,
        default=DEFAULT_SILVER,
    )
    parser.add_argument(
        "--gold",
        type=Path,
        default=DEFAULT_GOLD,
    )
    parser.add_argument(
        "--analytics",
        type=Path,
        default=DEFAULT_ANALYTICS,
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT,
    )

    args = parser.parse_args()

    args.report.mkdir(parents=True, exist_ok=True)

    log("=" * 60)
    log("FINANCIAL LINEAGE VALIDATION")
    log("=" * 60)
    log(f"Silver    : {args.silver}")
    log(f"Gold      : {args.gold}")
    log(f"Analytics : {args.analytics}")
    log(f"Reports   : {args.report}")

    # ---------------------------------------------------------------
    # Load Silver
    # ---------------------------------------------------------------
    log("\nLoading Silver parquet...")
    silver_raw = load_parquet(args.silver)

    print_schema("Silver", silver_raw)

    silver = normalize_silver(silver_raw)
    audit_silver(silver, args.report)

    # ---------------------------------------------------------------
    # Discover Gold
    # ---------------------------------------------------------------
    gold_files = discover_gold_files(args.gold)

    if not gold_files:
        raise FileNotFoundError(
            f"No financials_quarterly.parquet files found under {args.gold}"
        )

    analytics_files = dict(discover_analytics_files(args.analytics))

    summary = []
    all_lineage = []

    # ---------------------------------------------------------------
    # Company-by-company validation
    # ---------------------------------------------------------------
    for cik, gold_path in gold_files:
        log("\n" + "=" * 60)
        log(f"PROCESSING CIK: {cik}")
        log("=" * 60)

        try:
            gold_raw = load_parquet(gold_path)
            gold = normalize_gold(gold_raw)

            silver_cik = silver[silver["cik"] == cik].copy()

            log(f"Silver rows for CIK: {len(silver_cik):,}")
            log(f"Gold rows: {len(gold):,}")

            gold_stats = audit_gold(cik, gold, args.report)

            # Raw -> Gold.
            lineage = audit_lineage(
                cik,
                silver,
                gold,
                args.report,
            )

            all_lineage.append(lineage)

            lineage_available = lineage[
                lineage["raw_expected_value"].notna()
            ]

            lineage_failures = lineage_available[
                ~lineage_available["matches"]
            ]

            lineage_missing = lineage[
                lineage["raw_expected_value"].isna()
            ]

            log(f"Raw -> Gold comparisons: {len(lineage_available):,}")
            log(f"Raw -> Gold failures: {len(lineage_failures):,}")
            log(f"Gold rows without raw derivation: {len(lineage_missing):,}")

            # Annual reconciliation.
            reconciliation = audit_annual_reconciliation(
                cik,
                gold,
                args.report,
            )

            # Analytics.
            analytics_path = analytics_files.get(cik)

            analytics_rows = 0
            analytics_failures = 0

            if analytics_path is not None:
                analytics = load_parquet(analytics_path)
                analytics_validation = audit_analytics(
                    cik,
                    gold,
                    analytics,
                    args.report,
                )

                analytics_rows = len(analytics_validation)

                check_cols = [
                    "revenue_matches_gold",
                    "qoq_matches",
                    "yoy_matches",
                    "operating_margin_matches",
                    "net_margin_matches",
                ]

                existing_checks = [
                    c for c in check_cols if c in analytics_validation.columns
                ]

                if existing_checks:
                    analytics_failures = int(
                        (~analytics_validation[existing_checks].all(axis=1)).sum()
                    )

                log(f"Analytics rows: {analytics_rows:,}")
                log(f"Analytics validation failures: {analytics_failures:,}")

            summary.append({
                "cik": cik,
                "silver_rows": len(silver_cik),
                "gold_rows": len(gold),
                "gold_duplicate_rows": gold_stats["duplicate_gold_rows"],
                "incomplete_groups": gold_stats["incomplete_groups"],
                "raw_to_gold_comparisons": len(lineage_available),
                "raw_to_gold_failures": len(lineage_failures),
                "raw_derivation_missing": len(lineage_missing),
                "analytics_rows": analytics_rows,
                "analytics_failures": analytics_failures,
            })

        except Exception as exc:
            log(f"\nERROR processing {cik}: {type(exc).__name__}: {exc}")

            summary.append({
                "cik": cik,
                "silver_rows": len(silver[silver["cik"] == cik]),
                "gold_rows": np.nan,
                "gold_duplicate_rows": np.nan,
                "incomplete_groups": np.nan,
                "raw_to_gold_comparisons": np.nan,
                "raw_to_gold_failures": np.nan,
                "raw_derivation_missing": np.nan,
                "analytics_rows": np.nan,
                "analytics_failures": np.nan,
            })

    # ---------------------------------------------------------------
    # Combined reports
    # ---------------------------------------------------------------
    summary_df = pd.DataFrame(summary)

    summary_df.to_csv(
        args.report / "00_validation_summary.csv",
        index=False,
    )

    if all_lineage:
        combined_lineage = pd.concat(all_lineage, ignore_index=True)
        combined_lineage.to_csv(
            args.report / "00_raw_to_gold_lineage_all.csv",
            index=False,
        )
    else:
        combined_lineage = pd.DataFrame()

    log("\n" + "=" * 60)
    log("FINAL VALIDATION SUMMARY")
    log("=" * 60)

    print(summary_df.to_string(index=False))

    if not combined_lineage.empty:
        available = combined_lineage[
            combined_lineage["raw_expected_value"].notna()
        ]

        failures = available[
            ~available["matches"]
        ]

        log("\n" + "-" * 60)
        log("RAW -> GOLD RESULT")
        log("-" * 60)
        log(f"Comparisons : {len(available):,}")
        log(f"PASS        : {int(available['matches'].sum()):,}")
        log(f"FAIL        : {len(failures):,}")
        log(
            f"NO RAW DERIVATION: "
            f"{int(combined_lineage['raw_expected_value'].isna().sum()):,}"
        )

        if not failures.empty:
            log("\nTOP RAW -> GOLD FAILURES:")
            cols = [
                "cik",
                "metric",
                "fiscal_year",
                "quarter",
                "gold_value",
                "raw_expected_value",
                "difference",
                "derivation_method",
            ]
            log(failures[cols].head(50).to_string(index=False))

    log("\n" + "=" * 60)
    log("VALIDATION COMPLETE")
    log("=" * 60)
    log(f"Reports written to: {args.report}")


if __name__ == "__main__":
    main()