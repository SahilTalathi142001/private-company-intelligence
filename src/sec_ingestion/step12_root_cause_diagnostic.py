"""
STEP 12 - FINANCIAL VALIDATION ROOT-CAUSE DIAGNOSTIC

Purpose
-------
Reads the Step 10/11 Gold + Analytics datasets and the reports produced by
FINANCIAL LINEAGE VALIDATION V2.2, then creates three detailed exception files:

1. raw_gold_exceptions.csv
   - Gold rows for which V2.2 reported NO RAW DERIVATION.

2. annual_reconciliation_exceptions.csv
   - All material annual reconciliation failures from V2.2, enriched with
     quarter-level Gold values and the exact annual Silver reference.

3. analytics_failures.csv
   - Detailed validation of analytics calculations against Gold.

The script deliberately does NOT modify Silver, Gold, or Analytics.
It is diagnostic only.

Expected project layout
-----------------------
data/
  silver/financial_facts/cik=*/financial_facts.parquet
  gold/financials_quarterly_final/cik=*/financials_quarterly.parquet
  gold/financials_analytics/cik=*/financials_analytics.parquet
  gold/lineage_validation_v2_2/*.csv

Run
---
python src/sec_ingestion/step12_root_cause_diagnostic.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "financial_facts"
GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "financials_quarterly_final"
ANALYTICS_DIR = PROJECT_ROOT / "data" / "gold" / "financials_analytics"

# V2.2 output directory from the validator log
V22_REPORT_DIR = PROJECT_ROOT / "data" / "gold" / "lineage_validation_v2_2"

OUTPUT_DIR = PROJECT_ROOT / "data" / "gold" / "financial_root_cause"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

METRICS = ["revenue", "operating_income", "net_income"]

# Annual differences below this are considered minor.
# This is diagnostic only; it does not change the V2.2 classification.
MATERIAL_ANNUAL_PCT = 0.01  # 1%

# Analytics tolerances.
# Financial growth/margin calculations should match essentially exactly,
# allowing only floating point noise.
ANALYTICS_ABS_TOL = 1e-9
ANALYTICS_REL_TOL = 1e-9


# ============================================================
# LOGGING
# ============================================================

def banner(text: str) -> None:
    print("\n" + "=" * 60)
    print(text)
    print("=" * 60)


def log(text: str) -> None:
    print(text)


# ============================================================
# GENERAL HELPERS
# ============================================================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        re.sub(r"[^a-z0-9_]+", "_", str(c).strip().lower()).strip("_")
        for c in df.columns
    ]
    return df


def first_existing(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def ensure_columns(df: pd.DataFrame, columns: Iterable[str], default=np.nan) -> pd.DataFrame:
    df = df.copy()
    for c in columns:
        if c not in df.columns:
            df[c] = default
    return df


def to_numeric(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    df = df.copy()
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def to_dates(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    df = df.copy()
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def quarter_number(q: str) -> int:
    if pd.isna(q):
        return 0
    m = re.search(r"([1-4])", str(q).upper())
    return int(m.group(1)) if m else 0


def quarter_label(n: int) -> str:
    return f"Q{n}" if n in (1, 2, 3, 4) else ""


def safe_pct_difference(actual: float, expected: float) -> float:
    if pd.isna(actual) or pd.isna(expected):
        return np.nan
    if expected == 0:
        return np.nan if actual == 0 else np.inf
    return (actual - expected) / abs(expected)


def values_close(a, b, rtol=ANALYTICS_REL_TOL, atol=ANALYTICS_ABS_TOL) -> bool:
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    return bool(np.isclose(float(a), float(b), rtol=rtol, atol=atol))


# ============================================================
# FILE DISCOVERY
# ============================================================

def discover_parquet_partitions(root: Path, filename: str) -> dict[str, Path]:
    result = {}
    if not root.exists():
        return result

    for p in root.glob(f"cik=*/{filename}"):
        m = re.search(r"cik=([^/\\\\]+)", str(p))
        if m:
            result[m.group(1)] = p

    return dict(sorted(result.items()))


def discover_report_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        p for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in {".csv", ".parquet"}
    )


def read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


# ============================================================
# REPORT IDENTIFICATION
# ============================================================

def report_score(path: Path, purpose: str) -> int:
    """
    Identify a V2.2 report by column signatures rather than hard-coding
    filenames. This makes the script tolerant of filenames changing between
    validator versions.
    """
    try:
        df = normalize_columns(read_table(path).head(5))
    except Exception:
        return -1

    cols = set(df.columns)
    name = path.name.lower()

    score = 0

    if purpose == "missing_raw":
        signatures = [
            {"cik", "metric", "fiscal_year", "quarter"},
            {"gold_value", "derivation_method"},
            {"raw_expected_value"},
        ]
        if any(s.issubset(cols) for s in signatures):
            score += 10
        if "raw" in name or "derivation" in name:
            score += 3
        if "missing" in name:
            score += 3

    elif purpose == "annual":
        signatures = [
            {"cik", "metric", "fiscal_year", "quarter_sum"},
            {"raw_annual_reference", "difference", "difference_pct"},
            {"q1", "q2", "q3", "q4"},
        ]
        if any(s.issubset(cols) for s in signatures):
            score += 10
        if "annual" in name or "recon" in name:
            score += 3
        if "failure" in name or "exception" in name:
            score += 2

    elif purpose == "analytics":
        signatures = [
            {"cik", "fiscal_year", "quarter"},
            {"revenue_value"},
            {"operating_margin", "net_margin"},
        ]
        if any(s.issubset(cols) for s in signatures):
            score += 10
        if "analytics" in name:
            score += 4
        if "failure" in name or "validation" in name:
            score += 2

    return score


def find_best_report(root: Path, purpose: str) -> Path | None:
    candidates = []
    for p in discover_report_files(root):
        score = report_score(p, purpose)
        if score >= 10:
            candidates.append((score, p))

    candidates.sort(key=lambda x: (-x[0], str(x[1])))
    return candidates[0][1] if candidates else None


# ============================================================
# LOAD DATA
# ============================================================

def load_all_silver() -> pd.DataFrame:
    parts = discover_parquet_partitions(SILVER_DIR, "financial_facts.parquet")

    frames = []
    for cik, path in parts.items():
        df = normalize_columns(pd.read_parquet(path))
        if "cik" not in df.columns:
            df["cik"] = cik
        df["cik"] = df["cik"].astype(str).str.zfill(10)
        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No Silver parquet files found under {SILVER_DIR}")

    df = pd.concat(frames, ignore_index=True)
    return df


def load_all_gold() -> pd.DataFrame:
    parts = discover_parquet_partitions(
        GOLD_DIR, "financials_quarterly.parquet"
    )

    frames = []
    for cik, path in parts.items():
        df = normalize_columns(pd.read_parquet(path))
        if "cik" not in df.columns:
            df["cik"] = cik
        df["cik"] = df["cik"].astype(str).str.zfill(10)
        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No Gold parquet files found under {GOLD_DIR}")

    return pd.concat(frames, ignore_index=True)


def load_all_analytics() -> pd.DataFrame:
    parts = discover_parquet_partitions(
        ANALYTICS_DIR, "financials_analytics.parquet"
    )

    frames = []
    for cik, path in parts.items():
        df = normalize_columns(pd.read_parquet(path))
        if "cik" not in df.columns:
            df["cik"] = cik
        df["cik"] = df["cik"].astype(str).str.zfill(10)
        frames.append(df)

    if not frames:
        raise FileNotFoundError(
            f"No Analytics parquet files found under {ANALYTICS_DIR}"
        )

    return pd.concat(frames, ignore_index=True)


# ============================================================
# RAW CANDIDATE LOGIC
# ============================================================

def build_raw_candidates_for_gold(gold_row: pd.Series, silver: pd.DataFrame) -> pd.DataFrame:
    """
    Find Silver duration facts that could represent the Gold quarter.

    We deliberately do not blindly select one fact. Every candidate is retained
    so the exception report can show why a derivation was or was not possible.
    """
    cik = str(gold_row["cik"]).zfill(10)
    metric = str(gold_row["metric"]).lower()
    fy = int(gold_row["fiscal_year"])
    quarter = str(gold_row["quarter"]).upper()

    s = silver[
        (silver["cik"].astype(str).str.zfill(10) == cik)
        & (silver["metric"].astype(str).str.lower() == metric)
    ].copy()

    if s.empty:
        return s

    s = to_dates(s, ["period_start", "period_end", "filing_date"])

    gold_start = pd.to_datetime(gold_row.get("period_start"), errors="coerce")
    gold_end = pd.to_datetime(gold_row.get("period_end"), errors="coerce")

    # First preference: exact period boundary match.
    exact = s[
        (s["period_start"] == gold_start)
        & (s["period_end"] == gold_end)
    ].copy()

    if not exact.empty:
        exact["candidate_match_type"] = "exact_period"
        return exact

    # Second preference: same FY + quarter using period boundaries.
    # This is intentionally conservative. We do not invent a quarter from
    # arbitrary duration facts here.
    if "fiscal_year" in s.columns:
        s["fiscal_year_num"] = pd.to_numeric(s["fiscal_year"], errors="coerce")
        same_fy = s[s["fiscal_year_num"] == fy].copy()
    else:
        same_fy = s.copy()

    if same_fy.empty:
        return same_fy

    # Return all same-FY candidates to make the root cause visible.
    same_fy["candidate_match_type"] = "same_fiscal_year_candidate"
    return same_fy


# ============================================================
# A. RAW -> GOLD EXCEPTIONS
# ============================================================

def build_raw_gold_exceptions(
    silver: pd.DataFrame,
    gold: pd.DataFrame,
    v22_missing_path: Path | None,
) -> pd.DataFrame:

    # If V2.2 already produced an explicit missing-derivation report, use it
    # as the authoritative list. Otherwise identify rows from the Gold data.
    if v22_missing_path is not None:
        reported = normalize_columns(read_table(v22_missing_path))
        reported = ensure_columns(
            reported,
            ["cik", "metric", "fiscal_year", "quarter"]
        )
        reported["cik"] = reported["cik"].astype(str).str.zfill(10)

        keys = reported[["cik", "metric", "fiscal_year", "quarter"]].drop_duplicates()
    else:
        # Fallback: Gold rows for which no exact Silver period exists.
        records = []
        for _, row in gold.iterrows():
            candidates = build_raw_candidates_for_gold(row, silver)
            exact = candidates[
                (candidates["period_start"] == pd.to_datetime(row["period_start"]))
                & (candidates["period_end"] == pd.to_datetime(row["period_end"]))
            ] if not candidates.empty else candidates

            if exact.empty:
                records.append({
                    "cik": row["cik"],
                    "metric": row["metric"],
                    "fiscal_year": row["fiscal_year"],
                    "quarter": row["quarter"],
                })

        keys = pd.DataFrame(records)

    if keys.empty:
        return pd.DataFrame(columns=[
            "cik", "metric", "fiscal_year", "quarter", "gold_value",
            "gold_period_start", "gold_period_end", "gold_source_period_type",
            "gold_derivation_method", "candidate_count", "candidate_values",
            "candidate_periods", "candidate_forms", "candidate_accessions",
            "candidate_xbrl_tags", "root_cause_hint"
        ])

    gold2 = gold.copy()
    gold2["cik"] = gold2["cik"].astype(str).str.zfill(10)

    keys["fiscal_year"] = pd.to_numeric(keys["fiscal_year"], errors="coerce")
    gold2["fiscal_year"] = pd.to_numeric(gold2["fiscal_year"], errors="coerce")

    merged = keys.merge(
        gold2,
        on=["cik", "metric", "fiscal_year", "quarter"],
        how="left",
        suffixes=("", "_gold")
    )

    rows = []

    for _, row in merged.iterrows():
        candidates = build_raw_candidates_for_gold(row, silver)

        def join_values(col):
            if candidates.empty or col not in candidates.columns:
                return ""
            vals = candidates[col].dropna().astype(str).drop_duplicates().tolist()
            return " | ".join(vals[:20])

        candidate_values = join_values("value")

        if candidates.empty:
            hint = "NO_SILVER_CANDIDATES"
        else:
            exact = candidates[
                (candidates["period_start"] == pd.to_datetime(row["period_start"]))
                & (candidates["period_end"] == pd.to_datetime(row["period_end"]))
            ]

            if not exact.empty:
                hint = "EXACT_SILVER_PERIOD_EXISTS_BUT_NOT_SELECTED"
            elif len(candidates) > 1:
                hint = "MULTIPLE_SILVER_CANDIDATES_REVIEW_FACT_SELECTION"
            else:
                hint = "SILVER_CANDIDATE_PERIOD_DOES_NOT_MATCH_GOLD"

        rows.append({
            "cik": row["cik"],
            "metric": row["metric"],
            "fiscal_year": row["fiscal_year"],
            "quarter": row["quarter"],
            "gold_value": row.get("value", np.nan),
            "gold_period_start": row.get("period_start"),
            "gold_period_end": row.get("period_end"),
            "gold_source_period_type": row.get("source_period_type"),
            "gold_derivation_method": row.get("quarter_assignment_method"),
            "candidate_count": len(candidates),
            "candidate_values": candidate_values,
            "candidate_periods": join_values("period_start") + " -> " + join_values("period_end"),
            "candidate_forms": join_values("form"),
            "candidate_accessions": join_values("accession_number"),
            "candidate_xbrl_tags": join_values("xbrl_tag"),
            "root_cause_hint": hint,
        })

    return pd.DataFrame(rows)


# ============================================================
# B. ANNUAL RECONCILIATION EXCEPTIONS
# ============================================================

def get_annual_reference_from_silver(
    silver: pd.DataFrame,
    cik: str,
    metric: str,
    fiscal_year: int,
) -> pd.DataFrame:
    """
    Rebuild the annual-reference candidate set from Silver so the report
    includes the actual SEC observations behind the selected reference.
    """
    s = silver.copy()
    s["cik"] = s["cik"].astype(str).str.zfill(10)

    s = s[
        (s["cik"] == str(cik).zfill(10))
        & (s["metric"].astype(str).str.lower() == str(metric).lower())
    ].copy()

    if "fiscal_year" in s.columns:
        fy = pd.to_numeric(s["fiscal_year"], errors="coerce")
        s = s[fy == fiscal_year].copy()

    if "period_type" in s.columns:
        p = s["period_type"].astype(str).str.lower()
        s = s[p.isin(["annual", "fy", "year", "12m", "twelve_month"])].copy()

    if "period_start" in s.columns and "period_end" in s.columns:
        s = to_dates(s, ["period_start", "period_end"])
        duration_days = (s["period_end"] - s["period_start"]).dt.days
        # Annual duration is approximately 1 year. Fiscal calendars can be
        # 52/53 weeks, so use a broad range.
        s = s[(duration_days >= 300) & (duration_days <= 390)].copy()

    return s


def build_annual_exceptions(
    silver: pd.DataFrame,
    gold: pd.DataFrame,
    v22_annual_path: Path | None,
) -> pd.DataFrame:

    if v22_annual_path is not None:
        annual = normalize_columns(read_table(v22_annual_path))
    else:
        # Fallback: independently identify annual discrepancies from Gold.
        records = []

        g = gold.copy()
        g["cik"] = g["cik"].astype(str).str.zfill(10)
        g["fiscal_year"] = pd.to_numeric(g["fiscal_year"], errors="coerce")

        for (cik, metric, fy), group in g.groupby(
            ["cik", "metric", "fiscal_year"], dropna=False
        ):
            if metric not in METRICS or pd.isna(fy):
                continue

            q = (
                group[group["quarter"].isin(["Q1", "Q2", "Q3", "Q4"])]
                .groupby("quarter")["value"]
                .sum()
            )

            if not all(qn in q.index for qn in ["Q1", "Q2", "Q3", "Q4"]):
                continue

            refs = get_annual_reference_from_silver(
                silver, cik, metric, int(fy)
            )

            if refs.empty:
                continue

            # Select the latest filing-date annual observation as a fallback.
            refs = refs.sort_values(
                ["filing_date", "accession_number"],
                na_position="last"
            )
            ref = refs.iloc[-1]

            annual_value = ref["value"]
            quarter_sum = float(q["Q1"] + q["Q2"] + q["Q3"] + q["Q4"])
            difference = quarter_sum - annual_value
            diff_pct = safe_pct_difference(quarter_sum, annual_value)

            if abs(diff_pct) >= MATERIAL_ANNUAL_PCT:
                records.append({
                    "cik": cik,
                    "metric": metric,
                    "fiscal_year": int(fy),
                    "q1": q["Q1"],
                    "q2": q["Q2"],
                    "q3": q["Q3"],
                    "q4": q["Q4"],
                    "quarter_sum": quarter_sum,
                    "raw_annual_reference": annual_value,
                    "difference": difference,
                    "difference_pct": diff_pct,
                    "raw_annual_form": ref.get("form"),
                    "raw_annual_filing_date": ref.get("filing_date"),
                    "raw_annual_accession": ref.get("accession_number"),
                    "raw_annual_xbrl_tag": ref.get("xbrl_tag"),
                })

        annual = pd.DataFrame(records)

    if annual.empty:
        return annual

    annual = ensure_columns(
        annual,
        [
            "cik", "metric", "fiscal_year",
            "q1", "q2", "q3", "q4",
            "quarter_sum", "raw_annual_reference",
            "difference", "difference_pct",
            "raw_annual_form", "raw_annual_filing_date",
            "raw_annual_accession", "raw_annual_xbrl_tag",
        ],
    )

    annual["cik"] = annual["cik"].astype(str).str.zfill(10)

    # Keep material failures only for this exception file.
    annual["difference_pct"] = pd.to_numeric(
        annual["difference_pct"], errors="coerce"
    )

    annual = annual[
        annual["difference_pct"].abs() >= MATERIAL_ANNUAL_PCT
    ].copy()

    # Add all Silver annual candidates for investigation.
    candidate_counts = []
    candidate_forms = []
    candidate_accessions = []
    candidate_values = []
    candidate_periods = []

    for _, row in annual.iterrows():
        refs = get_annual_reference_from_silver(
            silver,
            row["cik"],
            row["metric"],
            int(row["fiscal_year"]),
        )

        candidate_counts.append(len(refs))

        if refs.empty:
            candidate_forms.append("")
            candidate_accessions.append("")
            candidate_values.append("")
            candidate_periods.append("")
        else:
            candidate_forms.append(
                " | ".join(refs.get("form", pd.Series(dtype=str))
                           .dropna().astype(str).drop_duplicates().tolist()[:20])
            )
            candidate_accessions.append(
                " | ".join(refs.get("accession_number", pd.Series(dtype=str))
                           .dropna().astype(str).drop_duplicates().tolist()[:20])
            )
            candidate_values.append(
                " | ".join(refs.get("value", pd.Series(dtype=str))
                           .dropna().astype(str).drop_duplicates().tolist()[:20])
            )
            candidate_periods.append(
                " | ".join(
                    (
                        refs["period_start"].astype(str)
                        + " -> "
                        + refs["period_end"].astype(str)
                    ).drop_duplicates().tolist()[:20]
                )
            )

    annual["silver_annual_candidate_count"] = candidate_counts
    annual["silver_annual_candidate_values"] = candidate_values
    annual["silver_annual_candidate_forms"] = candidate_forms
    annual["silver_annual_candidate_accessions"] = candidate_accessions
    annual["silver_annual_candidate_periods"] = candidate_periods

    annual["root_cause_hint"] = np.where(
        annual["silver_annual_candidate_count"] > 1,
        "MULTIPLE_ANNUAL_FACT_VERSIONS_REVIEW_FORM_AND_FILING_DATE",
        "SINGLE_OR_NO_ANNUAL_REFERENCE_CANDIDATE",
    )

    return annual


# ============================================================
# C. ANALYTICS VALIDATION
# ============================================================

def prepare_gold_for_analytics(gold: pd.DataFrame) -> pd.DataFrame:
    g = gold.copy()

    g["cik"] = g["cik"].astype(str).str.zfill(10)
    g["fiscal_year"] = pd.to_numeric(g["fiscal_year"], errors="coerce")
    g["quarter_num"] = g["quarter"].map(quarter_number)

    # Only the three core P&L metrics are needed.
    g = g[g["metric"].isin(METRICS)].copy()

    # Gold should already be unique, but keep a deterministic selection
    # if an upstream dataset contains duplicates.
    g = g.sort_values(
        ["cik", "metric", "fiscal_year", "quarter_num"]
    )

    return g


def calculate_expected_analytics(gold: pd.DataFrame) -> pd.DataFrame:
    """
    Independently calculate the analytics expected from Gold.

    This is intentionally separate from the production analytics script.
    The point is to catch a bug shared by the production calculation.
    """
    g = prepare_gold_for_analytics(gold)

    pivot = (
        g.pivot_table(
            index=["cik", "fiscal_year", "quarter", "quarter_num"],
            columns="metric",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )

    pivot.columns.name = None

    pivot = ensure_columns(
        pivot,
        ["revenue", "operating_income", "net_income"]
    )

    pivot = pivot.sort_values(
        ["cik", "fiscal_year", "quarter_num"]
    ).reset_index(drop=True)

    # Previous quarter is based on chronological fiscal-year/quarter order.
    pivot["prev_revenue"] = pivot.groupby("cik")["revenue"].shift(1)

    pivot["revenue_qoq_expected"] = np.where(
        pivot["prev_revenue"].notna() & (pivot["prev_revenue"] != 0),
        (pivot["revenue"] - pivot["prev_revenue"]) / pivot["prev_revenue"],
        np.nan,
    )

    # YoY = same quarter in prior fiscal year.
    yoy_lookup = pivot[
        ["cik", "fiscal_year", "quarter_num", "revenue"]
    ].copy()

    yoy_lookup["fiscal_year"] = yoy_lookup["fiscal_year"] + 1
    yoy_lookup = yoy_lookup.rename(
        columns={"revenue": "prior_year_same_quarter_revenue"}
    )

    pivot = pivot.merge(
        yoy_lookup,
        on=["cik", "fiscal_year", "quarter_num"],
        how="left",
    )

    pivot["revenue_yoy_expected"] = np.where(
        pivot["prior_year_same_quarter_revenue"].notna()
        & (pivot["prior_year_same_quarter_revenue"] != 0),
        (
            pivot["revenue"]
            - pivot["prior_year_same_quarter_revenue"]
        )
        / pivot["prior_year_same_quarter_revenue"],
        np.nan,
    )

    pivot["operating_margin_expected"] = np.where(
        pivot["revenue"].notna() & (pivot["revenue"] != 0)
        & pivot["operating_income"].notna(),
        pivot["operating_income"] / pivot["revenue"],
        np.nan,
    )

    pivot["net_margin_expected"] = np.where(
        pivot["revenue"].notna() & (pivot["revenue"] != 0)
        & pivot["net_income"].notna(),
        pivot["net_income"] / pivot["revenue"],
        np.nan,
    )

    return pivot


def build_analytics_failures(
    gold: pd.DataFrame,
    analytics: pd.DataFrame,
) -> pd.DataFrame:

    expected = calculate_expected_analytics(gold)

    a = analytics.copy()
    a["cik"] = a["cik"].astype(str).str.zfill(10)
    a["fiscal_year"] = pd.to_numeric(a["fiscal_year"], errors="coerce")
    a["quarter_num"] = a["quarter"].map(quarter_number)

    # Production analytics column aliases.
    aliases = {
        "revenue_value": ["revenue_value", "revenue"],
        "revenue_qoq_actual": [
            "revenue_qoq_growth_pct",
            "revenue_qoq",
            "qoq_growth",
        ],
        "revenue_yoy_actual": [
            "revenue_yoy_growth_pct",
            "revenue_yoy",
            "yoy_growth",
        ],
        "operating_margin_actual": ["operating_margin"],
        "net_margin_actual": ["net_margin"],
    }

    for target, candidates in aliases.items():
        source = first_existing(a, candidates)
        if source and source != target:
            a[target] = a[source]
        elif target not in a.columns:
            a[target] = np.nan

    merged = expected.merge(
        a[
            [
                "cik",
                "fiscal_year",
                "quarter",
                "quarter_num",
                "revenue_value",
                "revenue_qoq_actual",
                "revenue_yoy_actual",
                "operating_margin_actual",
                "net_margin_actual",
            ]
        ],
        on=["cik", "fiscal_year", "quarter", "quarter_num"],
        how="left",
        suffixes=("_expected", "_analytics"),
    )

    # Revenue source-of-truth check.
    merged["revenue_value_difference"] = (
        merged["revenue_value"] - merged["revenue"]
    )

    merged["qoq_difference"] = (
        merged["revenue_qoq_actual"] - merged["revenue_qoq_expected"]
    )

    merged["yoy_difference"] = (
        merged["revenue_yoy_actual"] - merged["revenue_yoy_expected"]
    )

    merged["operating_margin_difference"] = (
        merged["operating_margin_actual"]
        - merged["operating_margin_expected"]
    )

    merged["net_margin_difference"] = (
        merged["net_margin_actual"]
        - merged["net_margin_expected"]
    )

    def check_row(row):
        failures = []

        if not values_close(
            row["revenue_value"],
            row["revenue"],
            rtol=0,
            atol=0.5,
        ):
            failures.append("REVENUE_VALUE_MISMATCH")

        # Only validate growth when an independent expected value exists.
        if not pd.isna(row["revenue_qoq_expected"]):
            if not values_close(
                row["revenue_qoq_actual"],
                row["revenue_qoq_expected"],
            ):
                failures.append("QOQ_GROWTH_MISMATCH")

        if not pd.isna(row["revenue_yoy_expected"]):
            if not values_close(
                row["revenue_yoy_actual"],
                row["revenue_yoy_expected"],
            ):
                failures.append("YOY_GROWTH_MISMATCH")

        if not pd.isna(row["operating_margin_expected"]):
            if not values_close(
                row["operating_margin_actual"],
                row["operating_margin_expected"],
            ):
                failures.append("OPERATING_MARGIN_MISMATCH")

        if not pd.isna(row["net_margin_expected"]):
            if not values_close(
                row["net_margin_actual"],
                row["net_margin_expected"],
            ):
                failures.append("NET_MARGIN_MISMATCH")

        return "|".join(failures)

    merged["failed_checks"] = merged.apply(check_row, axis=1)

    failures = merged[
        merged["failed_checks"].astype(str).str.len() > 0
    ].copy()

    # Root-cause classification.
    def classify(row):
        f = row["failed_checks"]

        if "REVENUE_VALUE_MISMATCH" in f:
            return "ANALYTICS_REVENUE_SOURCE_MISMATCH"

        if "QOQ_GROWTH_MISMATCH" in f and "YOY_GROWTH_MISMATCH" in f:
            return "ANALYTICS_GROWTH_LOOKUP_OR_ORDERING"

        if "QOQ_GROWTH_MISMATCH" in f:
            return "ANALYTICS_QOQ_CALCULATION"

        if "YOY_GROWTH_MISMATCH" in f:
            return "ANALYTICS_YOY_CALCULATION"

        if "OPERATING_MARGIN_MISMATCH" in f:
            return "ANALYTICS_OPERATING_MARGIN_CALCULATION"

        if "NET_MARGIN_MISMATCH" in f:
            return "ANALYTICS_NET_MARGIN_CALCULATION"

        return "REVIEW"

    failures["root_cause_hint"] = failures.apply(classify, axis=1)

    # Make output readable and focused.
    output_cols = [
        "cik",
        "fiscal_year",
        "quarter",
        "revenue",
        "revenue_value",
        "revenue_value_difference",
        "prev_revenue",
        "revenue_qoq_expected",
        "revenue_qoq_actual",
        "qoq_difference",
        "prior_year_same_quarter_revenue",
        "revenue_yoy_expected",
        "revenue_yoy_actual",
        "yoy_difference",
        "operating_income",
        "operating_margin_expected",
        "operating_margin_actual",
        "operating_margin_difference",
        "net_income",
        "net_margin_expected",
        "net_margin_actual",
        "net_margin_difference",
        "failed_checks",
        "root_cause_hint",
    ]

    failures = ensure_columns(failures, output_cols)
    # Sort while quarter_num still exists; it is an internal helper and is
    # intentionally excluded from the final output columns.
    return (
        failures
        .sort_values(["cik", "fiscal_year", "quarter_num"])
        [output_cols]
    )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    banner("STEP 12 - FINANCIAL ROOT-CAUSE DIAGNOSTIC")

    log(f"Project root : {PROJECT_ROOT}")
    log(f"Silver       : {SILVER_DIR}")
    log(f"Gold         : {GOLD_DIR}")
    log(f"Analytics    : {ANALYTICS_DIR}")
    log(f"V2.2 reports : {V22_REPORT_DIR}")
    log(f"Output       : {OUTPUT_DIR}")

    # --------------------------------------------------------
    # Discover V2.2 reports
    # --------------------------------------------------------
    banner("DISCOVERING V2.2 REPORTS")

    report_files = discover_report_files(V22_REPORT_DIR)

    if not report_files:
        log("WARNING: No V2.2 CSV/Parquet reports found.")
        log("The script will use fallback calculations where possible.")

    missing_raw_path = find_best_report(V22_REPORT_DIR, "missing_raw")
    annual_path = find_best_report(V22_REPORT_DIR, "annual")
    analytics_report_path = find_best_report(V22_REPORT_DIR, "analytics")

    log(f"Missing-raw report : {missing_raw_path}")
    log(f"Annual report      : {annual_path}")
    log(f"Analytics report   : {analytics_report_path}")

    # --------------------------------------------------------
    # Load datasets
    # --------------------------------------------------------
    banner("LOADING SOURCE DATA")

    silver = load_all_silver()
    gold = load_all_gold()
    analytics = load_all_analytics()

    log(f"Silver rows    : {len(silver):,}")
    log(f"Gold rows      : {len(gold):,}")
    log(f"Analytics rows : {len(analytics):,}")

    # Normalize common columns.
    silver = ensure_columns(
        silver,
        [
            "cik", "metric", "value", "period_start", "period_end",
            "fiscal_year", "form", "filing_date", "accession_number",
            "xbrl_tag", "period_type",
        ],
    )

    gold = ensure_columns(
        gold,
        [
            "cik", "metric", "value", "period_start", "period_end",
            "fiscal_year", "quarter", "source_period_type",
            "quarter_assignment_method",
        ],
    )

    analytics = ensure_columns(
        analytics,
        ["cik", "fiscal_year", "quarter"],
    )

    silver["cik"] = silver["cik"].astype(str).str.zfill(10)
    gold["cik"] = gold["cik"].astype(str).str.zfill(10)
    analytics["cik"] = analytics["cik"].astype(str).str.zfill(10)

    silver["fiscal_year"] = pd.to_numeric(
        silver["fiscal_year"], errors="coerce"
    )
    gold["fiscal_year"] = pd.to_numeric(
        gold["fiscal_year"], errors="coerce"
    )
    analytics["fiscal_year"] = pd.to_numeric(
        analytics["fiscal_year"], errors="coerce"
    )

    # --------------------------------------------------------
    # A. RAW -> GOLD
    # --------------------------------------------------------
    banner("A. RAW -> GOLD EXCEPTIONS")

    raw_exceptions = build_raw_gold_exceptions(
        silver,
        gold,
        missing_raw_path,
    )

    raw_output = OUTPUT_DIR / "raw_gold_exceptions.csv"
    raw_exceptions.to_csv(raw_output, index=False)

    log(f"Rows: {len(raw_exceptions):,}")
    log(f"Saved: {raw_output}")

    if not raw_exceptions.empty:
        print(raw_exceptions.to_string(index=False))

    # --------------------------------------------------------
    # B. ANNUAL RECONCILIATION
    # --------------------------------------------------------
    banner("B. ANNUAL RECONCILIATION EXCEPTIONS")

    annual_exceptions = build_annual_exceptions(
        silver,
        gold,
        annual_path,
    )

    annual_output = OUTPUT_DIR / "annual_reconciliation_exceptions.csv"
    annual_exceptions.to_csv(annual_output, index=False)

    log(f"Material failures: {len(annual_exceptions):,}")
    log(f"Saved: {annual_output}")

    if not annual_exceptions.empty:
        print(
            annual_exceptions[
                [
                    "cik",
                    "metric",
                    "fiscal_year",
                    "quarter_sum",
                    "raw_annual_reference",
                    "difference",
                    "difference_pct",
                    "raw_annual_form",
                    "raw_annual_filing_date",
                    "raw_annual_accession",
                    "root_cause_hint",
                ]
            ].to_string(index=False)
        )

    # --------------------------------------------------------
    # C. ANALYTICS
    # --------------------------------------------------------
    banner("C. ANALYTICS EXCEPTIONS")

    analytics_exceptions = build_analytics_failures(
        gold,
        analytics,
    )

    analytics_output = OUTPUT_DIR / "analytics_failures.csv"
    analytics_exceptions.to_csv(analytics_output, index=False)

    log(f"Failures: {len(analytics_exceptions):,}")
    log(f"Saved: {analytics_output}")

    if not analytics_exceptions.empty:
        print(
            analytics_exceptions[
                [
                    "cik",
                    "fiscal_year",
                    "quarter",
                    "failed_checks",
                    "root_cause_hint",
                ]
            ].to_string(index=False)
        )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------
    banner("STEP 12 SUMMARY")

    summary = pd.DataFrame([
        {
            "exception_type": "raw_gold",
            "rows": len(raw_exceptions),
            "output": str(raw_output),
        },
        {
            "exception_type": "annual_reconciliation",
            "rows": len(annual_exceptions),
            "output": str(annual_output),
        },
        {
            "exception_type": "analytics",
            "rows": len(analytics_exceptions),
            "output": str(analytics_output),
        },
    ])

    summary_path = OUTPUT_DIR / "step12_summary.csv"
    summary.to_csv(summary_path, index=False)

    print(summary.to_string(index=False))
    log(f"\nSummary saved: {summary_path}")

    banner("STEP 12 COMPLETE")

    log("IMPORTANT:")
    log("This step is diagnostic only.")
    log("No Silver, Gold, or Analytics files were modified.")
    log("Do not change production derivation logic until these exceptions are reviewed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nFATAL ERROR: {type(exc).__name__}: {exc}")
        raise