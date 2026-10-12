from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

DEFAULT_SILVER = Path(
    r"C:\Users\stalathi\Desktop\private-company-intelligence\data\silver\financial_facts"
)
DEFAULT_GOLD = Path(
    r"C:\Users\stalathi\Desktop\private-company-intelligence\data\gold\financials_quarterly_final"
)
DEFAULT_ANALYTICS = Path(
    r"C:\Users\stalathi\Desktop\private-company-intelligence\data\gold\financials_analytics"
)
DEFAULT_REPORT = Path(
    r"C:\Users\stalathi\Desktop\private-company-intelligence\data\gold\lineage_validation_v2_2"
)

CORE_METRICS = {"revenue", "operating_income", "net_income"}

ABS_TOLERANCE = 1.0
REL_TOLERANCE = 1e-9


# ============================================================
# LOGGING / BASIC HELPERS
# ============================================================

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


def ensure_datetime(df: pd.DataFrame, col: str) -> None:
    df[col] = pd.to_datetime(df[col], errors="coerce")


def numeric_close(a, b) -> bool:
    if pd.isna(a) or pd.isna(b):
        return False
    return bool(
        math.isclose(
            float(a),
            float(b),
            rel_tol=REL_TOLERANCE,
            abs_tol=ABS_TOLERANCE,
        )
    )


def safe_pct(numerator, denominator):
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return np.nan
    return numerator / denominator


def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return pd.read_parquet(path, engine="pyarrow")


# ============================================================
# SILVER DISCOVERY
# ============================================================

def discover_silver_files(root: Path) -> list[tuple[str, Path]]:
    """
    Discover ALL CIK-partitioned Silver financial fact files.

    Supported layouts:
      data/silver/financial_facts/cik=0000320193/financial_facts.parquet
      data/silver/financial_facts/cik=0000320193/*.parquet
      data/silver/financial_facts.parquet
    """
    if not root.exists():
        raise FileNotFoundError(f"Silver path not found: {root}")

    result: list[tuple[str, Path]] = []

    partition_files = sorted(root.glob("cik=*/financial_facts.parquet"))
    if partition_files:
        for path in partition_files:
            cik = normalize_cik(path.parent.name.replace("cik=", ""))
            result.append((cik, path))
        return result

    all_partition_parquets = sorted(root.glob("cik=*/*.parquet"))
    if all_partition_parquets:
        for path in all_partition_parquets:
            cik = normalize_cik(path.parent.name.replace("cik=", ""))
            result.append((cik, path))
        return result

    if root.is_file() and root.suffix.lower() == ".parquet":
        return [("", root)]

    direct = sorted(root.glob("*.parquet"))
    if direct:
        return [("", p) for p in direct]

    raise FileNotFoundError(f"No Silver parquet files found under {root}")


def load_all_silver(root: Path) -> pd.DataFrame:
    files = discover_silver_files(root)

    log(f"Silver files discovered: {len(files):,}")
    for cik, path in files:
        log(f"  {cik or '<unpartitioned>'}: {path}")

    frames = []
    for _, path in files:
        frames.append(load_parquet(path))

    if not frames:
        raise FileNotFoundError("No Silver data loaded.")

    silver_raw = pd.concat(frames, ignore_index=True)
    return silver_raw


# ============================================================
# GOLD / ANALYTICS DISCOVERY
# ============================================================

def discover_gold_files(root: Path) -> list[tuple[str, Path]]:
    if not root.exists():
        raise FileNotFoundError(f"Gold directory not found: {root}")

    result = []
    for path in sorted(root.glob("cik=*/financials_quarterly.parquet")):
        cik = normalize_cik(path.parent.name.replace("cik=", ""))
        result.append((cik, path))

    if not result:
        raise FileNotFoundError(
            f"No financials_quarterly.parquet files found under {root}"
        )

    return result


def discover_analytics_files(root: Path) -> dict[str, Path]:
    if not root.exists():
        return {}

    result = {}
    for path in sorted(root.glob("cik=*/financials_analytics.parquet")):
        cik = normalize_cik(path.parent.name.replace("cik=", ""))
        result[cik] = path

    return result


# ============================================================
# NORMALIZATION
# ============================================================

def normalize_silver(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    aliases = {
        "cik": ["cik", "CIK"],
        "company_name": ["company_name", "company"],
        "metric": ["metric", "concept", "metric_name", "financial_metric"],
        "xbrl_tag": ["xbrl_tag", "tag", "concept_tag"],
        "unit": ["unit", "units"],
        "value": ["value", "val"],
        "period_start": ["period_start", "start", "start_date"],
        "period_end": ["period_end", "end", "end_date", "filed_period_end"],
        "fiscal_year": ["fiscal_year", "fy", "fiscalYear"],
        "fiscal_period": ["fiscal_period", "fp", "fiscalPeriod"],
        "form": ["form", "form_type", "filing_form"],
        "filing_date": ["filing_date", "filed", "filed_date"],
        "accession_number": ["accession_number", "accession", "accessionNumber"],
        "frame": ["frame"],
        "period_type": ["period_type", "periodType", "duration_type"],
        "fact_key": ["fact_key", "key"],
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
            f"Silver file missing required columns: {missing}. "
            f"Available columns: {list(df.columns)}"
        )

    df["cik"] = df["cik"].map(normalize_cik)
    df["metric"] = df["metric"].astype(str).str.lower().str.strip()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce")

    ensure_datetime(df, "period_start")
    ensure_datetime(df, "period_end")

    for c, default in {
        "company_name": "",
        "xbrl_tag": "",
        "unit": "",
        "fiscal_period": "",
        "form": "",
        "accession_number": "",
        "frame": "",
        "fact_key": "",
    }.items():
        if c not in df.columns:
            df[c] = default

    if "filing_date" not in df.columns:
        df["filing_date"] = pd.NaT
    else:
        ensure_datetime(df, "filing_date")

    df["period_type"] = (
        df["period_type"].astype(str).str.lower().str.strip()
    )
    df["form"] = df["form"].astype(str).str.upper().str.strip()
    df["fiscal_period"] = (
        df["fiscal_period"].astype(str).str.upper().str.strip()
    )

    return df


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
        "quarter_assignment_method": [
            "quarter_assignment_method",
            "assignment_method",
        ],
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
        "metric",
    ]
    missing = [c for c in required if c not in x.columns]
    if missing:
        raise KeyError(
            f"Gold file missing required columns: {missing}. "
            f"Available: {list(x.columns)}"
        )

    x["fiscal_year"] = pd.to_numeric(
        x["fiscal_year"], errors="coerce"
    ).astype("Int64")
    x["quarter"] = x["quarter"].astype(str).str.upper().str.strip()
    x["value"] = pd.to_numeric(x["value"], errors="coerce")
    x["metric"] = x["metric"].astype(str).str.lower().str.strip()

    ensure_datetime(x, "period_start")
    ensure_datetime(x, "period_end")

    if "source_period_type" not in x.columns:
        x["source_period_type"] = ""
    if "quarter_assignment_method" not in x.columns:
        x["quarter_assignment_method"] = ""

    return x


# ============================================================
# PERIOD CLASSIFICATION
# ============================================================

def is_instant_row(row: pd.Series) -> bool:
    """
    Instant facts have no duration start date in normal SEC facts.
    They should not be treated as invalid periods.
    """
    return pd.isna(row["period_start"]) and pd.notna(row["period_end"])


def quarter_period_type(pt: str) -> bool:
    return str(pt).lower().strip() in {
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
    normalized = str(pt).lower().strip()

    if months == 3:
        return quarter_period_type(normalized)
    if months == 6:
        return normalized in {
            "six_month",
            "6_month",
            "6m",
            "half_year",
        }
    if months == 9:
        return normalized in {"nine_month", "9_month", "9m"}
    if months == 12:
        return normalized in {"annual", "12_month", "12m", "fy"}

    return False


def duration_months(start, end) -> float:
    if pd.isna(start) or pd.isna(end):
        return np.nan
    return (end - start).days / 30.4375


# ============================================================
# SILVER AUDIT
# ============================================================

def audit_silver(silver: pd.DataFrame, report_dir: Path) -> None:
    log("\n" + "=" * 60)
    log("SILVER DATA AUDIT V2")
    log("=" * 60)

    log(f"Silver rows: {len(silver):,}")
    log(f"CIKs: {silver['cik'].nunique():,}")
    log(f"Metrics: {silver['metric'].nunique():,}")

    # Duplicate definition deliberately includes source identity.
    duplicate_cols = [
        "cik",
        "metric",
        "xbrl_tag",
        "unit",
        "value",
        "period_start",
        "period_end",
        "period_type",
        "fiscal_year",
        "form",
        "filing_date",
        "accession_number",
    ]
    duplicate_cols = [c for c in duplicate_cols if c in silver.columns]

    dup = silver[silver.duplicated(duplicate_cols, keep=False)].copy()
    dup.to_csv(report_dir / "01_silver_duplicates_v2.csv", index=False)

    log(f"Duplicate Silver observations: {len(dup):,}")

    # Duration-period invalidity only.
    invalid_duration = silver[
        silver["period_start"].notna()
        & silver["period_end"].notna()
        & (silver["period_end"] < silver["period_start"])
    ].copy()

    invalid_duration.to_csv(
        report_dir / "02_silver_invalid_duration_periods_v2.csv",
        index=False,
    )

    log(f"Invalid duration periods: {len(invalid_duration):,}")

    instant = silver[
        silver["period_start"].isna() & silver["period_end"].notna()
    ].copy()

    instant.to_csv(
        report_dir / "03_silver_instant_facts_v2.csv",
        index=False,
    )

    log(f"Instant facts excluded from duration validation: {len(instant):,}")

    core = silver[silver["metric"].isin(CORE_METRICS)].copy()
    core.to_csv(
        report_dir / "04_core_silver_facts_v2.csv",
        index=False,
    )


# ============================================================
# RAW CANDIDATE SELECTION
# ============================================================

def build_silver_candidates(
    silver: pd.DataFrame,
    cik: str,
    metric: str,
) -> pd.DataFrame:

    df = silver[
        (silver["cik"] == cik)
        & (silver["metric"] == metric)
        & silver["value"].notna()
        & silver["period_end"].notna()
    ].copy()

    if df.empty:
        return df

    df["form_priority"] = np.where(
        df["form"].isin(["10-K", "10-Q"]),
        1,
        0,
    )

    # Later filing wins only when we need a single representative.
    df["filing_date_sort"] = df["filing_date"].fillna(pd.Timestamp.min)

    return df.sort_values(
        [
            "period_end",
            "period_start",
            "filing_date_sort",
            "form_priority",
            "accession_number",
        ]
    )


def exact_period_rows(
    df: pd.DataFrame,
    start,
    end,
    period_type=None,
) -> pd.DataFrame:

    mask = df["period_end"].eq(end)

    if pd.notna(start):
        mask &= df["period_start"].eq(start)

    if period_type is not None:
        mask &= df["period_type"].eq(period_type)

    return df[mask].copy()


def choose_best(rows: pd.DataFrame) -> pd.Series | None:
    """
    Select one representative candidate for diagnostics only.

    Validation never depends on this selection: ALL candidates are tested.
    This function deliberately creates helper columns locally so a caller
    can pass either Silver rows or candidate dictionaries without relying
    on Silver-only columns such as form_priority.
    """
    if rows is None or rows.empty:
        return None

    x = rows.copy()

    if "filing_date" not in x.columns:
        x["filing_date"] = pd.NaT
    x["filing_date_sort"] = pd.to_datetime(
        x["filing_date"], errors="coerce"
    ).fillna(pd.Timestamp.min)

    if "form_priority" not in x.columns:
        form = (
            x["form"].astype(str).str.upper().str.strip()
            if "form" in x.columns
            else pd.Series("", index=x.index)
        )
        x["form_priority"] = form.isin(["10-K", "10-Q"]).astype(int)

    if "accession_number" not in x.columns:
        x["accession_number"] = ""

    return x.sort_values(
        ["filing_date_sort", "form_priority", "accession_number"],
        kind="stable",
    ).iloc[-1]


def candidate_descriptor(row: pd.Series | None) -> dict:
    if row is None:
        return {
            "value": np.nan,
            "xbrl_tag": "",
            "unit": "",
            "form": "",
            "filing_date": pd.NaT,
            "accession_number": "",
            "period_start": pd.NaT,
            "period_end": pd.NaT,
            "period_type": "",
            "fiscal_year": np.nan,
            "fiscal_period": "",
            "frame": "",
            "fact_key": "",
        }

    return {
        "value": row["value"],
        "xbrl_tag": row.get("xbrl_tag", ""),
        "unit": row.get("unit", ""),
        "form": row.get("form", ""),
        "form_priority": row.get("form_priority", 0),
        "filing_date": row.get("filing_date", pd.NaT),
        "accession_number": row.get("accession_number", ""),
        "period_start": row.get("period_start", pd.NaT),
        "period_end": row.get("period_end", pd.NaT),
        "period_type": row.get("period_type", ""),
        "fiscal_year": row.get("fiscal_year", np.nan),
        "fiscal_period": row.get("fiscal_period", ""),
        "frame": row.get("frame", ""),
        "fact_key": row.get("fact_key", ""),
    }


# ============================================================
# RAW -> GOLD EXPECTATION
# ============================================================

def derive_expected_candidates(
    silver_metric: pd.DataFrame,
    quarter: str,
    q_start,
    q_end,
    fy_start,
    fy_end,
) -> list[dict]:

    """
    Return ALL plausible raw derivations, not just one.

    This is the key difference from V1:
      - We do not arbitrarily throw away alternative SEC facts.
      - Every candidate is retained.
      - We can then ask whether Gold matches any valid SEC candidate.
    """

    candidates = []

    # --------------------------------------------------------
    # Direct quarter
    # --------------------------------------------------------
    direct = exact_period_rows(
        silver_metric,
        q_start,
        q_end,
    )
    direct = direct[
        direct["period_type"].map(quarter_period_type)
    ]

    for _, row in direct.iterrows():
        d = candidate_descriptor(row)
        d.update(
            {
                "expected_value": row["value"],
                "derivation_method": "direct_quarter",
                "component_1_accession": row["accession_number"],
                "component_2_accession": "",
                "component_1_value": row["value"],
                "component_2_value": np.nan,
                "component_3_accession": "",
                "component_3_value": np.nan,
            }
        )
        candidates.append(d)

    # --------------------------------------------------------
    # Q1
    # --------------------------------------------------------
    if quarter == "Q1":
        # Same 3M duration, retained as explicit cumulative_q1 route.
        q1 = exact_period_rows(silver_metric, q_start, q_end)
        q1 = q1[
            q1["period_type"].map(quarter_period_type)
        ]

        for _, row in q1.iterrows():
            d = candidate_descriptor(row)
            d.update(
                {
                    "expected_value": row["value"],
                    "derivation_method": "cumulative_q1",
                    "component_1_accession": row["accession_number"],
                    "component_1_value": row["value"],
                    "component_2_accession": "",
                    "component_2_value": np.nan,
                    "component_3_accession": "",
                    "component_3_value": np.nan,
                }
            )
            candidates.append(d)

    # --------------------------------------------------------
    # Q2 = 6M - Q1
    # --------------------------------------------------------
    six = exact_period_rows(
        silver_metric,
        fy_start,
        q_end,
    )
    six = six[
        six["period_type"].map(
            lambda x: cumulative_period_type(x, 6)
        )
    ]

    q1_rows = exact_period_rows(
        silver_metric,
        fy_start,
        q_start,
    )
    q1_rows = q1_rows[
        q1_rows["period_type"].map(quarter_period_type)
    ]

    if quarter == "Q2":
        for _, six_row in six.iterrows():
            for _, q1_row in q1_rows.iterrows():
                expected = six_row["value"] - q1_row["value"]

                d = candidate_descriptor(six_row)
                d.update(
                    {
                        "expected_value": expected,
                        "derivation_method": "derived_6m_minus_q1",
                        "component_1_accession": six_row["accession_number"],
                        "component_1_value": six_row["value"],
                        "component_2_accession": q1_row["accession_number"],
                        "component_2_value": q1_row["value"],
                        "component_3_accession": "",
                        "component_3_value": np.nan,
                    }
                )
                candidates.append(d)

    # --------------------------------------------------------
    # Q3 = 9M - 6M
    # --------------------------------------------------------
    nine = exact_period_rows(
        silver_metric,
        fy_start,
        q_end,
    )
    nine = nine[
        nine["period_type"].map(
            lambda x: cumulative_period_type(x, 9)
        )
    ]

    six_q3 = exact_period_rows(
        silver_metric,
        fy_start,
        q_start - pd.Timedelta(days=1)
        if pd.notna(q_start)
        else q_start,
    )
    # Period boundaries in SEC data are exact, but q_start can have
    # a one-day gap from prior quarter. Use duration endpoint matching
    # from Gold instead below if exact date exists.
    six_q3 = six_q3[
        six_q3["period_type"].map(
            lambda x: cumulative_period_type(x, 6)
        )
    ]

    # Also search all 6M rows whose end is before Q3 end and whose
    # start is the same FY start. This handles fiscal calendars.
    if quarter == "Q3":
        six_q3 = silver_metric[
            (silver_metric["period_start"] == fy_start)
            & (silver_metric["period_end"] < q_end)
            & silver_metric["period_end"].notna()
            & silver_metric["period_type"].map(
                lambda x: cumulative_period_type(x, 6)
            )
        ].copy()

        for _, nine_row in nine.iterrows():
            for _, six_row in six_q3.iterrows():
                expected = nine_row["value"] - six_row["value"]

                d = candidate_descriptor(nine_row)
                d.update(
                    {
                        "expected_value": expected,
                        "derivation_method": "derived_9m_minus_6m",
                        "component_1_accession": nine_row["accession_number"],
                        "component_1_value": nine_row["value"],
                        "component_2_accession": six_row["accession_number"],
                        "component_2_value": six_row["value"],
                        "component_3_accession": "",
                        "component_3_value": np.nan,
                    }
                )
                candidates.append(d)

    # --------------------------------------------------------
    # Q4 = FY - 9M
    # --------------------------------------------------------
    annual = exact_period_rows(
        silver_metric,
        fy_start,
        fy_end,
    )
    annual = annual[
        annual["period_type"].map(
            lambda x: cumulative_period_type(x, 12)
        )
    ]

    nine_q4 = exact_period_rows(
        silver_metric,
        fy_start,
        q_start - pd.Timedelta(days=1)
        if pd.notna(q_start)
        else q_start,
    )
    nine_q4 = nine_q4[
        nine_q4["period_type"].map(
            lambda x: cumulative_period_type(x, 9)
        )
    ]

    if quarter == "Q4":
        # Fiscal calendars can have one-day boundary differences, so
        # use all FY-started 9M facts ending before FY end.
        nine_q4 = silver_metric[
            (silver_metric["period_start"] == fy_start)
            & (silver_metric["period_end"] < fy_end)
            & silver_metric["period_end"].notna()
            & silver_metric["period_type"].map(
                lambda x: cumulative_period_type(x, 9)
            )
        ].copy()

        for _, annual_row in annual.iterrows():
            for _, nine_row in nine_q4.iterrows():
                expected = annual_row["value"] - nine_row["value"]

                d = candidate_descriptor(annual_row)
                d.update(
                    {
                        "expected_value": expected,
                        "derivation_method": "derived_fy_minus_9m",
                        "component_1_accession": annual_row["accession_number"],
                        "component_1_value": annual_row["value"],
                        "component_2_accession": nine_row["accession_number"],
                        "component_2_value": nine_row["value"],
                        "component_3_accession": "",
                        "component_3_value": np.nan,
                    }
                )
                candidates.append(d)

    # Remove exact duplicate candidate calculations.
    if not candidates:
        return []

    seen = set()
    unique = []

    for c in candidates:
        key = (
            c["expected_value"],
            c["derivation_method"],
            c["component_1_accession"],
            c["component_2_accession"],
        )
        if key not in seen:
            seen.add(key)
            unique.append(c)

    return unique


# ============================================================
# RAW -> GOLD LINEAGE
# ============================================================

def audit_lineage_v2(
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

            q_start = grow["period_start"]
            q_end = grow["period_end"]

            fy_rows = gold[
                (gold["metric"] == metric)
                & (gold["fiscal_year"] == fy)
            ]

            fy_start = (
                fy_rows["period_start"].min()
                if not fy_rows.empty
                else q_start
            )
            fy_end = (
                fy_rows["period_end"].max()
                if not fy_rows.empty
                else q_end
            )

            candidates = derive_expected_candidates(
                sm,
                quarter,
                q_start,
                q_end,
                fy_start,
                fy_end,
            )

            gold_value = grow["value"]

            if candidates:
                candidate_values = [
                    c["expected_value"]
                    for c in candidates
                    if pd.notna(c["expected_value"])
                ]

                matching = [
                    c for c in candidates
                    if numeric_close(
                        c["expected_value"],
                        gold_value,
                    )
                ]

                best = choose_best(
                    pd.DataFrame(
                        [
                            {
                                **c,
                                "filing_date": c["filing_date"],
                            }
                            for c in candidates
                        ]
                    )
                )

                # best candidate is for diagnostic display only.
                best_desc = best.to_dict() if best is not None else {}

                status = (
                    "MATCHED_RAW_CANDIDATE"
                    if matching
                    else "NO_RAW_CANDIDATE_MATCH"
                )

                rows.append(
                    {
                        "cik": cik,
                        "metric": metric,
                        "fiscal_year": fy,
                        "quarter": quarter,
                        "gold_value": gold_value,
                        "candidate_count": len(candidates),
                        "matching_candidate_count": len(matching),
                        "status": status,
                        "candidate_values": "|".join(
                            str(v) for v in candidate_values
                        ),
                        "candidate_methods": "|".join(
                            sorted(
                                set(
                                    c["derivation_method"]
                                    for c in candidates
                                )
                            )
                        ),
                        "gold_source_period_type": grow.get(
                            "source_period_type", ""
                        ),
                        "gold_assignment_method": grow.get(
                            "quarter_assignment_method", ""
                        ),
                        "best_candidate_value": best_desc.get(
                            "expected_value", np.nan
                        ),
                        "best_candidate_method": best_desc.get(
                            "derivation_method", ""
                        ),
                        "best_candidate_accession": best_desc.get(
                            "accession_number", ""
                        ),
                        "best_candidate_filing_date": best_desc.get(
                            "filing_date", pd.NaT
                        ),
                        "best_candidate_form": best_desc.get(
                            "form", ""
                        ),
                        "best_candidate_period_type": best_desc.get(
                            "period_type", ""
                        ),
                        "best_candidate_xbrl_tag": best_desc.get(
                            "xbrl_tag", ""
                        ),
                        "best_component_1_accession": best_desc.get(
                            "component_1_accession", ""
                        ),
                        "best_component_1_value": best_desc.get(
                            "component_1_value", np.nan
                        ),
                        "best_component_2_accession": best_desc.get(
                            "component_2_accession", ""
                        ),
                        "best_component_2_value": best_desc.get(
                            "component_2_value", np.nan
                        ),
                    }
                )

            else:
                rows.append(
                    {
                        "cik": cik,
                        "metric": metric,
                        "fiscal_year": fy,
                        "quarter": quarter,
                        "gold_value": gold_value,
                        "candidate_count": 0,
                        "matching_candidate_count": 0,
                        "status": "NO_RAW_DERIVATION",
                        "candidate_values": "",
                        "candidate_methods": "",
                        "gold_source_period_type": grow.get(
                            "source_period_type", ""
                        ),
                        "gold_assignment_method": grow.get(
                            "quarter_assignment_method", ""
                        ),
                        "best_candidate_value": np.nan,
                        "best_candidate_method": "",
                        "best_candidate_accession": "",
                        "best_candidate_filing_date": pd.NaT,
                        "best_candidate_form": "",
                        "best_candidate_period_type": "",
                        "best_candidate_xbrl_tag": "",
                        "best_component_1_accession": "",
                        "best_component_1_value": np.nan,
                        "best_component_2_accession": "",
                        "best_component_2_value": np.nan,
                    }
                )

    result = pd.DataFrame(rows)

    result.to_csv(
        report_dir / f"{cik}_raw_to_gold_lineage_v2.csv",
        index=False,
    )

    return result


# ============================================================
# INDEPENDENT ANNUAL RECONCILIATION
# ============================================================

def audit_annual_reconciliation_v2(
    cik: str,
    silver: pd.DataFrame,
    gold: pd.DataFrame,
    report_dir: Path,
) -> pd.DataFrame:
    """
    Independently reconcile the four Gold quarters against ALL matching
    annual SEC Silver facts.

    The report distinguishes:
      PASS                 exact/tolerance match
      MINOR_DIFFERENCE     <= 1% difference
      MATERIAL_FAILURE     > 1% difference
      NO_ANNUAL_REFERENCE  no matching annual Silver fact

    It also preserves the SEC metadata for the closest annual candidate.
    """
    records = []

    for (metric, fy), g in gold.groupby(
        ["metric", "fiscal_year"], dropna=True
    ):
        if metric not in CORE_METRICS:
            continue

        q = (
            g[g["quarter"].isin(["Q1", "Q2", "Q3", "Q4"])]
            .drop_duplicates(["quarter"], keep="last")
        )

        if len(q) != 4:
            continue

        fy_start = q["period_start"].min()
        fy_end = q["period_end"].max()
        quarter_sum = q["value"].sum()

        sm = build_silver_candidates(silver, cik, metric)

        annual = exact_period_rows(sm, fy_start, fy_end)
        annual = annual[
            annual["period_type"].map(
                lambda x: cumulative_period_type(x, 12)
            )
        ].copy()

        # Every annual candidate is retained in the diagnostic CSV.
        annual_diag = []
        for _, row in annual.iterrows():
            annual_diag.append(
                {
                    "value": row.get("value", np.nan),
                    "xbrl_tag": row.get("xbrl_tag", ""),
                    "unit": row.get("unit", ""),
                    "form": row.get("form", ""),
                    "filing_date": row.get("filing_date", pd.NaT),
                    "accession_number": row.get("accession_number", ""),
                    "period_start": row.get("period_start", pd.NaT),
                    "period_end": row.get("period_end", pd.NaT),
                    "period_type": row.get("period_type", ""),
                    "fiscal_year": row.get("fiscal_year", np.nan),
                    "fiscal_period": row.get("fiscal_period", ""),
                    "frame": row.get("frame", ""),
                    "fact_key": row.get("fact_key", ""),
                }
            )

        annual_values = [
            float(v) for v in annual["value"].dropna().tolist()
        ]

        if annual_values:
            # Choose the annual candidate closest to the independently
            # calculated quarter sum, not simply the last numeric value.
            best_idx = min(
                annual.index,
                key=lambda idx: abs(
                    float(annual.loc[idx, "value"]) - float(quarter_sum)
                ),
            )
            best_row = annual.loc[best_idx]
            best_annual = float(best_row["value"])

            difference = float(quarter_sum) - best_annual
            difference_pct = (
                abs(difference) / abs(best_annual)
                if best_annual != 0
                else np.nan
            )

            if numeric_close(quarter_sum, best_annual):
                status = "PASS"
            elif pd.notna(difference_pct) and difference_pct <= 0.01:
                status = "MINOR_DIFFERENCE"
            else:
                status = "MATERIAL_FAILURE"

            reconciled = status == "PASS"

            annual_metadata = {
                "raw_annual_reference": best_annual,
                "raw_annual_xbrl_tag": best_row.get("xbrl_tag", ""),
                "raw_annual_unit": best_row.get("unit", ""),
                "raw_annual_form": best_row.get("form", ""),
                "raw_annual_filing_date": best_row.get(
                    "filing_date", pd.NaT
                ),
                "raw_annual_accession": best_row.get(
                    "accession_number", ""
                ),
                "raw_annual_period_start": best_row.get(
                    "period_start", pd.NaT
                ),
                "raw_annual_period_end": best_row.get(
                    "period_end", pd.NaT
                ),
                "raw_annual_period_type": best_row.get(
                    "period_type", ""
                ),
                "raw_annual_fiscal_period": best_row.get(
                    "fiscal_period", ""
                ),
                "raw_annual_frame": best_row.get("frame", ""),
                "raw_annual_fact_key": best_row.get("fact_key", ""),
            }
        else:
            best_annual = np.nan
            difference = np.nan
            difference_pct = np.nan
            status = "NO_ANNUAL_REFERENCE"
            reconciled = False
            annual_metadata = {
                "raw_annual_reference": np.nan,
                "raw_annual_xbrl_tag": "",
                "raw_annual_unit": "",
                "raw_annual_form": "",
                "raw_annual_filing_date": pd.NaT,
                "raw_annual_accession": "",
                "raw_annual_period_start": pd.NaT,
                "raw_annual_period_end": pd.NaT,
                "raw_annual_period_type": "",
                "raw_annual_fiscal_period": "",
                "raw_annual_frame": "",
                "raw_annual_fact_key": "",
            }

        records.append(
            {
                "cik": cik,
                "metric": metric,
                "fiscal_year": int(fy),
                "q1": q[q["quarter"] == "Q1"]["value"].iloc[0],
                "q2": q[q["quarter"] == "Q2"]["value"].iloc[0],
                "q3": q[q["quarter"] == "Q3"]["value"].iloc[0],
                "q4": q[q["quarter"] == "Q4"]["value"].iloc[0],
                "quarter_sum": quarter_sum,
                "raw_annual_candidate_count": len(annual),
                "raw_annual_values": "|".join(
                    str(v) for v in annual_values
                ),
                "raw_annual_candidates": "|".join(
                    str(x) for x in annual_diag
                ),
                **annual_metadata,
                "difference": difference,
                "difference_pct": difference_pct,
                "reconciliation_status": status,
                "is_reconciled": reconciled,
            }
        )

    result = pd.DataFrame(records)

    result.to_csv(
        report_dir / f"{cik}_annual_reconciliation_v2_2.csv",
        index=False,
    )

    # Separate material diagnostics for rapid investigation.
    if not result.empty:
        material = result[
            result["reconciliation_status"] == "MATERIAL_FAILURE"
        ].copy()
        material.to_csv(
            report_dir / f"{cik}_annual_material_failures_v2_2.csv",
            index=False,
        )

        minor = result[
            result["reconciliation_status"] == "MINOR_DIFFERENCE"
        ].copy()
        minor.to_csv(
            report_dir / f"{cik}_annual_minor_differences_v2_2.csv",
            index=False,
        )

    return result


# ============================================================
# ANALYTICS VALIDATION
# ============================================================

def audit_analytics_v2(
    cik: str,
    gold: pd.DataFrame,
    analytics: pd.DataFrame,
    report_dir: Path,
) -> pd.DataFrame:
    """
    Independently recompute every analytics field that can be derived from
    Gold. No analytics calculation is trusted merely because it exists.

    Checks:
      - analytics revenue == Gold revenue
      - QoQ revenue growth
      - YoY revenue growth
      - operating margin, when Gold operating_income exists
      - net margin, when Gold net_income exists

    Missing source metrics are reported as NOT_TESTABLE rather than causing
    a KeyError.
    """
    a = analytics.copy()

    rename = {}
    aliases = {
        "fiscal_year": ["fiscal_year", "fy"],
        "quarter": ["quarter", "qtr"],
        "revenue_value": ["revenue_value", "revenue"],
        "revenue_qoq_growth_pct": ["revenue_qoq_growth_pct", "revenue_qoq"],
        "revenue_yoy_growth_pct": ["revenue_yoy_growth_pct", "revenue_yoy"],
        "operating_margin": ["operating_margin"],
        "net_margin": ["net_margin"],
    }
    for target, candidates in aliases.items():
        col = first_existing(a, candidates)
        if col is not None and col != target:
            rename[col] = target

    a = a.rename(columns=rename)

    required = ["fiscal_year", "quarter", "revenue_value"]
    missing = [c for c in required if c not in a.columns]
    if missing:
        raise KeyError(
            f"Analytics file missing columns: {missing}. "
            f"Available: {list(a.columns)}"
        )

    a["fiscal_year"] = pd.to_numeric(
        a["fiscal_year"], errors="coerce"
    ).astype("Int64")
    a["quarter"] = a["quarter"].astype(str).str.upper().str.strip()

    for c in [
        "revenue_value",
        "revenue_qoq_growth_pct",
        "revenue_yoy_growth_pct",
        "operating_margin",
        "net_margin",
    ]:
        if c in a.columns:
            a[c] = pd.to_numeric(a[c], errors="coerce")

    # Gold source-of-truth tables.
    revenue_gold = (
        gold[gold["metric"] == "revenue"]
        [["fiscal_year", "quarter", "value"]]
        .rename(columns={"value": "gold_revenue"})
    )

    merged = a.merge(
        revenue_gold,
        on=["fiscal_year", "quarter"],
        how="left",
    )

    merged["revenue_matches_gold"] = merged.apply(
        lambda r: (
            numeric_close(r["revenue_value"], r["gold_revenue"])
            if pd.notna(r["gold_revenue"])
            else False
        ),
        axis=1,
    )

    merged = merged.sort_values(
        ["fiscal_year", "quarter"]
    ).reset_index(drop=True)

    # Quarter-aware ordering rather than trusting lexical quarter order.
    q_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    merged["_q_order"] = merged["quarter"].map(q_order).fillna(99)
    merged = merged.sort_values(
        ["fiscal_year", "_q_order", "quarter"]
    ).reset_index(drop=True)

    # Independent QoQ calculation.
    merged["expected_qoq"] = np.nan
    for i in range(1, len(merged)):
        current = merged.loc[i, "revenue_value"]
        previous = merged.loc[i - 1, "revenue_value"]
        if (
            pd.notna(current)
            and pd.notna(previous)
            and previous != 0
        ):
            # Only compare QoQ when the previous row is actually the
            # immediately preceding fiscal quarter.
            prev_fy = merged.loc[i - 1, "fiscal_year"]
            prev_q = merged.loc[i - 1, "quarter"]
            cur_fy = merged.loc[i, "fiscal_year"]
            cur_q = merged.loc[i, "quarter"]
            expected_pair = (
                (cur_fy == prev_fy and q_order.get(cur_q, 99) == q_order.get(prev_q, 99) + 1)
                or (cur_fy == prev_fy + 1 and prev_q == "Q4" and cur_q == "Q1")
            )
            if expected_pair:
                merged.loc[i, "expected_qoq"] = (
                    current - previous
                ) / previous

    if "revenue_qoq_growth_pct" in merged.columns:
        merged["qoq_matches"] = merged.apply(
            lambda r: (
                numeric_close(
                    r["revenue_qoq_growth_pct"],
                    r["expected_qoq"],
                )
                if pd.notna(r["expected_qoq"])
                and pd.notna(r["revenue_qoq_growth_pct"])
                else True
            ),
            axis=1,
        )
        merged["qoq_status"] = np.where(
            merged["expected_qoq"].notna()
            & merged["revenue_qoq_growth_pct"].notna(),
            np.where(merged["qoq_matches"], "PASS", "FAIL"),
            "NOT_TESTABLE",
        )
    else:
        merged["qoq_matches"] = True
        merged["qoq_status"] = "NOT_PRESENT"

    # Independent YoY calculation.
    lookup = merged.set_index(
        ["fiscal_year", "quarter"]
    )["revenue_value"].to_dict()

    expected_yoy = []
    for _, r in merged.iterrows():
        if pd.isna(r["fiscal_year"]):
            expected_yoy.append(np.nan)
            continue

        prev = lookup.get(
            (int(r["fiscal_year"]) - 1, r["quarter"])
        )
        expected_yoy.append(
            safe_pct(r["revenue_value"] - prev, prev)
            if prev is not None
            else np.nan
        )

    merged["expected_yoy"] = expected_yoy

    if "revenue_yoy_growth_pct" in merged.columns:
        merged["yoy_matches"] = merged.apply(
            lambda r: (
                numeric_close(
                    r["revenue_yoy_growth_pct"],
                    r["expected_yoy"],
                )
                if pd.notna(r["expected_yoy"])
                and pd.notna(r["revenue_yoy_growth_pct"])
                else True
            ),
            axis=1,
        )
        merged["yoy_status"] = np.where(
            merged["expected_yoy"].notna()
            & merged["revenue_yoy_growth_pct"].notna(),
            np.where(merged["yoy_matches"], "PASS", "FAIL"),
            "NOT_TESTABLE",
        )
    else:
        merged["yoy_matches"] = True
        merged["yoy_status"] = "NOT_PRESENT"

    # Operating income is optional for a company/period.
    op = gold[gold["metric"] == "operating_income"][
        ["fiscal_year", "quarter", "value"]
    ].rename(columns={"value": "gold_operating_income"})

    if not op.empty:
        merged = merged.merge(
            op,
            on=["fiscal_year", "quarter"],
            how="left",
        )
        merged["expected_operating_margin"] = np.where(
            merged["revenue_value"].ne(0),
            merged["gold_operating_income"] / merged["revenue_value"],
            np.nan,
        )
    else:
        merged["gold_operating_income"] = np.nan
        merged["expected_operating_margin"] = np.nan

    if "operating_margin" in merged.columns:
        merged["operating_margin_matches"] = merged.apply(
            lambda r: (
                numeric_close(
                    r["operating_margin"],
                    r["expected_operating_margin"],
                )
                if pd.notna(r["expected_operating_margin"])
                and pd.notna(r["operating_margin"])
                else True
            ),
            axis=1,
        )
        merged["operating_margin_status"] = np.where(
            merged["expected_operating_margin"].notna()
            & merged["operating_margin"].notna(),
            np.where(
                merged["operating_margin_matches"], "PASS", "FAIL"
            ),
            "NOT_TESTABLE",
        )
    else:
        merged["operating_margin_matches"] = True
        merged["operating_margin_status"] = "NOT_PRESENT"

    # Net income is optional for a company/period.
    ni = gold[gold["metric"] == "net_income"][
        ["fiscal_year", "quarter", "value"]
    ].rename(columns={"value": "gold_net_income"})

    if not ni.empty:
        merged = merged.merge(
            ni,
            on=["fiscal_year", "quarter"],
            how="left",
        )
        merged["expected_net_margin"] = np.where(
            merged["revenue_value"].ne(0),
            merged["gold_net_income"] / merged["revenue_value"],
            np.nan,
        )
    else:
        merged["gold_net_income"] = np.nan
        merged["expected_net_margin"] = np.nan

    if "net_margin" in merged.columns:
        merged["net_margin_matches"] = merged.apply(
            lambda r: (
                numeric_close(
                    r["net_margin"],
                    r["expected_net_margin"],
                )
                if pd.notna(r["expected_net_margin"])
                and pd.notna(r["net_margin"])
                else True
            ),
            axis=1,
        )
        merged["net_margin_status"] = np.where(
            merged["expected_net_margin"].notna()
            & merged["net_margin"].notna(),
            np.where(merged["net_margin_matches"], "PASS", "FAIL"),
            "NOT_TESTABLE",
        )
    else:
        merged["net_margin_matches"] = True
        merged["net_margin_status"] = "NOT_PRESENT"

    merged.drop(columns=["_q_order"], inplace=True, errors="ignore")

    merged.to_csv(
        report_dir / f"{cik}_analytics_validation_v2_2.csv",
        index=False,
    )

    return merged


# ============================================================
# DIAGNOSTIC SUMMARY HELPERS
# ============================================================

def count_analytics_failures(av: pd.DataFrame) -> int:
    """Count only actual failed checks; NOT_TESTABLE is not a failure."""
    if av.empty:
        return 0

    failure_cols = [
        "revenue_matches_gold",
        "qoq_matches",
        "yoy_matches",
        "operating_margin_matches",
        "net_margin_matches",
    ]
    existing = [c for c in failure_cols if c in av.columns]
    if not existing:
        return 0

    failed = pd.DataFrame(index=av.index)
    for c in existing:
        failed[c] = av[c].fillna(True).astype(bool)

    return int((~failed.all(axis=1)).sum())


def write_cross_company_diagnostics(
    summary_df: pd.DataFrame,
    combined_lineage: pd.DataFrame,
    combined_reconciliation: pd.DataFrame,
    report_dir: Path,
) -> None:
    """
    Produce compact cross-company reports that are useful for deciding
    whether the pipeline needs correction or only documented exceptions.
    """
    summary_df.to_csv(
        report_dir / "00_validation_summary_v2_2.csv",
        index=False,
    )

    if not combined_lineage.empty:
        combined_lineage.to_csv(
            report_dir / "00_raw_to_gold_lineage_all_v2_2.csv",
            index=False,
        )

    if not combined_reconciliation.empty:
        combined_reconciliation.to_csv(
            report_dir / "00_annual_reconciliation_all_v2_2.csv",
            index=False,
        )

        material = combined_reconciliation[
            combined_reconciliation["reconciliation_status"]
            == "MATERIAL_FAILURE"
        ].copy()

        minor = combined_reconciliation[
            combined_reconciliation["reconciliation_status"]
            == "MINOR_DIFFERENCE"
        ].copy()

        material.to_csv(
            report_dir / "00_annual_material_failures_all_v2_2.csv",
            index=False,
        )
        minor.to_csv(
            report_dir / "00_annual_minor_differences_all_v2_2.csv",
            index=False,
        )


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Independent SEC Silver -> Gold -> Analytics "
            "financial lineage validator V2."
        )
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
    log("FINANCIAL LINEAGE VALIDATION V2.2")
    log("=" * 60)
    log(f"Silver    : {args.silver}")
    log(f"Gold      : {args.gold}")
    log(f"Analytics : {args.analytics}")
    log(f"Reports   : {args.report}")

    # --------------------------------------------------------
    # LOAD ALL SILVER
    # --------------------------------------------------------
    log("\nLoading ALL Silver partitions...")

    silver_raw = load_all_silver(args.silver)
    silver = normalize_silver(silver_raw)

    log(f"Total Silver rows loaded: {len(silver):,}")

    audit_silver(silver, args.report)

    # --------------------------------------------------------
    # GOLD / ANALYTICS DISCOVERY
    # --------------------------------------------------------
    gold_files = discover_gold_files(args.gold)
    analytics_files = discover_analytics_files(args.analytics)

    log(f"\nGold companies discovered: {len(gold_files):,}")
    log(f"Analytics companies discovered: {len(analytics_files):,}")

    summary = []
    all_lineage = []
    all_reconciliation = []

    # --------------------------------------------------------
    # COMPANY-BY-COMPANY VALIDATION
    # --------------------------------------------------------
    for cik, gold_path in gold_files:

        log("\n" + "=" * 60)
        log(f"PROCESSING CIK: {cik}")
        log("=" * 60)

        try:
            gold = normalize_gold(
                load_parquet(gold_path)
            )

            silver_cik = silver[
                silver["cik"] == cik
            ].copy()

            log(f"Silver rows: {len(silver_cik):,}")
            log(f"Gold rows: {len(gold):,}")

            # ------------------------------------------------
            # Gold duplicate audit
            # ------------------------------------------------
            key_cols = [
                "metric",
                "fiscal_year",
                "quarter",
            ]

            gold_dup = gold[
                gold.duplicated(
                    key_cols,
                    keep=False,
                )
            ]

            log(
                f"Gold duplicate keys: "
                f"{len(gold_dup):,}"
            )

            # ------------------------------------------------
            # Raw -> Gold
            # ------------------------------------------------
            lineage = audit_lineage_v2(
                cik,
                silver,
                gold,
                args.report,
            )

            all_lineage.append(lineage)

            matched = lineage[
                lineage["status"]
                == "MATCHED_RAW_CANDIDATE"
            ]

            no_match = lineage[
                lineage["status"]
                == "NO_RAW_CANDIDATE_MATCH"
            ]

            no_raw = lineage[
                lineage["status"]
                == "NO_RAW_DERIVATION"
            ]

            log(
                f"Gold rows with a matching raw candidate: "
                f"{len(matched):,}"
            )
            log(
                f"Gold rows with raw candidates but NO match: "
                f"{len(no_match):,}"
            )
            log(
                f"Gold rows with no raw derivation: "
                f"{len(no_raw):,}"
            )

            # ------------------------------------------------
            # Annual reconciliation
            # ------------------------------------------------
            reconciliation = audit_annual_reconciliation_v2(
                cik,
                silver,
                gold,
                args.report,
            )

            all_reconciliation.append(
                reconciliation
            )

            if not reconciliation.empty:
                annual_failures = reconciliation[
                    ~reconciliation["is_reconciled"]
                ]
            else:
                annual_failures = reconciliation

            log(
                f"Independent annual checks: "
                f"{len(reconciliation):,}"
            )
            log(
                f"Annual reconciliation failures: "
                f"{len(annual_failures):,}"
            )

            # ------------------------------------------------
            # Analytics
            # ------------------------------------------------
            analytics_rows = 0
            analytics_failures = 0

            analytics_path = analytics_files.get(cik)

            if analytics_path is not None:
                analytics = load_parquet(
                    analytics_path
                )

                av = audit_analytics_v2(
                    cik,
                    gold,
                    analytics,
                    args.report,
                )

                analytics_rows = len(av)

                check_cols = [
                    "revenue_matches_gold",
                    "qoq_matches",
                    "yoy_matches",
                    "operating_margin_matches",
                    "net_margin_matches",
                ]

                existing = [
                    c for c in check_cols
                    if c in av.columns
                ]

                if existing:
                    analytics_failures = count_analytics_failures(av)

                log(
                    f"Analytics rows: "
                    f"{analytics_rows:,}"
                )
                log(
                    f"Analytics validation failures: "
                    f"{analytics_failures:,}"
                )

            summary.append(
                {
                    "cik": cik,
                    "silver_rows": len(silver_cik),
                    "gold_rows": len(gold),
                    "gold_duplicate_rows": len(gold_dup),
                    "raw_candidate_matches": len(matched),
                    "raw_candidate_no_match": len(no_match),
                    "raw_derivation_missing": len(no_raw),
                    "annual_checks": len(reconciliation),
                    "annual_failures": len(annual_failures),
                    "analytics_rows": analytics_rows,
                    "analytics_failures": analytics_failures,
                }
            )

        except Exception as exc:
            log(
                f"\nERROR processing {cik}: "
                f"{type(exc).__name__}: {exc}"
            )

            summary.append(
                {
                    "cik": cik,
                    "silver_rows": len(
                        silver[silver["cik"] == cik]
                    ),
                    "gold_rows": np.nan,
                    "gold_duplicate_rows": np.nan,
                    "raw_candidate_matches": np.nan,
                    "raw_candidate_no_match": np.nan,
                    "raw_derivation_missing": np.nan,
                    "annual_checks": np.nan,
                    "annual_failures": np.nan,
                    "analytics_rows": np.nan,
                    "analytics_failures": np.nan,
                }
            )

    # --------------------------------------------------------
    # COMBINED REPORTS
    # --------------------------------------------------------
    summary_df = pd.DataFrame(summary)

    summary_df.to_csv(
        args.report / "00_validation_summary_v2_2.csv",
        index=False,
    )

    if all_lineage:
        combined_lineage = pd.concat(
            all_lineage,
            ignore_index=True,
        )
        combined_lineage.to_csv(
            args.report
            / "00_raw_to_gold_lineage_all_v2_2.csv",
            index=False,
        )
    else:
        combined_lineage = pd.DataFrame()

    if all_reconciliation:
        combined_reconciliation = pd.concat(
            all_reconciliation,
            ignore_index=True,
        )
        combined_reconciliation.to_csv(
            args.report
            / "00_annual_reconciliation_all_v2_2.csv",
            index=False,
        )
    else:
        combined_reconciliation = pd.DataFrame()

    # --------------------------------------------------------
    # FINAL CONSOLE OUTPUT
    # --------------------------------------------------------
    log("\n" + "=" * 60)
    log("FINAL VALIDATION SUMMARY V2")
    log("=" * 60)

    log(summary_df.to_string(index=False))

    if not combined_lineage.empty:
        log("\n" + "-" * 60)
        log("RAW -> GOLD LINEAGE RESULT")
        log("-" * 60)

        matched_count = int(
            (
                combined_lineage["status"]
                == "MATCHED_RAW_CANDIDATE"
            ).sum()
        )

        no_match_count = int(
            (
                combined_lineage["status"]
                == "NO_RAW_CANDIDATE_MATCH"
            ).sum()
        )

        no_raw_count = int(
            (
                combined_lineage["status"]
                == "NO_RAW_DERIVATION"
            ).sum()
        )

        log(
            f"Matched raw candidate : "
            f"{matched_count:,}"
        )
        log(
            f"Raw candidate mismatch: "
            f"{no_match_count:,}"
        )
        log(
            f"No raw derivation     : "
            f"{no_raw_count:,}"
        )

        failures = combined_lineage[
            combined_lineage["status"]
            == "NO_RAW_CANDIDATE_MATCH"
        ]

        if not failures.empty:
            cols = [
                "cik",
                "metric",
                "fiscal_year",
                "quarter",
                "gold_value",
                "candidate_count",
                "candidate_values",
                "candidate_methods",
                "gold_source_period_type",
                "gold_assignment_method",
            ]

            log("\nTOP RAW -> GOLD MISMATCHES:")
            log(
                failures[cols]
                .head(50)
                .to_string(index=False)
            )

    if not combined_reconciliation.empty:
        log("\n" + "-" * 60)
        log("INDEPENDENT ANNUAL RECONCILIATION")
        log("-" * 60)

        material_failures = combined_reconciliation[
            combined_reconciliation["reconciliation_status"]
            == "MATERIAL_FAILURE"
        ]
        minor_differences = combined_reconciliation[
            combined_reconciliation["reconciliation_status"]
            == "MINOR_DIFFERENCE"
        ]
        passes = combined_reconciliation[
            combined_reconciliation["reconciliation_status"] == "PASS"
        ]

        log(
            f"Checks            : "
            f"{len(combined_reconciliation):,}"
        )
        log(
            f"PASS              : "
            f"{len(passes):,}"
        )
        log(
            f"MINOR DIFFERENCE  : "
            f"{len(minor_differences):,}"
        )
        log(
            f"MATERIAL FAILURE  : "
            f"{len(material_failures):,}"
        )
        log(
            f"NO ANNUAL REF     : "
            f"{int((combined_reconciliation['reconciliation_status'] == 'NO_ANNUAL_REFERENCE').sum()):,}"
        )

        if not material_failures.empty:
            log("\nTOP MATERIAL ANNUAL FAILURES:")
            log(
                material_failures[
                    [
                        "cik",
                        "metric",
                        "fiscal_year",
                        "q1",
                        "q2",
                        "q3",
                        "q4",
                        "quarter_sum",
                        "raw_annual_reference",
                        "difference",
                        "difference_pct",
                        "raw_annual_form",
                        "raw_annual_filing_date",
                        "raw_annual_accession",
                        "raw_annual_xbrl_tag",
                    ]
                ]
                .head(50)
                .to_string(index=False)
            )

        if not minor_differences.empty:
            log("\nMINOR ANNUAL DIFFERENCES:")
            log(
                minor_differences[
                    [
                        "cik",
                        "metric",
                        "fiscal_year",
                        "quarter_sum",
                        "raw_annual_reference",
                        "difference",
                        "difference_pct",
                    ]
                ]
                .head(50)
                .to_string(index=False)
            )

    log("\n" + "=" * 60)
    log("VALIDATION V2.2 COMPLETE")
    log("=" * 60)
    log(
        f"Reports written to: "
        f"{args.report}"
    )


if __name__ == "__main__":
    main()