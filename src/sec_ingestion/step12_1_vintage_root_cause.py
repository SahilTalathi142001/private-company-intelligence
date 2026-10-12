
from pathlib import Path
import pandas as pd
import numpy as np
import re


# ============================================================
# STEP 12.1 - RAW FILING VINTAGE ROOT-CAUSE DIAGNOSTIC
#
# Purpose:
#   Permanently diagnose Step 9 annual/quarterly reconciliation
#   failures without hard-coded CIKs or fiscal years.
#
# It answers:
#   1. What quarterly values did Step 6 use?
#   2. Which filing accession supplied each quarter?
#   3. What annual value did Step 5 canonicalize?
#   4. What annual facts existed in Silver across ALL filing vintages?
#   5. Which raw annual value matches the quarterly sum?
#   6. Which raw annual value matches the canonical annual?
#   7. Is the discrepancy a restatement / filing-vintage issue,
#      a quarterly derivation issue, or a true reconciliation failure?
#
# Output:
#   data\gold\lineage_validation_v2\step12_1_vintage_root_cause.xlsx
#
# Sheets:
#   Summary
#   Failures
#   Raw_Annual_Candidates
#   Quarter_Evidence
#   Vintage_Comparison
#   Coverage
# ============================================================


BASE_DIR = Path(
    r"C:\Users\stalathi\Desktop\private-company-intelligence"
)

SILVER_DIR = (
    BASE_DIR
    / "data"
    / "silver"
    / "financial_facts"
)

ANNUAL_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "financials"
)

STEP6_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "quarterly_financials"
)

REPORT_DIR = (
    BASE_DIR
    / "data"
    / "gold"
    / "lineage_validation_v2"
)

REPORT_FILE = (
    REPORT_DIR
    / "step12_1_vintage_root_cause.xlsx"
)

METRICS = [
    "revenue",
    "operating_income",
    "net_income",
]

TOLERANCE_PCT = 0.001       # 0.1%, consistent with Step 9
ANNUAL_MIN_DAYS = 300
ANNUAL_MAX_DAYS = 400


# ============================================================
# BASIC HELPERS
# ============================================================

def normalize_cik(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(10)


def normalize_accession(value):
    if pd.isna(value):
        return ""
    text = str(value).strip()
    text = text.replace("/", "-")
    return text


def normalize_metric(value):
    return str(value).strip().lower()


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return np.nan


def pct_difference(a, b):
    if pd.isna(a) or pd.isna(b):
        return np.nan
    if b == 0:
        return np.nan if a == 0 else np.inf
    return abs(a - b) / abs(b)


def values_match(a, b):
    diff = pct_difference(a, b)
    if pd.isna(diff):
        return False
    return diff <= TOLERANCE_PCT


def first_existing(df, candidates):
    for col in candidates:
        if col in df.columns:
            return col
    return None


def accession_col(df):
    return first_existing(
        df,
        [
            "source_accession",
            "accession_number",
            "accession",
        ],
    )


def filing_date_col(df):
    return first_existing(
        df,
        [
            "source_filing_date",
            "filing_date",
        ],
    )


def parse_dates(df):
    if df.empty:
        return df

    for col in [
        "period_start",
        "period_end",
        "filing_date",
        "source_filing_date",
        "annual_period_start",
        "annual_period_end",
    ]:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col],
                errors="coerce",
            )

    return df


def read_partition(root, cik):
    path = (
        root
        / f"cik={cik}"
    )

    if not path.exists():
        return pd.DataFrame()

    files = list(path.glob("*.parquet"))

    if not files:
        return pd.DataFrame()

    frames = []

    for file in files:
        try:
            frames.append(
                pd.read_parquet(file)
            )
        except Exception as exc:
            print(
                f"WARNING: failed to read {file}: {exc}"
            )

    if not frames:
        return pd.DataFrame()

    df = pd.concat(
        frames,
        ignore_index=True,
    )

    if "cik" in df.columns:
        df["cik"] = (
            df["cik"]
            .map(normalize_cik)
        )

    return parse_dates(df)


def discover_ciks():
    ciks = set()

    for root in [
        SILVER_DIR,
        ANNUAL_DIR,
        STEP6_DIR,
    ]:
        if not root.exists():
            continue

        for path in root.glob("cik=*"):
            if path.is_dir():
                cik = path.name.split(
                    "cik=",
                    1,
                )[-1]

                if cik:
                    ciks.add(
                        normalize_cik(cik)
                    )

    return sorted(ciks)


# ============================================================
# STEP 6 / STEP 5 SELECTION
# ============================================================

def get_step6_quarters(step6, metric, fiscal_year):
    if step6.empty:
        return pd.DataFrame()

    df = step6[
        step6["metric"]
        .map(normalize_metric)
        .eq(normalize_metric(metric))
    ].copy()

    if "fiscal_year" not in df.columns:
        return pd.DataFrame()

    fy = pd.to_numeric(
        df["fiscal_year"],
        errors="coerce",
    )

    df = df[fy == fiscal_year].copy()

    if "quarter" in df.columns:
        quarter_num = (
            df["quarter"]
            .astype(str)
            .str.upper()
            .str.extract(r"([1-4])", expand=False)
        )
        df["quarter_num"] = pd.to_numeric(
            quarter_num,
            errors="coerce",
        )

    if "period_end" in df.columns:
        df = df.sort_values(
            ["quarter_num", "period_end"],
            na_position="last",
        )

    return df


def quarter_sum(step6, metric, fiscal_year):
    q = get_step6_quarters(
        step6,
        metric,
        fiscal_year,
    )

    values = {}

    for qtr in ["Q1", "Q2", "Q3", "Q4"]:
        matches = q[
            q["quarter"]
            .astype(str)
            .str.upper()
            .eq(qtr)
        ]

        if matches.empty:
            values[qtr] = np.nan
        else:
            values[qtr] = safe_float(
                matches.iloc[0]["value"]
            )

    valid = [
        v for v in values.values()
        if pd.notna(v)
    ]

    total = (
        sum(valid)
        if valid
        else np.nan
    )

    return total, values, q


def get_canonical_annual(
    annual,
    metric,
    fiscal_year,
):
    if annual.empty:
        return pd.Series(dtype=object)

    df = annual[
        annual["metric"]
        .map(normalize_metric)
        .eq(normalize_metric(metric))
    ].copy()

    if "fiscal_year" in df.columns:
        fy = pd.to_numeric(
            df["fiscal_year"],
            errors="coerce",
        )
        df = df[fy == fiscal_year].copy()

    if df.empty:
        return pd.Series(dtype=object)

    # Step 5 canonical files should normally already be unique.
    # If more than one survives, prefer:
    #   1. 10-K over 10-K/A only when filing date is equal
    #   2. latest filing date
    #   3. latest period_end
    sort_cols = []

    fcol = filing_date_col(df)
    if fcol:
        sort_cols.append(fcol)

    if "period_end" in df.columns:
        sort_cols.append("period_end")

    if sort_cols:
        df = df.sort_values(
            sort_cols,
            na_position="last",
        )

    return df.iloc[-1]


# ============================================================
# RAW SILVER ANNUAL CANDIDATES
# ============================================================

def annual_like_rows(df):
    if df.empty:
        return df

    out = df.copy()

    if (
        "period_start" not in out.columns
        or "period_end" not in out.columns
    ):
        return out.iloc[0:0].copy()

    out = out[
        out["period_start"].notna()
        & out["period_end"].notna()
    ].copy()

    duration_days = (
        out["period_end"]
        - out["period_start"]
    ).dt.days

    out["duration_days"] = duration_days

    out = out[
        out["duration_days"].between(
            ANNUAL_MIN_DAYS,
            ANNUAL_MAX_DAYS,
        )
    ].copy()

    return out


def get_raw_annual_candidates(
    silver,
    metric,
    fiscal_year,
):
    if silver.empty:
        return pd.DataFrame()

    df = silver[
        silver["metric"]
        .map(normalize_metric)
        .eq(normalize_metric(metric))
    ].copy()

    # First use fiscal_year where available.
    if "fiscal_year" in df.columns:
        fy = pd.to_numeric(
            df["fiscal_year"],
            errors="coerce",
        )

        exact = df[
            fy == fiscal_year
        ].copy()

        # If SEC metadata is incomplete, fall back later
        # to period-end year / annual duration.
        if not exact.empty:
            df = exact

    df = annual_like_rows(df)

    if df.empty:
        return df

    # Prefer annual forms, but DO NOT exclude other forms.
    # Historical facts can be carried in later filings.
    if "form" in df.columns:
        form = (
            df["form"]
            .astype(str)
            .str.upper()
        )
        df["is_annual_form"] = form.isin(
            ["10-K", "10-K/A"]
        )
    else:
        df["is_annual_form"] = False

    # Remove exact duplicate raw observations only.
    df = df.drop_duplicates()

    return df


def raw_value_candidates(
    silver,
    metric,
    fiscal_year,
    quarter_sum_value,
    canonical_value,
):
    df = get_raw_annual_candidates(
        silver,
        metric,
        fiscal_year,
    )

    if df.empty:
        return df

    df = df.copy()

    df["matches_quarter_sum"] = (
        df["value"]
        .map(safe_float)
        .map(
            lambda x: values_match(
                x,
                quarter_sum_value,
            )
        )
    )

    df["matches_canonical"] = (
        df["value"]
        .map(safe_float)
        .map(
            lambda x: values_match(
                x,
                canonical_value,
            )
        )
    )

    df["difference_from_quarter_sum"] = (
        df["value"].map(safe_float)
        - quarter_sum_value
        if pd.notna(quarter_sum_value)
        else np.nan
    )

    df["difference_from_canonical"] = (
        df["value"].map(safe_float)
        - canonical_value
        if pd.notna(canonical_value)
        else np.nan
    )

    if pd.notna(quarter_sum_value):
        df["pct_from_quarter_sum"] = (
            df["difference_from_quarter_sum"]
            .abs()
            / abs(quarter_sum_value)
            if quarter_sum_value != 0
            else np.nan
        )
    else:
        df["pct_from_quarter_sum"] = np.nan

    if pd.notna(canonical_value):
        df["pct_from_canonical"] = (
            df["difference_from_canonical"]
            .abs()
            / abs(canonical_value)
            if canonical_value != 0
            else np.nan
        )
    else:
        df["pct_from_canonical"] = np.nan

    return df


# ============================================================
# VINTAGE ANALYSIS
# ============================================================

def unique_accessions(df):
    col = accession_col(df)

    if col is None or df.empty:
        return []

    return sorted(
        {
            normalize_accession(x)
            for x in df[col].dropna()
            if normalize_accession(x)
        }
    )


def find_same_accession_annual(
    raw_candidates,
    accession,
):
    if raw_candidates.empty:
        return pd.DataFrame()

    col = accession_col(
        raw_candidates
    )

    if col is None:
        return pd.DataFrame()

    target = normalize_accession(
        accession
    )

    return raw_candidates[
        raw_candidates[col]
        .map(normalize_accession)
        .eq(target)
    ].copy()


def filing_date_value(row):
    if row.empty:
        return pd.NaT

    col = filing_date_col(
        row.to_frame().T
    )

    if not col:
        return pd.NaT

    return row[col]


def accession_value(row):
    if row.empty:
        return ""

    col = accession_col(
        row.to_frame().T
    )

    if not col:
        return ""

    return normalize_accession(
        row[col]
    )


def choose_best_candidate(
    candidates,
    target_value,
):
    if candidates.empty:
        return pd.Series(dtype=object)

    df = candidates.copy()

    if pd.notna(target_value):
        df["__distance"] = (
            df["value"].map(safe_float)
            - target_value
        ).abs()
    else:
        df["__distance"] = np.inf

    fcol = filing_date_col(df)

    if fcol:
        df = df.sort_values(
            [
                "__distance",
                fcol,
                "period_end",
            ],
            ascending=[
                True,
                False,
                False,
            ],
            na_position="last",
        )
    else:
        df = df.sort_values(
            "__distance",
            na_position="last",
        )

    return df.iloc[0].drop(
        labels=["__distance"],
        errors="ignore",
    )


def classify(
    quarter_sum_value,
    canonical_value,
    raw_candidates,
    quarter_accessions,
):
    if pd.isna(quarter_sum_value):
        return (
            "QUARTER_SUM_UNAVAILABLE",
            "Step 6 does not contain all required quarters."
        )

    if pd.isna(canonical_value):
        return (
            "CANONICAL_ANNUAL_UNAVAILABLE",
            "Step 5 has no canonical annual value."
        )

    if values_match(
        quarter_sum_value,
        canonical_value,
    ):
        return (
            "RECONCILES",
            "Quarterly sum agrees with canonical annual."
        )

    if raw_candidates.empty:
        return (
            "NO_RAW_ANNUAL_EVIDENCE",
            "No annual-duration raw Silver fact was found for this metric/FY."
        )

    matches_quarter = raw_candidates[
        raw_candidates["matches_quarter_sum"]
    ]

    matches_canonical = raw_candidates[
        raw_candidates["matches_canonical"]
    ]

    same_vintage_rows = []

    for accession in quarter_accessions:
        same = find_same_accession_annual(
            raw_candidates,
            accession,
        )

        if not same.empty:
            same_vintage_rows.append(
                same
            )

    same_vintage = (
        pd.concat(
            same_vintage_rows,
            ignore_index=True,
        )
        if same_vintage_rows
        else pd.DataFrame()
    )

    same_q_match = (
        not same_vintage.empty
        and same_vintage[
            "matches_quarter_sum"
        ].any()
    )

    if same_q_match:
        return (
            "RESTATEMENT_OR_VINTAGE_CHANGE",
            "A raw annual fact from the quarterly filing vintage matches the quarterly sum, while canonical annual differs."
        )

    if not matches_quarter.empty:
        return (
            "RAW_VINTAGE_SUPPORTS_QUARTERS",
            "A raw annual fact elsewhere in the filing history matches the quarterly sum, but not necessarily the quarterly source accession."
        )

    if not matches_canonical.empty:
        return (
            "CANONICAL_SUPPORTED_BY_RAW",
            "Canonical annual is supported by a raw annual fact, but quarterly sum uses a different historical vintage."
        )

    return (
        "TRUE_RECONCILIATION_OR_DERIVATION_ISSUE",
        "No raw annual fact matches either the quarterly sum or canonical annual within tolerance."
    )


# ============================================================
# MAIN ANALYSIS
# ============================================================

def build_report():
    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    ciks = discover_ciks()

    print("=" * 80)
    print("STEP 12.1 - RAW FILING VINTAGE ROOT-CAUSE DIAGNOSTIC")
    print("=" * 80)
    print(f"Silver : {SILVER_DIR}")
    print(f"Annual : {ANNUAL_DIR}")
    print(f"Step 6 : {STEP6_DIR}")
    print(f"Output : {REPORT_FILE}")
    print()
    print(f"Companies discovered: {len(ciks)}")

    all_failures = []
    raw_evidence = []
    quarter_evidence = []
    vintage_rows = []
    coverage_rows = []

    for cik in ciks:
        print()
        print("=" * 80)
        print(f"PROCESSING {cik}")
        print("=" * 80)

        silver = read_partition(
            SILVER_DIR,
            cik,
        )

        annual = read_partition(
            ANNUAL_DIR,
            cik,
        )

        step6 = read_partition(
            STEP6_DIR,
            cik,
        )

        print(
            f"Silver rows : {len(silver):,}"
        )
        print(
            f"Annual rows : {len(annual):,}"
        )
        print(
            f"Step 6 rows: {len(step6):,}"
        )

        company_failures = 0
        company_checks = 0

        for metric in METRICS:
            if step6.empty:
                continue

            fiscal_years = pd.to_numeric(
                step6.get(
                    "fiscal_year",
                    pd.Series(dtype=float),
                ),
                errors="coerce",
            ).dropna()

            for fiscal_year in sorted(
                fiscal_years.astype(int).unique()
            ):
                qsum, qvalues, qdf = quarter_sum(
                    step6,
                    metric,
                    fiscal_year,
                )

                if qdf.empty:
                    continue

                annual_row = get_canonical_annual(
                    annual,
                    metric,
                    fiscal_year,
                )

                canonical_value = safe_float(
                    annual_row.get(
                        "value",
                        np.nan,
                    )
                    if not annual_row.empty
                    else np.nan
                )

                # Only diagnose actual Step 9-type failures.
                if (
                    pd.isna(qsum)
                    or pd.isna(canonical_value)
                ):
                    continue

                company_checks += 1

                if values_match(
                    qsum,
                    canonical_value,
                ):
                    continue

                company_failures += 1

                raw = raw_value_candidates(
                    silver,
                    metric,
                    fiscal_year,
                    qsum,
                    canonical_value,
                )

                quarter_accessions = unique_accessions(
                    qdf
                )

                root_cause, explanation = classify(
                    qsum,
                    canonical_value,
                    raw,
                    quarter_accessions,
                )

                canonical_accession = accession_value(
                    annual_row
                )

                canonical_filing_date = filing_date_value(
                    annual_row
                )

                best_quarter_match = choose_best_candidate(
                    raw[
                        raw["matches_quarter_sum"]
                    ]
                    if not raw.empty
                    else pd.DataFrame(),
                    qsum,
                )

                best_canonical_match = choose_best_candidate(
                    raw[
                        raw["matches_canonical"]
                    ]
                    if not raw.empty
                    else pd.DataFrame(),
                    canonical_value,
                )

                same_vintage_evidence = []

                for accession in quarter_accessions:
                    same = find_same_accession_annual(
                        raw,
                        accession,
                    )

                    if same.empty:
                        same_vintage_evidence.append(
                            {
                                "quarter_accession": accession,
                                "raw_annual_found": False,
                                "raw_annual_value": np.nan,
                                "raw_annual_filing_date": pd.NaT,
                                "matches_quarter_sum": False,
                                "matches_canonical": False,
                            }
                        )
                    else:
                        for _, row in same.iterrows():
                            same_vintage_evidence.append(
                                {
                                    "quarter_accession": accession,
                                    "raw_annual_found": True,
                                    "raw_annual_value": safe_float(
                                        row.get(
                                            "value",
                                            np.nan,
                                        )
                                    ),
                                    "raw_annual_filing_date": (
                                        filing_date_value(row)
                                    ),
                                    "matches_quarter_sum": bool(
                                        row.get(
                                            "matches_quarter_sum",
                                            False,
                                        )
                                    ),
                                    "matches_canonical": bool(
                                        row.get(
                                            "matches_canonical",
                                            False,
                                        )
                                    ),
                                }
                            )

                # ------------------------------------------------
                # FAILURE SUMMARY
                # ------------------------------------------------

                failure = {
                    "cik": cik,
                    "metric": metric,
                    "fiscal_year": fiscal_year,
                    "q1": qvalues["Q1"],
                    "q2": qvalues["Q2"],
                    "q3": qvalues["Q3"],
                    "q4": qvalues["Q4"],
                    "quarter_sum": qsum,
                    "canonical_annual_value": canonical_value,
                    "difference": qsum - canonical_value,
                    "difference_pct": pct_difference(
                        qsum,
                        canonical_value,
                    ),
                    "quarter_accessions": "; ".join(
                        quarter_accessions
                    ),
                    "quarter_vintage_count": len(
                        quarter_accessions
                    ),
                    "canonical_accession": (
                        canonical_accession
                    ),
                    "canonical_filing_date": (
                        canonical_filing_date
                    ),
                    "raw_annual_candidates": len(
                        raw
                    ),
                    "raw_matches_quarter_sum": int(
                        raw["matches_quarter_sum"].sum()
                    )
                    if not raw.empty
                    else 0,
                    "raw_matches_canonical": int(
                        raw["matches_canonical"].sum()
                    )
                    if not raw.empty
                    else 0,
                    "same_vintage_raw_matches_quarter_sum": int(
                        sum(
                            x["matches_quarter_sum"]
                            for x in same_vintage_evidence
                        )
                    ),
                    "same_vintage_raw_found": int(
                        sum(
                            x["raw_annual_found"]
                            for x in same_vintage_evidence
                        )
                    ),
                    "root_cause": root_cause,
                    "explanation": explanation,
                }

                if not best_quarter_match.empty:
                    failure.update(
                        {
                            "best_quarter_sum_match_value":
                                safe_float(
                                    best_quarter_match.get(
                                        "value",
                                        np.nan,
                                    )
                                ),
                            "best_quarter_sum_match_accession":
                                accession_value(
                                    best_quarter_match
                                ),
                            "best_quarter_sum_match_filing_date":
                                filing_date_value(
                                    best_quarter_match
                                ),
                        }
                    )
                else:
                    failure.update(
                        {
                            "best_quarter_sum_match_value":
                                np.nan,
                            "best_quarter_sum_match_accession":
                                "",
                            "best_quarter_sum_match_filing_date":
                                pd.NaT,
                        }
                    )

                if not best_canonical_match.empty:
                    failure.update(
                        {
                            "best_canonical_match_value":
                                safe_float(
                                    best_canonical_match.get(
                                        "value",
                                        np.nan,
                                    )
                                ),
                            "best_canonical_match_accession":
                                accession_value(
                                    best_canonical_match
                                ),
                            "best_canonical_match_filing_date":
                                filing_date_value(
                                    best_canonical_match
                                ),
                        }
                    )
                else:
                    failure.update(
                        {
                            "best_canonical_match_value":
                                np.nan,
                            "best_canonical_match_accession":
                                "",
                            "best_canonical_match_filing_date":
                                pd.NaT,
                        }
                    )

                all_failures.append(
                    failure
                )

                # ------------------------------------------------
                # QUARTER EVIDENCE
                # ------------------------------------------------

                for _, row in qdf.iterrows():
                    out = row.to_dict()

                    out.update(
                        {
                            "diagnostic_cik": cik,
                            "diagnostic_metric": metric,
                            "diagnostic_fiscal_year":
                                fiscal_year,
                        }
                    )

                    quarter_evidence.append(
                        out
                    )

                # ------------------------------------------------
                # RAW ANNUAL EVIDENCE
                # ------------------------------------------------

                if not raw.empty:
                    for _, row in raw.iterrows():
                        out = row.to_dict()

                        out.update(
                            {
                                "diagnostic_cik": cik,
                                "diagnostic_metric": metric,
                                "diagnostic_fiscal_year":
                                    fiscal_year,
                            }
                        )

                        # Explicitly flag whether this accession
                        # supplied one of the quarterly values.
                        acc = accession_value(row)

                        out[
                            "is_quarter_vintage"
                        ] = (
                            acc in quarter_accessions
                        )

                        out[
                            "is_canonical_accession"
                        ] = (
                            acc
                            == canonical_accession
                        )

                        raw_evidence.append(
                            out
                        )

                # ------------------------------------------------
                # VINTAGE COMPARISON
                # ------------------------------------------------

                for item in same_vintage_evidence:
                    vintage_rows.append(
                        {
                            "cik": cik,
                            "metric": metric,
                            "fiscal_year": fiscal_year,
                            **item,
                            "quarter_sum": qsum,
                            "canonical_annual_value":
                                canonical_value,
                            "canonical_accession":
                                canonical_accession,
                            "root_cause":
                                root_cause,
                        }
                    )

        coverage_rows.append(
            {
                "cik": cik,
                "silver_rows": len(silver),
                "step6_rows": len(step6),
                "annual_rows": len(annual),
                "reconciliation_checks": company_checks,
                "reconciliation_failures": company_failures,
            }
        )

        print(
            f"Reconciliation checks : {company_checks}"
        )
        print(
            f"Reconciliation failures: {company_failures}"
        )

    failures_df = pd.DataFrame(
        all_failures
    )

    raw_df = pd.DataFrame(
        raw_evidence
    )

    quarter_df = pd.DataFrame(
        quarter_evidence
    )

    vintage_df = pd.DataFrame(
        vintage_rows
    )

    coverage_df = pd.DataFrame(
        coverage_rows
    )

    # ========================================================
    # SORTING
    # ========================================================

    for df in [
        failures_df,
        raw_df,
        quarter_df,
        vintage_df,
        coverage_df,
    ]:
        if df.empty:
            continue

        if "cik" in df.columns:
            df["cik"] = (
                df["cik"]
                .map(normalize_cik)
            )

        if "fiscal_year" in df.columns:
            df["fiscal_year"] = pd.to_numeric(
                df["fiscal_year"],
                errors="coerce",
            )

    if not failures_df.empty:
        failures_df = failures_df.sort_values(
            [
                "cik",
                "fiscal_year",
                "metric",
            ]
        )

    if not raw_df.empty:
        raw_df = raw_df.sort_values(
            [
                "cik",
                "fiscal_year",
                "metric",
                "filing_date"
                if "filing_date" in raw_df.columns
                else "value",
            ],
            na_position="last",
        )

    if not vintage_df.empty:
        vintage_df = vintage_df.sort_values(
            [
                "cik",
                "fiscal_year",
                "metric",
                "quarter_accession",
            ]
        )

    # ========================================================
    # WRITE EXCEL
    # ========================================================

    print()
    print("=" * 80)
    print("WRITING ROOT-CAUSE EXCEL REPORT")
    print("=" * 80)

    with pd.ExcelWriter(
        REPORT_FILE,
        engine="openpyxl",
        datetime_format="yyyy-mm-dd",
    ) as writer:

        coverage_df.to_excel(
            writer,
            sheet_name="Coverage",
            index=False,
        )

        if failures_df.empty:
            pd.DataFrame(
                {
                    "status": [
                        "No reconciliation failures detected."
                    ]
                }
            ).to_excel(
                writer,
                sheet_name="Summary",
                index=False,
            )
        else:
            # Compact executive summary first.
            summary = (
                failures_df[
                    [
                        "cik",
                        "metric",
                        "fiscal_year",
                        "quarter_sum",
                        "canonical_annual_value",
                        "difference",
                        "difference_pct",
                        "quarter_accessions",
                        "canonical_accession",
                        "canonical_filing_date",
                        "raw_annual_candidates",
                        "raw_matches_quarter_sum",
                        "raw_matches_canonical",
                        "same_vintage_raw_found",
                        "same_vintage_raw_matches_quarter_sum",
                        "root_cause",
                        "explanation",
                    ]
                ].copy()
            )

            summary.to_excel(
                writer,
                sheet_name="Summary",
                index=False,
            )

            failures_df.to_excel(
                writer,
                sheet_name="Failures",
                index=False,
            )

        if raw_df.empty:
            pd.DataFrame(
                {
                    "status": [
                        "No raw annual evidence generated."
                    ]
                }
            ).to_excel(
                writer,
                sheet_name="Raw_Annual_Candidates",
                index=False,
            )
        else:
            raw_df.to_excel(
                writer,
                sheet_name="Raw_Annual_Candidates",
                index=False,
            )

        if quarter_df.empty:
            pd.DataFrame(
                {
                    "status": [
                        "No quarter evidence generated."
                    ]
                }
            ).to_excel(
                writer,
                sheet_name="Quarter_Evidence",
                index=False,
            )
        else:
            quarter_df.to_excel(
                writer,
                sheet_name="Quarter_Evidence",
                index=False,
            )

        if vintage_df.empty:
            pd.DataFrame(
                {
                    "status": [
                        "No same-vintage evidence generated."
                    ]
                }
            ).to_excel(
                writer,
                sheet_name="Vintage_Comparison",
                index=False,
            )
        else:
            vintage_df.to_excel(
                writer,
                sheet_name="Vintage_Comparison",
                index=False,
            )

    # ========================================================
    # CONSOLE SUMMARY
    # ========================================================

    print()
    print("=" * 80)
    print("STEP 12.1 COMPLETE")
    print("=" * 80)

    print(
        f"Companies checked       : {len(ciks)}"
    )
    print(
        f"Reconciliation failures : {len(failures_df):,}"
    )

    if not failures_df.empty:
        print()
        print(
            failures_df[
                [
                    "cik",
                    "metric",
                    "fiscal_year",
                    "root_cause",
                ]
            ].to_string(
                index=False
            )
        )

    print()
    print(
        f"Excel written: {REPORT_FILE}"
    )


if __name__ == "__main__":
    try:
        build_report()

    except Exception as exc:
        print()
        print("=" * 80)
        print("FATAL ERROR")
        print("=" * 80)
        print(
            f"{type(exc).__name__}: {exc}"
        )
        raise
