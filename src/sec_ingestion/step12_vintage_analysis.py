from pathlib import Path
import pandas as pd
import numpy as np


# ============================================================
# STEP 12 - REPORTING VINTAGE ANALYSIS
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

CANONICAL_DIR = (
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
    / "step12_vintage_analysis.xlsx"
)


# ============================================================
# TARGETS
# ============================================================

TARGETS = [
    {
        "cik": "0000789019",
        "fiscal_year": 2016,
        "metrics": ["revenue", "operating_income", "net_income"],
    },
    {
        "cik": "0001018724",
        "fiscal_year": 2011,
        "metrics": ["revenue", "net_income"],
    },
    {
        "cik": "0001018724",
        "fiscal_year": 2012,
        "metrics": ["net_income"],
    },
]


# ============================================================
# HELPERS
# ============================================================

def normalize_cik(value):
    return str(value).strip().zfill(10)


def normalize_accession(value):
    if pd.isna(value):
        return ""

    return (
        str(value)
        .strip()
        .replace("/", "-")
    )


def load_cik_data(root: Path, cik: str):
    if not root.exists():
        return pd.DataFrame()

    cik = normalize_cik(cik)

    files = [
        f for f in root.rglob("*.parquet")
        if f"cik={cik}" in str(f)
    ]

    if not files:
        return pd.DataFrame()

    frames = []

    for file in files:
        try:
            df = pd.read_parquet(file)
            frames.append(df)
        except Exception as exc:
            print(f"WARNING: failed to read {file}: {exc}")

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    if "cik" in df.columns:
        df["cik"] = (
            df["cik"]
            .astype(str)
            .str.replace(".0", "", regex=False)
            .str.zfill(10)
        )

    return df


def parse_dates(df):
    date_columns = [
        "period_start",
        "period_end",
        "filing_date",
        "annual_period_start",
        "annual_period_end",
        "source_filing_date",
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col],
                errors="coerce",
            )

    return df


def get_accession_column(df):
    for col in [
        "source_accession",
        "accession_number",
        "accession",
    ]:
        if col in df.columns:
            return col

    return None


def get_filing_date_column(df):
    for col in [
        "source_filing_date",
        "filing_date",
    ]:
        if col in df.columns:
            return col

    return None


def filter_metric(df, metric):
    if df.empty or "metric" not in df.columns:
        return df.iloc[0:0].copy()

    return df[
        df["metric"]
        .astype(str)
        .str.lower()
        .eq(metric.lower())
    ].copy()


def filter_fy(df, fiscal_year):
    if df.empty or "fiscal_year" not in df.columns:
        return df.iloc[0:0].copy()

    fy = pd.to_numeric(
        df["fiscal_year"],
        errors="coerce",
    )

    return df[fy == fiscal_year].copy()


def accession_series(df):
    col = get_accession_column(df)

    if col is None:
        return pd.Series("", index=df.index)

    return df[col].map(normalize_accession)


def find_annual_candidates(canonical, metric, fiscal_year):
    df = filter_metric(canonical, metric)
    df = filter_fy(df, fiscal_year)

    if df.empty:
        return df

    # Annual observations should represent duration facts.
    if "period_start" in df.columns and "period_end" in df.columns:
        df = df[
            df["period_start"].notna()
            & df["period_end"].notna()
        ].copy()

    return df.drop_duplicates()


def find_step6_target(step6, metric, fiscal_year):
    df = filter_metric(step6, metric)
    df = filter_fy(df, fiscal_year)

    if df.empty:
        return df

    if "quarter" in df.columns:
        order = {
            "Q1": 1,
            "Q2": 2,
            "Q3": 3,
            "Q4": 4,
        }

        df["__quarter_num"] = (
            df["quarter"]
            .astype(str)
            .str.upper()
            .map(order)
        )

        df = df.sort_values(
            ["__quarter_num", "period_end"],
            na_position="last",
        )

    return df


def identify_quarter_vintages(step6_rows):
    accession_col = get_accession_column(step6_rows)

    if accession_col is None:
        return []

    return sorted(
        {
            normalize_accession(x)
            for x in step6_rows[accession_col].dropna()
            if normalize_accession(x)
        }
    )


def choose_canonical_annual(candidates):
    if candidates.empty:
        return pd.Series(dtype=object)

    # Step 5's canonical output is assumed to have already selected
    # the authoritative annual observation. If multiple rows remain,
    # prefer the latest filing date, then latest period_end.
    sort_cols = [
        c for c in [
            get_filing_date_column(candidates),
            "period_end",
        ]
        if c is not None and c in candidates.columns
    ]

    if sort_cols:
        candidates = candidates.sort_values(
            sort_cols,
            na_position="last",
        )

    return candidates.iloc[-1]


def find_same_vintage_raw_annual(
    silver,
    metric,
    fiscal_year,
    quarter_accessions,
):
    df = filter_metric(silver, metric)
    df = filter_fy(df, fiscal_year)

    if df.empty:
        return df

    accession_col = get_accession_column(df)

    if accession_col is None:
        return df.iloc[0:0].copy()

    accession_values = {
        normalize_accession(x)
        for x in quarter_accessions
        if normalize_accession(x)
    }

    if not accession_values:
        return df.iloc[0:0].copy()

    df["__accession_normalized"] = (
        df[accession_col].map(normalize_accession)
    )

    df = df[
        df["__accession_normalized"].isin(
            accession_values
        )
    ].copy()

    # Keep annual-duration observations only.
    if "period_start" in df.columns and "period_end" in df.columns:
        df = df[
            df["period_start"].notna()
            & df["period_end"].notna()
        ].copy()

    # Prefer observations whose period spans roughly the fiscal year.
    if (
        "period_start" in df.columns
        and "period_end" in df.columns
    ):
        duration_days = (
            df["period_end"] - df["period_start"]
        ).dt.days

        df["__duration_days"] = duration_days

        # Annual facts generally have a duration close to a year.
        # Use a broad range because fiscal calendars differ.
        annual_like = df[
            df["__duration_days"].between(
                300,
                400,
            )
        ].copy()

        if not annual_like.empty:
            df = annual_like

    return df.drop(
        columns=[
            "__accession_normalized",
            "__duration_days",
        ],
        errors="ignore",
    ).drop_duplicates()


def choose_same_vintage_annual(df):
    if df.empty:
        return pd.Series(dtype=object)

    sort_cols = [
        c for c in [
            get_filing_date_column(df),
            "period_end",
        ]
        if c is not None and c in df.columns
    ]

    if sort_cols:
        df = df.sort_values(
            sort_cols,
            na_position="last",
        )

    return df.iloc[-1]


def safe_value(row, column):
    if row is None or row.empty or column not in row.index:
        return np.nan

    return row[column]


def classify_root_cause(
    quarter_sum,
    canonical_value,
    same_vintage_value,
    same_vintage_available,
    vintage_match,
):
    if pd.isna(quarter_sum):
        return "NO_QUARTER_SUM"

    if pd.isna(canonical_value):
        return "NO_CANONICAL_ANNUAL"

    canonical_diff = quarter_sum - canonical_value

    if abs(canonical_diff) <= max(
        1.0,
        abs(canonical_value) * 0.001,
    ):
        return "SAME_VALUE_RECONCILES"

    if vintage_match:
        return "SAME_VINTAGE_RECONCILIATION_FAILURE"

    if same_vintage_available:
        if pd.notna(same_vintage_value):
            same_diff = quarter_sum - same_vintage_value

            if abs(same_diff) <= max(
                1.0,
                abs(same_vintage_value) * 0.001,
            ):
                return "DIFFERENT_FILING_VINTAGE"

            return "SAME_VINTAGE_RAW_ALSO_DIFFERS"

    return "DIFFERENT_FILING_VINTAGE_NO_RAW_ANNUAL"


# ============================================================
# BUILD ANALYSIS
# ============================================================

def build_vintage_analysis():
    summary_rows = []
    quarterly_rows = []
    annual_rows = []
    raw_rows = []

    ciks = sorted(
        {
            normalize_cik(t["cik"])
            for t in TARGETS
        }
    )

    silver_cache = {}
    canonical_cache = {}
    step6_cache = {}

    print()
    print("=" * 80)
    print("LOADING STEP 12 DATA")
    print("=" * 80)

    for cik in ciks:
        print(f"Loading {cik}")

        silver_cache[cik] = parse_dates(
            load_cik_data(
                SILVER_DIR,
                cik,
            )
        )

        canonical_cache[cik] = parse_dates(
            load_cik_data(
                CANONICAL_DIR,
                cik,
            )
        )

        step6_path = (
            STEP6_DIR
            / f"cik={cik}"
            / "quarterly_financials.parquet"
        )

        if step6_path.exists():
            step6_cache[cik] = parse_dates(
                pd.read_parquet(step6_path)
            )
        else:
            step6_cache[cik] = pd.DataFrame()

        print(
            f"  Silver    : {len(silver_cache[cik]):,}"
        )
        print(
            f"  Canonical : {len(canonical_cache[cik]):,}"
        )
        print(
            f"  Step 6    : {len(step6_cache[cik]):,}"
        )

    for target in TARGETS:
        cik = normalize_cik(target["cik"])
        fiscal_year = target["fiscal_year"]

        silver = silver_cache[cik]
        canonical = canonical_cache[cik]
        step6 = step6_cache[cik]

        for metric in target["metrics"]:
            print()
            print(
                f"Analyzing {cik} | "
                f"{metric} | FY {fiscal_year}"
            )

            q = find_step6_target(
                step6,
                metric,
                fiscal_year,
            )

            annual_candidates = find_annual_candidates(
                canonical,
                metric,
                fiscal_year,
            )

            annual = choose_canonical_annual(
                annual_candidates
            )

            quarter_values = {}

            for quarter in ["Q1", "Q2", "Q3", "Q4"]:
                if (
                    not q.empty
                    and "quarter" in q.columns
                    and "value" in q.columns
                ):
                    matches = q[
                        q["quarter"]
                        .astype(str)
                        .str.upper()
                        .eq(quarter)
                    ]

                    if not matches.empty:
                        quarter_values[quarter] = (
                            matches.iloc[0]["value"]
                        )
                    else:
                        quarter_values[quarter] = np.nan
                else:
                    quarter_values[quarter] = np.nan

            valid_values = [
                quarter_values[qtr]
                for qtr in ["Q1", "Q2", "Q3", "Q4"]
                if pd.notna(quarter_values[qtr])
            ]

            quarter_sum = (
                sum(valid_values)
                if valid_values
                else np.nan
            )

            quarter_accessions = identify_quarter_vintages(q)

            annual_accession_col = get_accession_column(
                annual.to_frame().T
            )

            annual_accession = (
                normalize_accession(
                    annual[annual_accession_col]
                )
                if (
                    not annual.empty
                    and annual_accession_col is not None
                )
                else ""
            )

            annual_filing_col = get_filing_date_column(
                annual.to_frame().T
            )

            annual_filing_date = (
                annual[annual_filing_col]
                if (
                    not annual.empty
                    and annual_filing_col is not None
                )
                else pd.NaT
            )

            vintage_match = (
                bool(
                    annual_accession
                    and len(quarter_accessions) == 1
                    and annual_accession
                    == quarter_accessions[0]
                )
            )

            same_vintage_raw = (
                find_same_vintage_raw_annual(
                    silver,
                    metric,
                    fiscal_year,
                    quarter_accessions,
                )
            )

            same_vintage_available = (
                not same_vintage_raw.empty
            )

            same_vintage_annual = (
                choose_same_vintage_annual(
                    same_vintage_raw
                )
            )

            same_vintage_value = safe_value(
                same_vintage_annual,
                "value",
            )

            canonical_value = safe_value(
                annual,
                "value",
            )

            difference = (
                quarter_sum - canonical_value
                if pd.notna(quarter_sum)
                and pd.notna(canonical_value)
                else np.nan
            )

            difference_pct = (
                abs(difference)
                / abs(canonical_value)
                if pd.notna(difference)
                and canonical_value != 0
                else np.nan
            )

            same_vintage_difference = (
                quarter_sum - same_vintage_value
                if pd.notna(quarter_sum)
                and pd.notna(same_vintage_value)
                else np.nan
            )

            same_vintage_difference_pct = (
                abs(same_vintage_difference)
                / abs(same_vintage_value)
                if pd.notna(same_vintage_difference)
                and same_vintage_value != 0
                else np.nan
            )

            root_cause = classify_root_cause(
                quarter_sum,
                canonical_value,
                same_vintage_value,
                same_vintage_available,
                vintage_match,
            )

            summary_rows.append(
                {
                    "cik": cik,
                    "metric": metric,
                    "fiscal_year": fiscal_year,
                    "q1": quarter_values["Q1"],
                    "q2": quarter_values["Q2"],
                    "q3": quarter_values["Q3"],
                    "q4": quarter_values["Q4"],
                    "quarter_sum": quarter_sum,
                    "canonical_annual_value": canonical_value,
                    "difference": difference,
                    "difference_pct": difference_pct,
                    "quarter_accessions": "; ".join(
                        quarter_accessions
                    ),
                    "quarter_vintage_count": len(
                        quarter_accessions
                    ),
                    "annual_accession": annual_accession,
                    "annual_filing_date": annual_filing_date,
                    "vintage_match": vintage_match,
                    "same_vintage_raw_available": (
                        same_vintage_available
                    ),
                    "same_vintage_annual_value": (
                        same_vintage_value
                    ),
                    "same_vintage_difference": (
                        same_vintage_difference
                    ),
                    "same_vintage_difference_pct": (
                        same_vintage_difference_pct
                    ),
                    "root_cause": root_cause,
                }
            )

            # ----------------------------------------------------
            # Quarterly evidence
            # ----------------------------------------------------

            if not q.empty:
                q_out = q.copy()

                q_out.insert(
                    0,
                    "diagnostic_fiscal_year",
                    fiscal_year,
                )
                q_out.insert(
                    0,
                    "diagnostic_metric",
                    metric,
                )
                q_out.insert(
                    0,
                    "diagnostic_cik",
                    cik,
                )

                quarterly_rows.append(q_out)

            # ----------------------------------------------------
            # Annual evidence
            # ----------------------------------------------------

            if not annual_candidates.empty:
                a_out = annual_candidates.copy()

                a_out.insert(
                    0,
                    "selected_canonical",
                    False,
                )

                if not annual.empty:
                    annual_acc = normalize_accession(
                        safe_value(
                            annual,
                            get_accession_column(
                                annual.to_frame().T
                            )
                            if get_accession_column(
                                annual.to_frame().T
                            )
                            else "",
                        )
                    )

                    a_acc_col = get_accession_column(
                        a_out
                    )

                    if a_acc_col:
                        a_out["selected_canonical"] = (
                            a_out[a_acc_col]
                            .map(normalize_accession)
                            .eq(annual_acc)
                        )

                a_out.insert(
                    0,
                    "diagnostic_fiscal_year",
                    fiscal_year,
                )
                a_out.insert(
                    0,
                    "diagnostic_metric",
                    metric,
                )
                a_out.insert(
                    0,
                    "diagnostic_cik",
                    cik,
                )

                annual_rows.append(a_out)

            # ----------------------------------------------------
            # Same-vintage raw evidence
            # ----------------------------------------------------

            if not same_vintage_raw.empty:
                r_out = same_vintage_raw.copy()

                r_out.insert(
                    0,
                    "diagnostic_fiscal_year",
                    fiscal_year,
                )
                r_out.insert(
                    0,
                    "diagnostic_metric",
                    metric,
                )
                r_out.insert(
                    0,
                    "diagnostic_cik",
                    cik,
                )

                raw_rows.append(r_out)

    summary_df = pd.DataFrame(summary_rows)

    quarterly_df = (
        pd.concat(
            quarterly_rows,
            ignore_index=True,
        )
        if quarterly_rows
        else pd.DataFrame()
    )

    annual_df = (
        pd.concat(
            annual_rows,
            ignore_index=True,
        )
        if annual_rows
        else pd.DataFrame()
    )

    raw_df = (
        pd.concat(
            raw_rows,
            ignore_index=True,
        )
        if raw_rows
        else pd.DataFrame()
    )

    return (
        summary_df,
        quarterly_df,
        annual_df,
        raw_df,
    )


# ============================================================
# EXCEL WRITER
# ============================================================

def write_excel(
    summary_df,
    quarterly_df,
    annual_df,
    raw_df,
):
    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print()
    print("=" * 80)
    print("WRITING STEP 12 EXCEL REPORT")
    print("=" * 80)

    print(f"Output: {REPORT_FILE}")

    with pd.ExcelWriter(
        REPORT_FILE,
        engine="openpyxl",
        datetime_format="yyyy-mm-dd",
    ) as writer:

        summary_df.to_excel(
            writer,
            sheet_name="Summary",
            index=False,
        )

        if summary_df.empty:
            pd.DataFrame(
                {
                    "message": [
                        "No vintage analysis rows generated."
                    ]
                }
            ).to_excel(
                writer,
                sheet_name="Vintage_Analysis",
                index=False,
            )
        else:
            summary_df.to_excel(
                writer,
                sheet_name="Vintage_Analysis",
                index=False,
            )

        if quarterly_df.empty:
            pd.DataFrame(
                {
                    "message": [
                        "No quarterly evidence found."
                    ]
                }
            ).to_excel(
                writer,
                sheet_name="Quarterly_Evidence",
                index=False,
            )
        else:
            quarterly_df.to_excel(
                writer,
                sheet_name="Quarterly_Evidence",
                index=False,
            )

        if annual_df.empty:
            pd.DataFrame(
                {
                    "message": [
                        "No annual evidence found."
                    ]
                }
            ).to_excel(
                writer,
                sheet_name="Annual_Evidence",
                index=False,
            )
        else:
            annual_df.to_excel(
                writer,
                sheet_name="Annual_Evidence",
                index=False,
            )

        if raw_df.empty:
            pd.DataFrame(
                {
                    "message": [
                        "No same-vintage raw annual observations found."
                    ]
                }
            ).to_excel(
                writer,
                sheet_name="Raw_Same_Vintage",
                index=False,
            )
        else:
            raw_df.to_excel(
                writer,
                sheet_name="Raw_Same_Vintage",
                index=False,
            )

        workbook = writer.book

        for worksheet in workbook.worksheets:
            worksheet.freeze_panes = "A2"

            if worksheet.max_row > 1:
                worksheet.auto_filter.ref = (
                    worksheet.dimensions
                )

            for column_cells in worksheet.columns:
                max_length = 0
                column_letter = (
                    column_cells[0].column_letter
                )

                for cell in column_cells:
                    try:
                        max_length = max(
                            max_length,
                            len(str(cell.value)),
                        )
                    except Exception:
                        pass

                worksheet.column_dimensions[
                    column_letter
                ].width = min(
                    max(max_length + 2, 10),
                    50,
                )

    print("Excel report successfully written.")


# ============================================================
# TERMINAL SUMMARY
# ============================================================

def print_summary(summary_df):
    print()
    print("=" * 80)
    print("STEP 12 - VINTAGE ANALYSIS SUMMARY")
    print("=" * 80)

    if summary_df.empty:
        print("No analysis rows generated.")
        return

    cols = [
        "cik",
        "metric",
        "fiscal_year",
        "quarter_sum",
        "canonical_annual_value",
        "difference_pct",
        "quarter_accessions",
        "annual_accession",
        "vintage_match",
        "same_vintage_raw_available",
        "same_vintage_annual_value",
        "same_vintage_difference_pct",
        "root_cause",
    ]

    cols = [
        c for c in cols
        if c in summary_df.columns
    ]

    print(
        summary_df[cols]
        .to_string(index=False)
    )

    print()
    print("ROOT CAUSE COUNTS")
    print("-" * 80)

    print(
        summary_df["root_cause"]
        .value_counts(dropna=False)
        .to_string()
    )


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 80)
    print("STEP 12 - REPORTING VINTAGE ANALYSIS")
    print("=" * 80)

    print()
    print(f"Base directory : {BASE_DIR}")
    print(f"Silver         : {SILVER_DIR}")
    print(f"Canonical      : {CANONICAL_DIR}")
    print(f"Step 6         : {STEP6_DIR}")
    print(f"Excel report   : {REPORT_FILE}")

    if not SILVER_DIR.exists():
        raise FileNotFoundError(
            f"Silver directory not found:\n{SILVER_DIR}"
        )

    if not CANONICAL_DIR.exists():
        raise FileNotFoundError(
            f"Canonical directory not found:\n{CANONICAL_DIR}"
        )

    if not STEP6_DIR.exists():
        raise FileNotFoundError(
            f"Step 6 directory not found:\n{STEP6_DIR}"
        )

    (
        summary_df,
        quarterly_df,
        annual_df,
        raw_df,
    ) = build_vintage_analysis()

    print_summary(summary_df)

    write_excel(
        summary_df,
        quarterly_df,
        annual_df,
        raw_df,
    )

    print()
    print("=" * 80)
    print("STEP 12 COMPLETE")
    print("=" * 80)

    print()
    print("Excel report:")
    print(REPORT_FILE)


if __name__ == "__main__":
    main()
