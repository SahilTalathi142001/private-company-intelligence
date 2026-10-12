import sys
from pathlib import Path
import pandas as pd

if __package__ in (None, ""):
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))


from src.sec_ingestion.client import SECAPIClient
from src.sec_ingestion.quarterly_data_quality import (
    validate_quarterly_data,
)

from src.sec_ingestion.company import (
    ingest_company_master,
)

from src.sec_ingestion.submissions import (
    ingest_submissions,
)

from src.sec_ingestion.build_financial_silver import (
    BASE_DIR,
    process_all,
)
from src.sec_ingestion.canonicalize_financials import (
    canonicalize_company,
)
from src.sec_ingestion.config import (
    SILVER_DIR,
    DATA_DIR,
)
from src.sec_ingestion.derive_quarters import derive_quarters

from src.sec_ingestion.gold_financial_model import (
    build_quarterly_gold,
)

from src.sec_ingestion.gold_annual_financials import (
    build_annual_gold,
)

from src.sec_ingestion.build_metric_dimension import (
    build_metric_dimension,
)
from src.sec_ingestion.config import (
    BASE_DIR,
    DATA_DIR,
)

from src.sec_ingestion.quarterly_reconciliation import (
    OUTPUT_DIR,
    build_annual_reconciliation,
    build_incomplete_years,
    build_quarter_completeness,
    build_summary,
    ensure_output_dirs,
    find_quarter_duplicates,
    load_annual,
    load_quarterly,
)

from src.sec_ingestion.quarterly_data_quality import (
    validate_quarterly_data,
)

from src.sec_ingestion.gold_loader import (
    load_single_parquet,
)

from src.sec_ingestion.final_quarterly_gold import (
    process_company,
)

from src.sec_ingestion.financial_analytics import (
    process_company as process_financial_analytics,
)

COMPANIES = [
    "0000320193",  # Apple
    "0000789019",  # Microsoft
    "0001018724",  # Amazon
    "0001652044",  # Alphabet
    "0001045810",  # NVIDIA
]


def main():

    print("=" * 70)
    print("PRIVATE COMPANY INTELLIGENCE PLATFORM")
    print("SEC DATA PIPELINE")
    print("=" * 70)

    client = SECAPIClient()

    # --------------------------------------------------
    # STEP 1
    # Company Master
    # --------------------------------------------------

    # print("\nSTEP 1 - COMPANY MASTER")

    # company_df = ingest_company_master(
    #     client
    # )

    # print(
    #     f"Companies available: "
    #     f"{len(company_df):,}"
    # )

    # # --------------------------------------------------
    # # STEP 2
    # # Submissions
    # # --------------------------------------------------

    # print("\nSTEP 2 - SUBMISSIONS")

    # for cik in COMPANIES:

    #     try:

    #         df = ingest_submissions(
    #             client,
    #             cik,
    #         )

    #         print(
    #             f"{cik}: "
    #             f"{len(df):,} filings"
    #         )

    #     except Exception as e:

    #         print(
    #             f"{cik}: ERROR - {e}"
    #         )

    # # --------------------------------------------------
    # # STEP 3
    # # Financial Facts
    # # --------------------------------------------------

    # print("\nSTEP 3 - COMPANYFACTS")

    # from src.sec_ingestion.facts import (
    #     ingest_company_facts,
    # )

    # for cik in COMPANIES:

    #     try:

    #         df = ingest_company_facts(
    #             client,
    #             cik,
    #         )

    #         print(
    #             f"{cik}: "
    #             f"{len(df):,} raw facts"
    #         )

    #     except Exception as e:

    #         print(
    #             f"{cik}: ERROR - {e}"
    #         )

    # # --------------------------------------------------
    # # STEP 4
    # # Normalize → Silver
    # # --------------------------------------------------

    # print("\nSTEP 4 - FINANCIAL NORMALIZATION")

    # process_all(
    #     COMPANIES
    # )

    # print("\n")
    # print("=" * 70)
    # print("PIPELINE COMPLETED")
    # print("=" * 70)


#     print("\n")
#     print("=" * 60)
#     print("STEP 5 - FINANCIAL CANONICALIZATION")
#     print("=" * 60)

#     GOLD_DIR = DATA_DIR / "gold"

#     for cik in COMPANIES:

#         print(f"\nProcessing {cik}")

#         try:

#             financials_df = canonicalize_company(
#                 silver_root=SILVER_DIR,
#                 gold_root=GOLD_DIR,
#                 cik=cik,
#             )

#             print(
#                 f"Canonical financial rows: "
#                 f"{len(financials_df):,}"
#             )

#         except Exception as e:

#             print(
#                 f"ERROR - canonicalization failed "
#                 f"for {cik}: {e}"
#             )

#     print("\n")
#     print("=" * 60)
#     print("STEP 6 - QUARTERLY FINANCIAL DERIVATION")
#     print("=" * 60)

#     quarterly_results = []

#     for cik in COMPANIES:

#             print(f"\nProcessing quarterly financials: {cik}")

#             try:

#         # Load Silver financial facts
#                 silver_path = (
#                 SILVER_DIR
#                 / "financial_facts"
#                 / f"cik={str(cik).zfill(10)}"
#                 / "financial_facts.parquet"
#             )

#                 df = pd.read_parquet(silver_path)

#         # Derive standalone quarters
#                 quarterly_df = derive_quarters(df)
#                 quarterly_results.append(quarterly_df)

#                 print(
#             f"Quarterly rows generated: "
#             f"{len(quarterly_df):,}"
#                 )

#                 if not quarterly_df.empty:

#                     print("\nQuarter distribution:")
#                     print(
#                     quarterly_df["quarter"]
#                     .value_counts()
#                     .sort_index()
#                     )

#                     print("\nSample:")
#                     print(
#                     quarterly_df.head(10)
#                     .to_string(index=False)
#                     )

#             # Write Gold quarterly data
#                     gold_path = (
#                         DATA_DIR
#                         / "gold"
#                         / "quarterly_financials"
#                         / f"cik={str(cik).zfill(10)}"
#                         / "quarterly_financials.parquet"
#                     )   

#                     gold_path.parent.mkdir(
#                         parents=True,
#                         exist_ok=True,
#                     )

#                     quarterly_df.to_parquet(
#                         gold_path,
#                         index=False,
#                     )

#                     print(
#                         f"\nGold quarterly written: "
#                         f"{gold_path}"
#                     )

#             except Exception as e:

#                 print(
#                 f"ERROR - quarterly derivation "
#                 f"failed for {cik}: {e}"
#                 )

#     all_quarterly_df = (
#     pd.concat(
#         quarterly_results,
#         ignore_index=True
#     )
#     if quarterly_results
#     else pd.DataFrame()
# )

#     if all_quarterly_df.empty:
#         print("\nNo quarterly financials were generated; skipping data quality validation.")
#     else:
#         dq_df = validate_quarterly_data(
#             all_quarterly_df
#         )

#         print("\nQuarterly Data Quality:")

#         print(
#             dq_df[
#                 [
#                     "metric",
#                     "fiscal_year",
#                     "quarters_found",
#                     "missing_quarters",
#                     "is_complete",
#                 ]
#             ]
#             .tail(20)
#             .to_string(index=False)
#         )

#         incomplete = dq_df[
#             ~dq_df["is_complete"]
#         ]

#         print(
#             f"\nIncomplete fiscal years: "
#             f"{len(incomplete):,}"
#         )

#     print("\n")
#     print("=" * 60)
#     print("STEP 7 - GOLD FINANCIAL MODEL")
#     print("=" * 60)

#     for cik in COMPANIES:

#         cik = str(cik).zfill(10)

#         print(f"\nProcessing Gold financial model: {cik}")

#         try:

#         # ==================================================
#         # QUARTERLY GOLD
#         # ==================================================

#             quarterly_path = (
#             DATA_DIR
#             / "gold"
#             / "quarterly_financials"
#             / f"cik={cik}"
#             / "quarterly_financials.parquet"
#         )

#             quarterly_df = pd.read_parquet(
#             quarterly_path
#         )

#             quarterly_gold = build_quarterly_gold(
#             quarterly_df
#         )

#             quarterly_gold_path = (
#             DATA_DIR
#             / "gold"
#             / "financials_quarterly"
#             / f"cik={cik}"
#             / "financials_quarterly.parquet"
#         )

#             quarterly_gold_path.parent.mkdir(
#             parents=True,
#             exist_ok=True,
#         )

#             quarterly_gold.to_parquet(
#             quarterly_gold_path,
#             index=False,
#         )

#             print(
#             f"Quarterly Gold rows: "
#             f"{len(quarterly_gold):,}"
#         )

#         # ==================================================
#         # ANNUAL GOLD
#         # ==================================================

#             annual_path = (
#             DATA_DIR
#             / "gold"
#             / "financials"
#             / f"cik={cik}"
#             / "financials.parquet"
#         )

#             if annual_path.exists():

#                 annual_df = pd.read_parquet(
#                 annual_path
#             )

#                 annual_gold = build_annual_gold(
#                 annual_df
#             )

#                 annual_gold_path = (
#                 DATA_DIR
#                 / "gold"
#                 / "financials_annual"
#                 / f"cik={cik}"
#                 / "financials_annual.parquet"
#             )

#                 annual_gold_path.parent.mkdir(
#                 parents=True,
#                 exist_ok=True,
#             )

#                 annual_gold.to_parquet(
#                 annual_gold_path,
#                 index=False,
#             )

#                 print(
#                 f"Annual Gold rows: "
#                 f"{len(annual_gold):,}"
#                 )

#             else:

#                 print(
#                 "WARNING - Annual Gold source "
#                 f"not found: {annual_path}"
#                 )

#         except Exception as e:

#             print(
#             f"ERROR - Gold financial model "
#             f"failed for {cik}: {e}"
#             )         


#     print("\n")
#     print("=" * 60)
#     print("STEP 8 - METRIC DIMENSION")
#     print("=" * 60)

#     try:
#         metric_config_path = (
#         BASE_DIR
#         / "config"
#         / "metrics_catalog.json"
#     )

#         metrics_df = build_metric_dimension(
#         metric_config_path
#     )

#         metrics_gold_path = (
#         DATA_DIR
#         / "gold"
#         / "metrics"
#         / "metrics.parquet"
#     )

#         metrics_gold_path.parent.mkdir(
#         parents=True,
#         exist_ok=True,
#     )

#         metrics_df.to_parquet(
#         metrics_gold_path,
#         index=False,
#     )

#         print(
#         f"Metrics loaded: "
#         f"{len(metrics_df):,}"
#     )

#         print("\nMetric Dimension:")
#         print(
#         metrics_df.to_string(
#             index=False
#         )
#     )

#         print(
#         f"\nGold metric dimension written: "
#         f"{metrics_gold_path}"
#     )

#     except Exception as e:

#         print(
#         f"ERROR - Metric dimension failed: {e}"
#         )



    # print("=" * 60)
    # print("STEP 9 - FINANCIAL DATA QUALITY & RECONCILIATION")
    # print("=" * 60)

    # ensure_output_dirs()

    # summaries = []

    # all_completeness = []
    # all_reconciliation = []
    # all_incomplete = []
    # all_duplicates = []

    # for cik in COMPANIES:

    #     print("\n" + "=" * 60)
    #     print(f"Validating financials: {cik}")
    #     print("=" * 60)

    #     try:

    #         quarterly = load_quarterly(
    #             cik
    #         )

    #         annual = load_annual(
    #             cik
    #         )

    #         print(
    #             f"Quarterly rows: {len(quarterly):,}"
    #         )

    #         print(
    #             f"Annual rows: {len(annual):,}"
    #         )

    #         # ------------------------------------------------
    #         # DUPLICATES
    #         # ------------------------------------------------

    #         duplicates = find_quarter_duplicates(
    #             quarterly,
    #             cik,
    #         )

    #         print(
    #             f"Quarter duplicate groups: "
    #             f"{len(duplicates):,}"
    #         )

    #         if not duplicates.empty:
    #             all_duplicates.append(
    #                 duplicates
    #             )

    #         # ------------------------------------------------
    #         # COMPLETENESS
    #         # ------------------------------------------------

    #         completeness = (
    #             build_quarter_completeness(
    #                 quarterly,
    #                 cik,
    #             )
    #         )

    #         incomplete = (
    #             build_incomplete_years(
    #                 completeness
    #             )
    #         )

    #         print(
    #             f"Quarter completeness checks: "
    #             f"{len(completeness):,}"
    #         )

    #         print(
    #             f"Incomplete fiscal-year groups: "
    #             f"{len(incomplete):,}"
    #         )

    #         all_completeness.append(
    #             completeness
    #         )

    #         if not incomplete.empty:
    #             all_incomplete.append(
    #                 incomplete
    #             )

    #         # ------------------------------------------------
    #         # RECONCILIATION
    #         # ------------------------------------------------

    #         reconciliation = (
    #             build_annual_reconciliation(
    #                 quarterly,
    #                 annual,
    #                 completeness,
    #                 cik,
    #             )
    #         )

    #         print(
    #             f"Annual reconciliation checks: "
    #             f"{len(reconciliation):,}"
    #         )

    #         failures = reconciliation[
    #             reconciliation[
    #                 "reconciliation_status"
    #             ]
    #             == "RECONCILIATION_EXCEPTION"
    #         ]

    #         print(
    #             f"Reconciliation failures: "
    #             f"{len(failures):,}"
    #         )

    #         all_reconciliation.append(
    #             reconciliation
    #         )

    #         # ------------------------------------------------
    #         # SUMMARY
    #         # ------------------------------------------------

    #         summary = build_summary(
    #             cik,
    #             quarterly,
    #             annual,
    #             completeness,
    #             reconciliation,
    #             duplicates,
    #         )

    #         summaries.append(
    #             summary
    #         )

    #     except Exception as exc:

    #         print(
    #             f"\nERROR processing {cik}: "
    #             f"{type(exc).__name__}: {exc}"
    #         )

    # # ========================================================
    # # COMBINE OUTPUTS
    # # ========================================================

    # completeness_all = (
    #     pd.concat(
    #         all_completeness,
    #         ignore_index=True,
    #     )
    #     if all_completeness
    #     else pd.DataFrame()
    # )

    # reconciliation_all = (
    #     pd.concat(
    #         all_reconciliation,
    #         ignore_index=True,
    #     )
    #     if all_reconciliation
    #     else pd.DataFrame()
    # )

    # incomplete_all = (
    #     pd.concat(
    #         all_incomplete,
    #         ignore_index=True,
    #     )
    #     if all_incomplete
    #     else pd.DataFrame()
    # )

    # duplicates_all = (
    #     pd.concat(
    #         all_duplicates,
    #         ignore_index=True,
    #     )
    #     if all_duplicates
    #     else pd.DataFrame()
    # )

    # summary_df = pd.DataFrame(
    #     summaries
    # )

    # # ========================================================
    # # WRITE PARQUET
    # # ========================================================

    # completeness_path = (
    #     OUTPUT_DIR
    #     / "quarterly_completeness"
    #     / "quarterly_completeness.parquet"
    # )

    # reconciliation_path = (
    #     OUTPUT_DIR
    #     / "annual_reconciliation"
    #     / "annual_reconciliation.parquet"
    # )

    # failures_path = (
    #     OUTPUT_DIR
    #     / "reconciliation_failures"
    #     / "reconciliation_failures.parquet"
    # )

    # incomplete_path = (
    #     OUTPUT_DIR
    #     / "incomplete_fiscal_years"
    #     / "incomplete_fiscal_years.parquet"
    # )

    # duplicates_path = (
    #     OUTPUT_DIR
    #     / "dq_summary"
    #     / "quarter_duplicates.parquet"
    # )

    # summary_path = (
    #     OUTPUT_DIR
    #     / "dq_summary"
    #     / "financial_dq_summary.parquet"
    # )

    # completeness_all.to_parquet(
    #     completeness_path,
    #     index=False,
    # )

    # reconciliation_all.to_parquet(
    #     reconciliation_path,
    #     index=False,
    # )

    # reconciliation_failures = (
    #     reconciliation_all[
    #         reconciliation_all[
    #             "reconciliation_status"
    #         ]
    #         == "RECONCILIATION_EXCEPTION"
    #     ]
    #     .copy()
    # )

    # reconciliation_failures.to_parquet(
    #     failures_path,
    #     index=False,
    # )

    # incomplete_all.to_parquet(
    #     incomplete_path,
    #     index=False,
    # )

    # duplicates_all.to_parquet(
    #     duplicates_path,
    #     index=False,
    # )

    # summary_df.to_parquet(
    #     summary_path,
    #     index=False,
    # )

    # # ========================================================
    # # FINAL TERMINAL SUMMARY
    # # ========================================================

    # print("\n")
    # print("=" * 60)
    # print("STEP 9 - DQ SUMMARY")
    # print("=" * 60)

    # print(
    #     f"\nQuarter completeness checks: "
    #     f"{len(completeness_all):,}"
    # )

    # print(
    #     f"Incomplete fiscal years: "
    #     f"{len(incomplete_all):,}"
    # )

    # print(
    #     f"\nAnnual reconciliation checks: "
    #     f"{len(reconciliation_all):,}"
    # )

    # print(
    #     f"Reconciliation failures: "
    #     f"{len(reconciliation_failures):,}"
    # )

    # print("\n" + "-" * 60)
    # print("RECONCILIATION FAILURES")
    # print("-" * 60)

    # if reconciliation_failures.empty:
    #     print("None")
    # else:
    #     print(
    #         reconciliation_failures[
    #             [
    #                 "cik",
    #                 "metric",
    #                 "fiscal_year",
    #                 "quarter_sum",
    #                 "annual_value",
    #                 "difference",
    #                 "difference_pct",
    #             ]
    #         ]
    #         .to_string(
    #             index=False
    #         )
    #     )

    # print("\n" + "-" * 60)
    # print("INCOMPLETE FISCAL YEARS")
    # print("-" * 60)

    # if incomplete_all.empty:
    #     print("None")
    # else:
    #     print(
    #         incomplete_all[
    #             [
    #                 "cik",
    #                 "metric",
    #                 "fiscal_year",
    #                 "quarters_found",
    #                 "missing_quarters",
    #                 "is_complete",
    #             ]
    #         ]
    #         .to_string(
    #             index=False
    #         )
    #     )

    # print("\n" + "-" * 60)
    # print("OUTPUTS")
    # print("-" * 60)

    # print(
    #     f"Quarter completeness:\n"
    #     f"{completeness_path}"
    # )

    # print(
    #     f"\nAnnual reconciliation:\n"
    #     f"{reconciliation_path}"
    # )

    # print(
    #     f"\nReconciliation failures:\n"
    #     f"{failures_path}"
    # )

    # print(
    #     f"\nIncomplete fiscal years:\n"
    #     f"{incomplete_path}"
    # )

    # print(
    #     f"\nDQ summary:\n"
    #     f"{summary_path}"
    # )

    # print("\n")
    # print("=" * 60)
    # print("STEP 9 COMPLETE")
    # print("=" * 60)


    # print()
    # print("=" * 60)
    # print("STEP 10 - FINAL QUARTERLY GOLD FINANCIALS")
    # print("=" * 60)

    # results = []
    # failures = []

    # for cik in COMPANIES:

    #     try:
    #         result = process_company(cik)
    #         results.append(result)

    #     except Exception as exc:

    #         failures.append(
    #             {
    #                 "cik": cik,
    #                 "error": repr(exc),
    #             }
    #         )

    #         print()
    #         print("=" * 60)
    #         print(f"ERROR processing {cik}")
    #         print("=" * 60)
    #         print(repr(exc))

    # # --------------------------------------------------------
    # # STEP 10 SUMMARY
    # # --------------------------------------------------------

    # print()
    # print("=" * 60)
    # print("STEP 10 SUMMARY")
    # print("=" * 60)

    # if results:
    #     summary = pd.DataFrame(results)

    #     print(
    #         summary[
    #             [
    #                 "cik",
    #                 "step6_rows",
    #                 "candidate_gold_rows",
    #                 "final_gold_rows",
    #                 "quarantine_rows",
    #                 "reconciliation_exception_rows",
    #                 "incomplete_fiscal_year_rows",
    #                 "duplicate_exception_rows",
    #             ]
    #         ].to_string(index=False)
    #     )

    # else:
    #     print("No companies completed successfully.")

    # # --------------------------------------------------------
    # # FAILURES
    # # --------------------------------------------------------

    # if failures:
    #     print()
    #     print("=" * 60)
    #     print("STEP 10 FAILURES")
    #     print("=" * 60)

    #     print(
    #         pd.DataFrame(failures).to_string(index=False)
    #     )

    # print()
    # print("=" * 60)

    # if failures:
    #     print(
    #         f"STEP 10 COMPLETED WITH ERRORS "
    #         f"({len(failures)} failed, {len(results)} succeeded)"
    #     )
    # else:
    #     print("STEP 10 COMPLETE - ALL COMPANIES SUCCESSFUL")

    # print("=" * 60)

    # ============================================================
# # STEP 11 - FINANCIAL ANALYTICS
# # ============================================================

    print()
    print("=" * 60)
    print("STEP 11 - FINANCIAL ANALYTICS")
    print("=" * 60)

    analytics_results = []
    analytics_failures = []

    for cik in COMPANIES:

        try:

            result = process_financial_analytics(cik)

            analytics_results.append(result)

        except Exception as exc:

            analytics_failures.append(
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


    print()
    print("=" * 60)
    print("STEP 11 SUMMARY")
    print("=" * 60)

    if analytics_results:

        analytics_summary = pd.DataFrame(
        analytics_results
        )

        print(
        analytics_summary[
            [
                "cik",
                "input_rows",
                "analytics_rows",
                "duplicate_rows_removed",
            ]
        ].to_string(index=False)
        )

    else:

        print(
        "No companies completed successfully."
        )


    if analytics_failures:

        print()
        print("=" * 60)
        print("STEP 11 FAILURES")
        print("=" * 60)

        print(
        pd.DataFrame(
            analytics_failures
        ).to_string(index=False)
    )

        print()
        print("STEP 11 COMPLETED WITH ERRORS")

    else:

        print()
        print("STEP 11 COMPLETE - ALL COMPANIES SUCCESSFUL")


if __name__ == "__main__":
     main()