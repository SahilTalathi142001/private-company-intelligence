from pathlib import Path


def test_components_contains_incremental_step9_contract():
    p = Path(__file__).resolve().parent / "components.py"
    text = p.read_text(encoding="utf-8")
    assert "build_annual_reconciliation" in text
    assert "build_quarter_completeness" in text
    assert "build_incomplete_years" in text
    assert "find_quarter_duplicates" in text
    assert "normalize_columns" in text
    assert "normalize_quarter" in text
    assert "quarterly_dq = normalize_quarter(normalize_columns(quarterly))" in text
    assert "reconciliation_failures" in text
    assert "build_reconciliation_keys" in text
    assert "reconciliation_failures" in text


def test_only_failed_reconciliation_rows_become_exception_keys():
    import pandas as pd
    from sec_ingestion.final_quarterly_gold import build_reconciliation_keys

    reconciliation = pd.DataFrame([
        {"cik": "0000320193", "metric": "revenue", "fiscal_year": 2024, "reconciliation_status": "RECONCILED"},
        {"cik": "0000320193", "metric": "revenue", "fiscal_year": 2025, "reconciliation_status": "RECONCILIATION_EXCEPTION"},
    ])
    failures = reconciliation[reconciliation["reconciliation_status"] == "RECONCILIATION_EXCEPTION"]
    assert build_reconciliation_keys(failures) == {("0000320193", "revenue", 2025)}
