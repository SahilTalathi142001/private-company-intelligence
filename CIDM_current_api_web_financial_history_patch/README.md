# CIDM financial history API + web patch (based on current uploaded files)

This patch was built against the uploaded API and web sources that report API version `1.0.0`.
It does not rerun ingestion, edit Gold/Silver artifacts, or generate financial values.

## Changes

- Adds `GET /api/v1/companies/{cik}/financial-history`.
- Supports metric substring, fiscal year, fiscal period, sort direction, pagination, and available filter options.
- Uses the existing company-scoped Gold financials artifact, with the service's existing configured Silver fallback.
- Returns source artifact, coverage facets, and notes alongside facts.
- Adds a financial-history interface on the company Financials tab with filters, pagination, and artifact/business-data/completeness/audit statuses.
- Preserves missing values as null; filters do not fill or interpolate values.
- Filters rows by CIK defensively if a CIK column exists in the selected frame.

## Apply on Windows

From the extracted patch directory:

```powershell
.\apply_cidm_financial_history_patch.ps1 -RepoRoot "C:\Users\stalathi\Desktop\private-company-intelligence"
```

The script checks that expected target files and version markers exist, creates timestamped backups under `backups\cidm_financial_history_<timestamp>`, and then copies the patched files. If a target does not match, it stops rather than overwriting it.

## Validate

From the repository root:

```powershell
python -m py_compile company_platform\api\service.py company_platform\api\app.py
python -m unittest discover -s tests -p "test_financial_history_api.py" -v
```

Restart the API process, then check:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/companies/0000320193/financial-history?limit=10"
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/companies/0000789019/financial-history?limit=10"
```

The included unit tests use mocked data for deterministic filtering and isolation checks. They do not prove the real local Parquet artifacts are valid or that fiscal-year labels are correct. Validate the returned rows against the source facts/SEC filings before treating financial periods as reconciled.
