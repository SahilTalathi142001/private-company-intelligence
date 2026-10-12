# CIDM — Direct integration with the current data

This package replaces the website and read-only presentation API in the repository's **`src/company_platform/`** layout. It does not contain, copy, seed, or replace the project's `data/` directory. The API reads the existing `data/silver` and `data/gold` artifacts at runtime.

## Install once

From PowerShell, extract this package to a temporary folder and run:

```powershell
powershell -ExecutionPolicy Bypass -File .\install_cidm.ps1 -RepoRoot "C:\Users\stalathi\Desktop\private-company-intelligence"
```

If your data directory is outside the repository, pass it explicitly:

```powershell
powershell -ExecutionPolicy Bypass -File .\install_cidm.ps1 -RepoRoot "C:\Users\stalathi\Desktop\private-company-intelligence" -DataRoot "D:\CIDM\data" -PythonPath "C:\Users\stalathi\Desktop\private-company-intelligence\.venv\Scripts\python.exe"
```

The installer verifies the real registry, structured-event DB, control DB, and unified company view **before changing anything**. The selected Python interpreter and data root are saved in `.cidm_python_path` and `.cidm_data_root`, so subsequent launches use the same environment and actual data directory. It also checks the selected Python environment for FastAPI, Uvicorn, Pydantic, pandas, and PyArrow; if needed, it installs the versions in `requirements-api.txt` before modifying source files. Pass `-PythonPath` if you want to pin a specific interpreter. It backs up the existing API and website under `backups\cidm_presentation_<timestamp>`, replaces only `src/company_platform/api` and `src/company_platform/web`, and leaves data and pipeline modules untouched. If installation fails, it restores the previous API and website.

## Run and verify against the real artifacts

```powershell
.\run_cidm.ps1
```

Open `http://127.0.0.1:8000/` and `http://127.0.0.1:8000/api/docs`.

From the repository root, run the read-only integration verifier once before starting the server:

```powershell
.\run_cidm.ps1 -Verify
```

The verifier is read-only. It checks the actual registry and every listed company across pages, company/CIK isolation, per-domain artifact readability, and the production-event boundary. Missing domain data is reported as missing rather than converted to zero. The verifier exits non-zero on registry errors, identity conflicts, artifact read errors, or non-production events leaking into the production list.

## API

- `GET /api/v1/health`
- `GET /api/v1/companies?q=&limit=50&offset=0&sort=name&direction=asc`
- `GET /api/v1/companies/{cik}`
- `GET /api/v1/companies/{cik}/domains/{domain}`
- `GET /api/v1/coverage`
- `GET /api/v1/operations/summary`
- OpenAPI: `/api/openapi.json`; interactive docs: `/api/docs`

Company domains are sourced from the existing project artifacts: unified company view, Gold financials (Silver financial facts only as a fallback), Step 17 funding snapshots/events, Step 18 ownership snapshots, Step 19 valuation snapshot, Step 20 investment profile/signals, and Step 34.1 coverage matrix. Artifact paths are returned in the response for traceability.

## Data correctness rules

- No demo companies, fake values, synthetic financial history, zero-fill, or estimated valuation is introduced.
- Company identity remains anchored to the Silver company registry. Duplicate active registry rows are exposed as identity conflicts; they are not silently deduplicated.
- `AVAILABLE` means the expected artifact has matching rows. It does **not** mean the data is complete, current, or independently verified.
- Missing, unreadable, and ambiguous artifacts are distinguished (`MISSING`, `ERROR`, or `CONFLICT`).
- The current `structured_events` schema uses `source_document_path`; the API reads that field and recognizes production only from persisted SEC bronze paths. Test and unknown-source events are not displayed as production.
- Run and task statuses are nested inside the `data` JSON field in the current `control.db`; the API parses that actual schema instead of counting every run as an unknown status.
- SEC Form D observations are not represented as a complete fundraising history.
- The website/API are read-only. They do not run ingestion, retry events, modify artifacts, or mutate databases.

## Runtime requirements

Use the repository's existing virtual environment and dependencies, including pandas, pyarrow (for Parquet), FastAPI, and Uvicorn. `CIDM_DATA_DIR` may be set when data is outside `<repo>\data`.
