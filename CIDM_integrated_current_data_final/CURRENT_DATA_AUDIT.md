# CIDM current-data integration audit

**Basis:** the supplied `private-company-intelligence(2).zip` snapshot. This is a read-only audit of the artifacts in that archive, not a claim that the live local repository was modified by this tool.

## Artifact presence

The archive contains the artifacts the replacement API reads:

- `data/silver/platform/company_registry.parquet`
- `data/silver/platform/structured_events.db`
- `data/silver/platform/control.db`
- `data/gold/unified_company_intelligence/unified_company_view/unified_company_view.parquet`
- Step 17 company funding snapshot and funding events
- Step 18 ownership snapshot
- Step 19 valuation snapshot
- Step 20 investment profile and investment signals
- Step 34.1 coverage matrix
- 10 `data/gold/financials/cik=*/financials.parquet` company partitions

## Counts read directly from the current SQLite stores

| Artifact/table | Rows | Status breakdown |
|---|---:|---|
| `structured_events.db:structured_events` | 145 | 101 recognized persisted production SEC-path events; 44 test events excluded |
| `control.db:events` | 1,035 | 110 completed; 72 failed; 853 pending |
| `control.db:pipeline_runs` | 406 | 297 success; 108 failed; 1 partial |
| `control.db:task_runs` | 4,314 | 4,014 success; 214 blocked; 80 failed; 6 capability-blocked |

The event boundary was checked against the actual `source_document_path` column. The API does not infer production from the absence of a test prefix; it requires a persisted event and a recognized SEC bronze document path. In this archive, the classification matches the observed 101 production / 44 test split.

## Explicit contamination safeguard

The archive contains a `TEST-E2E-DOCUMENT-...` marker inside:

`data/gold/company_investment_intelligence/cik=0000320193/unified_investment_intelligence.parquet`

That file is **not** used as a fallback for the website's Step 20 investment-signal domain. The API reads the dedicated Step 20 `investment_signals.parquet` instead. Event-derived state/signal fields in the unified company view are also withheld unless `source_document_id` matches a persisted production event for the same CIK. This avoids promoting test-derived or unverifiable state to production UI fields.

## Validation boundary

The SQLite counts and production/test classification were executed against the actual files extracted from the supplied archive. Python compilation, JavaScript syntax, route/static-file smoke checks, and 22 updated Step 32/33 contract tests passed in the build environment. Because this container lacks a Linux-compatible Parquet engine, the fixture-based contract tests used a temporary JSON shim for Parquet test files; that shim is not included in the package and is never used by the application. The real Parquet rows were not decoded end-to-end here. The included `run_cidm.ps1 -Verify` command performs the final read-only validation against the real Parquet files using the repository's Windows Python environment and dependencies.
