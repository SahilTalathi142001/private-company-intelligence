# Integration v2 — Five-Component Financial Execution Chain

This release wires the existing financial pipeline into A8 orchestration and A7 promotion gates.

## Chain

SEC ingestion -> financial normalization -> quarter derivation -> financial features -> financial decision

The `financial_decision` adapter is deliberately a composite around frozen Steps 13, 13.1, 14, 15 and 15.1. Step 14 is peer/global by design, so v2 does **not** falsely claim single-company ranking. A future incremental ranking design must explicitly handle peer-population invalidation.

## A8 / A7 behavior

A8 creates the run and five tasks. Each handler executes the existing implementation and then invokes A7 with an operational artifact contract. A7 persists both the DQ result and promotion decision. A8 records SUCCESS/FAILED/QUARANTINED in A5 and blocks downstream tasks on failure.

## Run

From project root:

```powershell
$env:PYTHONPATH="$PWD\src"
python -m company_platform.integration_v2.runner --cik 0000320193 --dry-run
python -m company_platform.integration_v2.runner --cik 0000320193
```

The first real run should use Apple. The run will refresh SEC submissions/companyfacts, rebuild the affected financial artifacts, and execute the financial decision population.

## Important contract findings

1. Existing Step 6 and the old SEC `main.py` still contain hardcoded development-company loops. v2 does not call those `main()` functions; it calls the reusable company-scoped functions.
2. Step 10 and Step 11 are included inside the `quarter_derivation` adapter because Step 12 requires their final Gold and analytics artifacts.
3. Step 13/14/15 are included inside `financial_decision` because Step 15 consumes Step 13 intelligence, Step 14 ranking and calibration; A4 currently models this as one financial_decision component.
4. Local Parquet run/DQ stores remain reference/local state. They are not yet transaction-safe for distributed production.


## Integration v2.1

v2.1 fixes repository-root path resolution for pipeline definitions, A5 run/task state, and A7 DQ/promotion state. These paths no longer depend on the shell current working directory. The A5 inspection CLI also supports `--run-id` for both `runs` and `tasks`.

Run from either the repository root or `src`:
```powershell
$env:PYTHONPATH="$PWD\src"
python -m company_platform.integration_v2.runner --cik 0000320193
```
If currently inside `src`, the same command remains valid.
