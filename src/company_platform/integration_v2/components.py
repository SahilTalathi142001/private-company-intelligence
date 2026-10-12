from __future__ import annotations

import importlib
import os
import tempfile
from pathlib import Path
from typing import Callable
import pandas as pd
import logging
from company_platform.document_classification import (
    classify_documents_for_company,
)
from company_platform.run_control.run_store import RunStore

from src.sec_ingestion.document_ingestion import ingest_sec_filing_documents
from company_platform.document_extraction import extract_documents_for_company

logger = logging.getLogger(__name__)

from company_platform.orchestrator.engine import ExecutionOutcome
from company_platform.dq_gates.gate_engine import DQGateEngine
from .execution_context import ExecutionContext
from company_platform.registry.company_registry import (
    register_company,
    mark_onboarding_active,
    mark_onboarding_failed,
)
from company_platform.document_understanding.structured_event_persistence import (
    persist_structured_events,
)

from company_platform.registry.source_registry import (
    enable_company_sources,
)

from company_platform.document_understanding import (
    understand_documents_for_company,
)
from company_platform.company_intelligence.state_projector import (
    CompanyStateProjector,
)
from company_platform.investment_intelligence import (
    UnifiedInvestmentIntelligence,
)
ROOT = Path(__file__).resolve().parents[3]


def _path(*parts: str) -> Path:
    return ROOT.joinpath(*parts)


def _require(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f'{label} not found: {path}')
    return path


def _gate(context: ExecutionContext, component_id: str, task_id: str,
          checks: int, failed: int = 0, fatal: int = 0, warnings: int = 0,
          message: str = '') -> ExecutionOutcome:
    engine = DQGateEngine()
    result = engine.evaluate(
        run_id=context.run_id, task_id=task_id, component_id=component_id,
        check_count=checks, failed_count=failed, fatal_count=fatal,
        warning_count=warnings, message=message or None,
    )
    decision = engine.promote(result)
    # Persist the actual A7 result/decision. The orchestrator will persist task state.
    from company_platform.dq_gates.promotion_store import DQPromotionStore
    store = DQPromotionStore()
    store.append_dq(result)
    store.append_promotion(decision)
    if decision.decision == 'BLOCK':
        return ExecutionOutcome('FAILED', decision.reason_code, dq_failed=True)
    if decision.decision == 'QUARANTINE':
        return ExecutionOutcome('QUARANTINED', decision.reason_code, dq_failed=True, quarantine=True)
    return ExecutionOutcome('SUCCESS', dq_failed=False)


def _event_document_id(context: ExecutionContext) -> str | None:
    metadata = context.metadata or {}
    source_id = str(metadata.get("source_id") or "").strip().upper()
    payload = metadata.get("payload") or {}
    explicit = payload.get("document_id")
    if explicit:
        return str(explicit).strip()
    if source_id not in {"SEC", "SEC_FORM_D"}:
        return None
    accession = str(payload.get("accession_number") or "").strip()
    primary = str(payload.get("primary_document") or "").strip()
    if not accession:
        return None
    manifest = _path("data", "bronze", "sec", "documents", f"cik={context.cik}", "document_manifest.parquet")
    if not manifest.is_file():
        return None
    frame = pd.read_parquet(manifest)
    if frame.empty or "accession_number" not in frame.columns:
        return None
    mask = frame["accession_number"].astype(str).eq(accession)
    if primary and "primary_document" in frame.columns:
        mask &= frame["primary_document"].astype(str).eq(primary)
    matches = frame.loc[mask]
    if matches.empty:
        return None
    return str(matches.iloc[-1]["document_id"]).strip()

def _artifact_dq(context, component_id, task_id, artifacts, row_count=None):
    checks = len(artifacts) + 2
    failed = sum(1 for p in artifacts if not Path(p).exists())
    failed += 1 if row_count is not None and row_count <= 0 else 0
    return _gate(context, component_id, task_id, checks, failed=failed,
                 fatal=failed, message=f'{component_id}: artifact contract validation')

def _document_extraction(context, component_id, task_id):
    metadata = context.metadata or {}
    payload = metadata.get("payload") or {}
    source_id = metadata.get("source_id")

    result = extract_documents_for_company(
        cik=context.cik,
        source_id=source_id,
        document_id=_event_document_id(context),
    )

    if int(result["failed"]) > 0:
        raise RuntimeError(
            f"Document extraction failed: {result}"
        )

    extracted = int(result["extracted"])
    skipped = int(result["skipped"])

    if extracted == 0 and skipped == 0:
        raise RuntimeError(
            "Document extraction produced no extracted or skipped documents"
        )

    return _artifact_dq(
        context,
        component_id,
        task_id,
        [],
        row_count=extracted + skipped,
    )

def _sec_ingestion(context: ExecutionContext, component_id: str, task_id: str) -> ExecutionOutcome:
    from src.sec_ingestion.client import SECAPIClient
    from src.sec_ingestion.submissions import ingest_submissions
    from src.sec_ingestion.facts import ingest_company_facts
    cik = context.cik
    client = SECAPIClient()
    submissions = ingest_submissions(client, cik)
    facts = ingest_company_facts(client, cik)
    artifacts = [
        _path('data', 'bronze', 'sec', 'submissions', f'{cik}.json'),
        _path('data', 'bronze', 'sec', 'companyfacts', f'{cik}.json'),
    ]
    checks = 6
    failed = sum(not p.exists() for p in artifacts)
    failed += int(submissions is None or len(submissions) == 0)
    failed += int(facts is None or len(facts) == 0)
    return _gate(context, component_id, task_id, checks, failed, failed,
                 message=f'SEC ingestion: submissions={len(submissions)}, facts={len(facts)}')


def _financial_normalization(context: ExecutionContext, component_id: str, task_id: str) -> ExecutionOutcome:
    from src.sec_ingestion.build_financial_silver import process_company
    cik = context.cik
    result = process_company(cik)
    path = _path('data', 'silver', 'financial_facts', f'cik={cik}', 'financial_facts.parquet')
    df = pd.read_parquet(_require(path, 'Financial Silver'))
    return _artifact_dq(context, component_id, task_id, [path], len(df))


def _quarter_derivation(context: ExecutionContext, component_id: str, task_id: str) -> ExecutionOutcome:
    from src.sec_ingestion.derive_quarters import derive_quarters
    from src.sec_ingestion.final_quarterly_gold import (
        build_reconciliation_keys, build_incomplete_keys,
        build_duplicate_keys, process_company as promote_final_gold,
    )
    from src.sec_ingestion.financial_analytics import process_company as analytics_company

    cik = context.cik
    silver = _require(_path('data', 'silver', 'financial_facts', f'cik={cik}', 'financial_facts.parquet'), 'Financial Silver')
    df = pd.read_parquet(silver)
    quarterly = derive_quarters(df)
    qpath = _path('data', 'gold', 'financials_quarterly', f'cik={cik}', 'financials_quarterly.parquet')
    qpath.parent.mkdir(parents=True, exist_ok=True)
    quarterly.to_parquet(qpath, index=False)

    # Step 9 DQ is a prerequisite of Step 10, but the historical Step 9
    # driver is batch-oriented and its main() is commented out. For an
    # incremental company run, execute the same frozen Step 9 functions for
    # this CIK and pass the resulting keys directly into Step 10. This avoids
    # treating a stale/global parquet artifact as the dependency contract and
    # avoids overwriting global DQ outputs during a single-company run.
    from src.sec_ingestion.quarterly_reconciliation import (
        build_annual_reconciliation,
        build_incomplete_years,
        build_quarter_completeness,
        find_quarter_duplicates,
        load_annual,
        normalize_columns,
        normalize_quarter,
    )

    # Use the quarterly dataframe produced by Step 6 above. Do NOT reload a
    # legacy Step 9 quarterly artifact: that path can contain stale/global
    # data from a previous batch run and would break incremental correctness.
    quarterly_dq = normalize_quarter(normalize_columns(quarterly))
    annual_dq = load_annual(cik)
    duplicates = find_quarter_duplicates(quarterly_dq, cik)
    completeness = build_quarter_completeness(quarterly_dq, cik)
    incomplete = build_incomplete_years(completeness)
    reconciliation = build_annual_reconciliation(
        quarterly_dq, annual_dq, completeness, cik
    )

    # Persist a company-scoped Step 9 audit artifact for lineage/debugging.
    step9_root = _path('data', 'gold', 'financial_dq', f'cik={cik}')
    (step9_root / 'quarterly_completeness').mkdir(parents=True, exist_ok=True)
    (step9_root / 'annual_reconciliation').mkdir(parents=True, exist_ok=True)
    (step9_root / 'reconciliation_failures').mkdir(parents=True, exist_ok=True)
    (step9_root / 'incomplete_fiscal_years').mkdir(parents=True, exist_ok=True)
    (step9_root / 'dq_summary').mkdir(parents=True, exist_ok=True)

    completeness.to_parquet(
        step9_root / 'quarterly_completeness' / 'quarterly_completeness.parquet',
        index=False,
    )
    reconciliation.to_parquet(
        step9_root / 'annual_reconciliation' / 'annual_reconciliation.parquet',
        index=False,
    )
    failures = reconciliation[
        reconciliation['reconciliation_status'] == 'RECONCILIATION_EXCEPTION'
    ].copy()
    failures.to_parquet(
        step9_root / 'reconciliation_failures' / 'reconciliation_failures.parquet',
        index=False,
    )
    incomplete.to_parquet(
        step9_root / 'incomplete_fiscal_years' / 'incomplete_fiscal_years.parquet',
        index=False,
    )
    duplicates.to_parquet(
        step9_root / 'dq_summary' / 'quarter_duplicates.parquet',
        index=False,
    )

    # Step 9's reconciliation table contains BOTH reconciled and failed
    # metric/fiscal-year rows. Step 10 must receive exception keys only for
    # rows that actually failed reconciliation. Passing the full reconciliation
    # table would quarantine nearly every complete metric-year.
    reconciliation_failures = reconciliation[
        reconciliation["reconciliation_status"]
        == "RECONCILIATION_EXCEPTION"
    ].copy()

    reconciliation_keys = build_reconciliation_keys(
        reconciliation_failures
    )
    incomplete_keys = build_incomplete_keys(incomplete)
    duplicate_keys = build_duplicate_keys(duplicates)

    logger.info(
        "STEP 9 reconciliation audit | cik=%s | checks=%d | failures=%d | exception_keys=%d",
        cik,
        len(reconciliation),
        len(reconciliation_failures),
        len(reconciliation_keys),
    )

    promote_final_gold(
        cik,
        reconciliation_keys,
        incomplete_keys,
        duplicate_keys,
    )
    analytics_result = analytics_company(cik)
    final_path = _path('data', 'gold', 'financials_quarterly_final', f'cik={cik}', 'financials_quarterly.parquet')
    analytics_path = _path('data', 'gold', 'financials_analytics', f'cik={cik}', 'financials_analytics.parquet')
    artifacts = [qpath, final_path, analytics_path]
    row_count = len(pd.read_parquet(final_path)) if final_path.exists() else 0
    return _artifact_dq(context, component_id, task_id, artifacts, row_count)


def _financial_features(context: ExecutionContext, component_id: str, task_id: str) -> ExecutionOutcome:
    from src.sec_ingestion.step12_financial_features import process_company
    cik = context.cik
    features, summary = process_company(cik)
    # Step 12 separates computation from persistence; the adapter owns the
    # component output contract and must persist the returned features.
    from src.sec_ingestion.step12_financial_features import save_company_output
    save_company_output(cik, features)
    path = _path('data', 'gold', 'financials_features', f'cik={cik}', 'financial_features.parquet')
    if not path.exists():
        # Use the module's configured output discovery if its filename differs.
        candidates = list(_path('data', 'gold').glob(f'**/cik={cik}/financial_features.parquet'))
        if candidates: path = candidates[0]
    return _artifact_dq(context, component_id, task_id, [path], len(features))


def _financial_decision(context: ExecutionContext, component_id: str, task_id: str) -> ExecutionOutcome:
    """Composite adapter for frozen Steps 13-15.

    Step 14 is peer/global by design, so this stage currently recomputes the
    financial decision population rather than claiming single-company ranking.
    """
    modules = [
        ('src.sec_ingestion.step13_financial_intelligence', 'main'),
        ('src.sec_ingestion.step13_1_validate_intelligence', 'main'),
        ('src.sec_ingestion.step14_financial_ranking', 'main'),
        ('src.sec_ingestion.step14_1_ranking_calibration', 'main'),
        ('src.sec_ingestion.step15_financial_decision_intelligence', 'main'),
        ('src.sec_ingestion.step15_1_validate_decision_intelligence', 'main'),
    ]
    # Existing Steps 13-15 expose CLI-oriented main() functions that call
    # argparse. The platform orchestrator owns the CLI, so never allow the
    # orchestrator's arguments (for example --cik) to leak into those parsers.
    import sys
    original_argv = sys.argv[:]
    sec_dir = str(_path('src', 'sec_ingestion'))
    added_sec_dir = sec_dir not in sys.path
    if added_sec_dir:
        sys.path.insert(0, sec_dir)
    try:
        for module_name, fn_name in modules:
            module = importlib.import_module(module_name)
            sys.argv = [module_name]
            getattr(module, fn_name)()
    finally:
        sys.argv = original_argv
        if added_sec_dir:
            try:
                sys.path.remove(sec_dir)
            except ValueError:
                pass

    cik = context.cik
    out_root = _path('data', 'gold', 'financial_decision_intelligence')
    artifacts = [
        out_root / 'decision_signals' / 'financial_decision_signals.parquet',
        out_root / 'company_financial_decision_snapshot' / 'company_financial_decision_snapshot.parquet',
        out_root / 'financial_decision_summary' / 'financial_decision_summary.parquet',
    ]
    existing = [p for p in artifacts if p.exists()]
    if not existing:
        raise FileNotFoundError(f'No financial decision artifacts found under {out_root}')
    rows = 0
    company_rows = 0
    for p in existing:
        try:
            df = pd.read_parquet(p)
            rows += len(df)
            if 'cik' in df.columns:
                company_rows += int((df['cik'].astype(str).str.zfill(10) == cik).sum())
        except Exception:
            pass
    if company_rows == 0:
        raise RuntimeError(f'Financial decision artifacts contain no row for CIK {cik}')
    return _artifact_dq(context, component_id, task_id, artifacts, rows)


def _a8_adapter(handler: Callable) -> Callable:
    """Adapt A8's keyword-based handler contract to ExecutionContext.

    Resolve execution state from the same run store used by the
    orchestrator. Event-driven runs use SQLiteRunStore; legacy/manual
    runs may still use the Parquet RunStore.
    """
    def wrapped(*, run_id: str, component_id: str, task_id: str):
        from company_platform.run_control.sqlite_run_store import SQLiteRunStore
        from company_platform.run_control.run_store import RunStore

        # Event-driven execution uses SQLiteRunStore.
        sqlite_store = SQLiteRunStore()
        run = sqlite_store.get_run(run_id)

        if run is not None:
            store = sqlite_store
        else:
            # Preserve compatibility with existing manual/legacy runs
            # stored in Parquet.
            store = RunStore()
            run = store.get_run(run_id)

        tasks = store.get_tasks_for_run(run_id)
        rows = tasks[
            tasks["task_id"].astype(str) == str(task_id)
        ]

        if run is None:
            raise KeyError(f"Run not found: {run_id}")

        if rows.empty:
            raise KeyError(f"Task not found: {task_id}")

        context = ExecutionContext.from_task(
            run,
            rows.iloc[0].to_dict(),
        )

        return handler(
            context=context,
            component_id=component_id,
            task_id=task_id,
        )

    return wrapped
def _company_registry(
    context: ExecutionContext,
    component_id: str,
    task_id: str,
) -> ExecutionOutcome:

    record = register_company(
        cik=context.cik,
        source="USER",
    )

    if not record:
        return ExecutionOutcome(
            status="FAILED",
            error_message="Company registration returned no record",
        )

    if record["cik"] != context.cik:
        return ExecutionOutcome(
            status="FAILED",
            error_message="Company registry CIK mismatch",
        )

    return _gate(
        context,
        component_id,
        task_id,
        checks=5,
        failed=0,
        fatal=0,
        message=(
            f"Company registry: "
            f"{record['company_name']} "
            f"({record['cik']})"
        ),
    )

def _source_registry(
    context: ExecutionContext,
    component_id: str,
    task_id: str,
) -> ExecutionOutcome:

    mappings = enable_company_sources(
        company_id=context.company_id,
    )

    if mappings.empty:
        return ExecutionOutcome(
            status="FAILED",
            error_message=(
                f"No enabled onboarding sources for "
                f"{context.company_id}"
            ),
        )

    return _gate(
        context,
        component_id,
        task_id,
        checks=4,
        failed=0,
        fatal=0,
        message=(
            f"Source registry configured "
            f"{len(mappings)} source mappings"
        ),
    )


def _company_profile(
    context: ExecutionContext,
    component_id: str,
    task_id: str,
) -> ExecutionOutcome:
    import sys

    modules = [
        (
            "src.sec_ingestion.step16_company_intelligence_profile",
            "main",
        ),
        (
            "src.sec_ingestion.step16_1_company_intelligence_data_quality",
            "main",
        ),
    ]

    original_argv = sys.argv[:]
    sec_dir = str(_path("src", "sec_ingestion"))
    added_sec_dir = sec_dir not in sys.path

    if added_sec_dir:
        sys.path.insert(0, sec_dir)

    try:
        for module_name, fn_name in modules:
            module = importlib.import_module(module_name)
            sys.argv = [module_name]
            getattr(module, fn_name)()
    finally:
        sys.argv = original_argv
        if added_sec_dir:
            try:
                sys.path.remove(sec_dir)
            except ValueError:
                pass

    out_root = _path(
        "data", "gold", "financial_company_intelligence"
    )
    dq_root = _path(
        "data", "gold", "financial_company_intelligence_dq"
    )

    artifacts = [
        out_root / "company_intelligence_profile"
                  / "company_intelligence_profile.parquet",
        out_root / "company_intelligence_history"
                  / "company_intelligence_history.parquet",
        out_root / "company_intelligence_signals"
                  / "company_intelligence_signals.parquet",
        out_root / "company_intelligence_summary"
                  / "company_intelligence_summary.parquet",
        out_root / "company_intelligence_lineage"
                  / "company_intelligence_lineage.parquet",
        dq_root / "profile_quality_audit"
                / "profile_quality_audit.parquet",
        dq_root / "profile_dq_summary"
                / "profile_dq_summary.parquet",
    ]

    missing = [str(p) for p in artifacts if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Step 16/16.1 artifacts missing: " + ", ".join(missing)
        )

    profile = pd.read_parquet(artifacts[0])
    if profile.empty:
        raise RuntimeError("Step 16 produced an empty company profile.")

    return _artifact_dq(
        context, component_id, task_id,
        artifacts, len(profile)
    )


def _run_step_mains(modules):
    import sys

    sec_dir = str(_path("src", "sec_ingestion"))
    original_argv = sys.argv[:]
    added = sec_dir not in sys.path

    if added:
        sys.path.insert(0, sec_dir)

    try:
        for module_name in modules:
            module = importlib.import_module(module_name)
            sys.argv = [module_name]
            module.main()
    finally:
        sys.argv = original_argv
        if added:
            try:
                sys.path.remove(sec_dir)
            except ValueError:
                pass

def _form_d_ingestion(context, component_id, task_id):
    _run_step_mains([
        "src.sec_ingestion.step17_funding_capital_intelligence",
        "src.sec_ingestion.step17_1_funding_data_quality",
    ])
    return _artifact_dq(
        context, component_id, task_id,
        [
            _path("data", "gold", "funding_capital_intelligence",
                  "funding_events", "funding_events.parquet"),
            _path("data", "gold", "funding_capital_intelligence",
                  "company_funding_snapshot",
                  "company_funding_snapshot.parquet"),
        ],
    )


def _investor_ownership(context, component_id, task_id):
    _run_step_mains([
        "src.sec_ingestion.step18_investor_ownership_intelligence",
        "src.sec_ingestion.step18_1_investor_ownership_data_quality",
    ])
    return _artifact_dq(
        context, component_id, task_id,
        [
            _path(
                "data", "gold", "investor_ownership_intelligence",
                "ownership_snapshot", "ownership_snapshot.parquet"
            ),
        ],
    )


def _valuation(context, component_id, task_id):
    _run_step_mains([
        "src.sec_ingestion.step19_company_valuation_intelligence",
        "src.sec_ingestion.step19_1_company_valuation_data_quality",
    ])
    return _artifact_dq(
        context, component_id, task_id,
        [
            _path("data", "gold", "valuation_intelligence",
                  "company_valuation_snapshot",
                  "company_valuation_snapshot.parquet"),
        ],
    )


def _investment_intelligence(context, component_id, task_id):
    """
    Run Step 20 and Step 20.1 using the authoritative Step 15 financial
    scope.

    Step 20 derives its company scope directly from the Step 15 financial
    decision snapshot. No event-level CIK override is injected here because
    Step 20 produces shared company-level Gold artifacts.
    """
    previous_ciks = os.environ.pop("STEP20_CIKS", None)

    try:
        _run_step_mains([
            "src.sec_ingestion.step20_unified_investment_intelligence",
            "src.sec_ingestion.step20_1_unified_investment_intelligence_dq",
        ])
    finally:
        if previous_ciks is not None:
            os.environ["STEP20_CIKS"] = previous_ciks

    return _artifact_dq(
        context, component_id, task_id,
        [
            _path("data", "gold", "unified_investment_intelligence",
                  "investment_profile", "investment_profile.parquet"),
            _path("data", "gold", "unified_investment_intelligence",
                  "investment_signals", "investment_signals.parquet"),
            _path("data", "gold", "unified_investment_intelligence",
                  "investment_summary", "investment_summary.parquet"),
        ],
    )


def _financial_normalization(context, component_id, task_id):
    from src.sec_ingestion.build_financial_silver import process_company
    from src.sec_ingestion.canonicalize_financials import canonicalize_company

    cik = context.cik

    # 1. Build Financial Silver
    result = process_company(cik)

    silver_path = _path(
        'data', 'silver', 'financial_facts',
        f'cik={cik}', 'financial_facts.parquet'
    )
    df = pd.read_parquet(_require(silver_path, 'Financial Silver'))

    # 2. Build Annual Gold (ADD THIS)
    canonical_df = canonicalize_company(
        _path('data', 'silver'),
        _path('data', 'gold'),
        cik
    )

    annual_path = _path(
        'data', 'gold', 'financials',
        f'cik={cik.zfill(10)}', 'financials.parquet'
    )

    # 3. Validate Annual Gold exists
    _require(annual_path, 'Annual Gold')

    annual_df = pd.read_parquet(annual_path)
    if annual_df.empty:
        raise ValueError(f"Annual Gold is empty for CIK {cik}")

    # 4. Include both outputs in the task's DQ/artifact check
    return _artifact_dq(
        context,
        component_id,
        task_id,
        [silver_path, annual_path],
        len(df)
    )



def _unified_company_view(
    context,
    component_id,
    task_id,
):
    """Build and persist the canonical Unified Company View.

    The repository already has a first-class UnifiedCompanyViewBuilder which
    is responsible for converging Step 16, Step 20, company state, investment
    signals, decision signals, and company-master identity.  The integration
    adapter must delegate to that implementation rather than maintaining a
    second, partially compatible UCV writer.

    This is especially important for company_state_version: the canonical
    company-state store defines it as an INTEGER.  The builder therefore owns
    its type normalization and the adapter does not invent a string version
    such as ``0.0.0``.
    """
    from company_platform.company_intelligence.unified_company_view import (
        UnifiedCompanyViewBuilder,
    )

    builder = UnifiedCompanyViewBuilder()

    frame, output_path = builder.build_and_write()

    if frame is None or frame.empty:
        raise RuntimeError(
            "Unified company view builder produced no rows."
        )

    output_path = Path(output_path)

    if not output_path.exists():
        raise RuntimeError(
            f"Unified company view artifact was not created: {output_path}"
        )

    if "cik" not in frame.columns:
        raise RuntimeError(
            "Unified company view is missing required column 'cik'."
        )

    # The UCV is a company-grain serving artifact.  The builder creates the
    # complete current company universe, while this run records the artifact
    # produced by the convergence component.
    return _artifact_dq(
        context,
        component_id,
        task_id,
        [output_path],
        row_count=len(frame),
    )


def _company_discovery(context, component_id, task_id):
    """Run existing Step 21 discovery and Step 21.1 DQ/promotion gates."""
    _run_step_mains([
        "src.sec_ingestion.step_21_company_discovery_screening",
        "src.sec_ingestion.step_21_1_company_discovery_screening_dq",
    ])
    root = _path("data", "gold", "company_intelligence")
    dq_root = _path("data", "gold", "company_intelligence_dq")
    artifacts = [
        root / "company_search" / "company_search.parquet",
        root / "company_screening" / "company_screening.parquet",
        root / "company_intelligence_summary" / "company_intelligence_summary.parquet",
        root / "company_intelligence_lineage" / "company_intelligence_lineage.parquet",
        root / "company_intelligence_promotion" / "company_intelligence_promotion.parquet",
        dq_root / "company_intelligence_validation_checks" / "company_intelligence_validation_checks.parquet",
        dq_root / "company_intelligence_exceptions" / "company_intelligence_exceptions.parquet",
        dq_root / "company_intelligence_dq_summary" / "company_intelligence_dq_summary.parquet",
        dq_root / "company_intelligence_dq_lineage" / "company_intelligence_dq_lineage.parquet",
        dq_root / "company_intelligence_promotion" / "company_intelligence_promotion.parquet",
    ]
    missing = [str(path) for path in artifacts if not path.exists()]
    if missing:
        raise FileNotFoundError("Step 21/21.1 artifacts missing: " + ", ".join(missing))
    search = pd.read_parquet(artifacts[0])
    screening = pd.read_parquet(artifacts[1])
    if search.empty or screening.empty:
        raise ValueError("Step 21 produced empty search or screening output.")
    promotion = pd.read_parquet(artifacts[-1])
    if promotion.empty or "promotion_decision" not in promotion.columns:
        raise ValueError("Step 21.1 promotion artifact is missing its decision.")
    decision = set(promotion["promotion_decision"].astype(str).str.upper())
    if decision != {"PROMOTE"}:
        raise RuntimeError(f"Step 21.1 did not approve promotion: {sorted(decision)}")
    return _artifact_dq(context, component_id, task_id, artifacts, len(search))


def _document_ingestion(context, component_id, task_id):
    metadata = context.metadata or {}
    source_id = str(metadata.get("source_id", "")).strip().upper()
    payload = metadata.get("payload") or {}

    if source_id == "SEC":
        result = ingest_sec_filing_documents(
            cik=context.cik,
            limit=10,
            accession_number=payload.get("accession_number"),
            primary_document=payload.get("primary_document"),
        )

        manifest = Path(result["manifest_path"])
        if not manifest.is_file():
            raise FileNotFoundError(
                f"SEC document manifest was not created: {manifest}"
            )

        failed = int(result["failed_in_batch"])
        downloaded_or_skipped = int(result["downloaded_or_skipped"])

        # A failed download is a component failure, not a successful DQ pass.
        if failed:
            raise RuntimeError(
                f"SEC document ingestion had {failed} failed documents. "
                f"Manifest: {manifest}"
            )

        if downloaded_or_skipped == 0:
            raise RuntimeError(
                "SEC document ingestion produced no downloaded or "
                "verified existing documents."
            )

        return _artifact_dq(
            context,
            component_id,
            task_id,
            [manifest],
            row_count=downloaded_or_skipped,
        )

    if source_id == "SEC_FORM_D":
        filing_record = payload.get("filing_record")
        if not isinstance(filing_record, dict):
            raise ValueError(
                "SEC_FORM_D document event requires a filing_record object"
            )
        result = ingest_sec_filing_documents(
            cik=context.cik,
            limit=None,
            filing_records=[filing_record],
        )
        manifest = Path(result["manifest_path"])
        if not manifest.is_file():
            raise FileNotFoundError(
                f"SEC Form D document manifest was not created: {manifest}"
            )
        if int(result["failed_in_batch"]) > 0:
            raise RuntimeError(
                "SEC Form D document ingestion failed: "
                f"{result}"
            )
        if int(result["downloaded_or_skipped"]) == 0:
            raise RuntimeError(
                "SEC Form D document ingestion produced no verified document."
            )
        return _artifact_dq(
            context,
            component_id,
            task_id,
            [manifest],
            row_count=int(result["downloaded_or_skipped"]),
        )

    if source_id == "DOCUMENT":
        import hashlib
        import json
        import shutil
        from datetime import datetime, timezone

        payload = metadata.get("payload") or {}

        staged_path = payload.get("staged_path")
        if not staged_path:
            raise ValueError(
                "DOCUMENT event requires metadata.payload.staged_path"
            )

        source_path = Path(staged_path).expanduser().resolve()

        if not source_path.is_file():
            raise FileNotFoundError(
                f"Staged document not found: {source_path}"
            )

        filename = (
            str(payload.get("filename") or source_path.name)
            .strip()
        )

        if not filename:
            raise ValueError(
                "DOCUMENT event requires a valid filename"
            )

        # Never allow the event payload to escape the company-scoped directory.
        safe_filename = Path(filename).name

        document_id = str(
            payload.get("document_id")
            or hashlib.sha256(
                source_path.read_bytes()
            ).hexdigest()
        ).strip()

        if not document_id:
            raise ValueError("DOCUMENT event produced an empty document_id")

        document_root = _path(
            "data",
            "bronze",
            "documents",
            f"cik={context.cik}",
            document_id,
        )

        document_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        destination = document_root / safe_filename
        temp_destination = document_root / f".{safe_filename}.tmp"

        sha256 = hashlib.sha256()

        with source_path.open("rb") as src:
            with temp_destination.open("wb") as dst:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    sha256.update(chunk)
                    dst.write(chunk)

        digest = sha256.hexdigest()

        if destination.exists():
            existing_hash = hashlib.sha256(
                destination.read_bytes()
            ).hexdigest()

            if existing_hash != digest:
                temp_destination.unlink(missing_ok=True)
                raise RuntimeError(
                    "Document ID already exists with different content: "
                    f"{document_id}"
                )

            temp_destination.unlink(missing_ok=True)
        else:
            temp_destination.replace(destination)

        manifest_path = document_root / "document_manifest.parquet"

        manifest_row = pd.DataFrame([
            {
                "document_id": document_id,
                "company_id": context.company_id,
                "cik": context.cik,
                "source_id": "DOCUMENT",
                "filename": safe_filename,
                "source_path": str(source_path),
                "stored_path": str(destination),
                "local_path": str(destination),
                "status": "DOWNLOADED",
                "sha256": digest,
                "file_size_bytes": destination.stat().st_size,
                "received_at_utc": datetime.now(timezone.utc).isoformat(),
                "event_id": context.trigger_event_id,
            }
        ])

        if manifest_path.exists():
            existing = pd.read_parquet(manifest_path)

            existing = existing[
                existing["document_id"].astype(str) != document_id
            ]

            manifest = pd.concat(
                [existing, manifest_row],
                ignore_index=True,
            )
        else:
            manifest = manifest_row

        manifest.to_parquet(
            manifest_path,
            index=False,
        )

        return _artifact_dq(
            context,
            component_id,
            task_id,
            [destination, manifest_path],
            row_count=1,
        )

    raise ValueError(f"Unsupported document source_id: {source_id}")

def _document_classification(context, component_id, task_id):
    result = classify_documents_for_company(
        cik=context.cik,
        document_id=_event_document_id(context),
    )

    if int(result["failed"]) > 0:
        raise RuntimeError(
            f"Document classification failed: {result}"
        )

    classified = int(result["classified"])
    skipped = int(result["skipped"])

    if classified == 0 and skipped == 0:
        raise RuntimeError(
            "Document classification produced no classified "
            "or skipped documents"
        )

    return _artifact_dq(
        context,
        component_id,
        task_id,
        [],
        row_count=classified + skipped,
    )

def _document_understanding(context, component_id, task_id):
    result = understand_documents_for_company(
        cik=context.cik,
        document_id=_event_document_id(context),
    )

    if int(result["failed"]) > 0:
        raise RuntimeError(
            f"Document understanding failed: {result}"
        )

    processed = (
        int(result["understood"])
        + int(result["skipped"])
    )

    if processed == 0:
        raise RuntimeError(
            "Document understanding produced "
            "no processed documents"
        )

    return _artifact_dq(
        context,
        component_id,
        task_id,
        [],
        row_count=processed,
    )

def _structured_event_persistence(
    context,
    component_id,
    task_id,
):
    from company_platform.document_understanding import (
        understand_documents_for_company,
    )

    # Re-read the deterministic understanding result.
    # This keeps persistence independent from the in-memory
    # execution result of the previous component.
    understanding = understand_documents_for_company(
        cik=context.cik,
        document_id=_event_document_id(context),
    )

    if int(understanding["failed"]) > 0:
        raise RuntimeError(
            f"Cannot persist structured events because "
            f"document understanding failed: {understanding}"
        )

    payload = (context.metadata or {}).get("payload") or {}
    additional_facts = {}
    document_id = payload.get("document_id")
    funding_facts = payload.get("funding_facts")
    if document_id and isinstance(funding_facts, dict):
        additional_facts[str(document_id)] = funding_facts

    persistence = persist_structured_events(
        cik=context.cik,
        company_id=context.company_id,
        source_event_id=context.trigger_event_id,
        source_run_id=context.run_id,
        results=understanding["results"],
        additional_facts=additional_facts,
    )

    if int(persistence["failed"]) > 0:
        raise RuntimeError(
            f"Structured event persistence failed: {persistence}"
        )

    processed = int(persistence["processed"])

    # A document with no supported business event is valid.
    if processed == 0:
        return _artifact_dq(
            context,
            component_id,
            task_id,
            [],
        )

    return _artifact_dq(
        context,
        component_id,
        task_id,
        [],
        row_count=processed,
    )

def _company_state_projection(
    context,
    component_id,
    task_id,
):
    projector = CompanyStateProjector()

    result = projector.project_company(
        context.company_id
    )

    if result.get("status") == "NO_DATA":
        return _artifact_dq(
            context,
            component_id,
            task_id,
            [],
        )

    if not result.get("projected"):
        raise RuntimeError(
            f"Company state projection failed: {result}"
        )

    return _artifact_dq(
        context,
        component_id,
        task_id,
        [],
        row_count=1,
    )
def _company_state_investment_intelligence(
    context,
    component_id,
    task_id,
):
    intelligence = (
        UnifiedInvestmentIntelligence()
    )

    result = intelligence.project_company(
        context.company_id
    )

    if result.get("status") == "NO_DATA":
        return _artifact_dq(
            context,
            component_id,
            task_id,
            [],
        )

    output_path = (
        intelligence.write_company_artifact(
            context.company_id
        )
    )

    if output_path is None:
        raise RuntimeError(
            "Unified investment intelligence "
            "artifact was not created."
        )

    return _artifact_dq(
        context,
        component_id,
        task_id,
        [output_path],
        row_count=1,
    )
HANDLERS: dict[str, Callable] = {
    'company_registry': _a8_adapter(_company_registry),
    'source_registry': _a8_adapter(_source_registry),

    'sec_ingestion': _a8_adapter(_sec_ingestion),
    'financial_normalization': _a8_adapter(_financial_normalization),
    'quarter_derivation': _a8_adapter(_quarter_derivation),
    'financial_features': _a8_adapter(_financial_features),
    'financial_decision': _a8_adapter(_financial_decision),
    'company_profile': _a8_adapter(_company_profile),
    'form_d_ingestion': _a8_adapter(_form_d_ingestion),
    'investor_ownership': _a8_adapter(_investor_ownership),
    'valuation': _a8_adapter(_valuation),
    'investment_intelligence': _a8_adapter(_investment_intelligence),
    'funding_capital_intelligence': _a8_adapter(_form_d_ingestion),
    'investor_ownership_intelligence': _a8_adapter(_investor_ownership),
    'valuation_intelligence': _a8_adapter(_valuation),
    'unified_investment_intelligence': _a8_adapter(_investment_intelligence),
    'company_discovery': _a8_adapter(_company_discovery),
    'unified_company_view': _a8_adapter(_unified_company_view),
    'document_ingestion': _a8_adapter(_document_ingestion),
    "document_extraction": _a8_adapter(_document_extraction),
    "document_classification": _a8_adapter(
    _document_classification),
    "document_understanding": _a8_adapter(
    _document_understanding),
    "structured_event_persistence": _a8_adapter(
    _structured_event_persistence),
    "company_state_projection": _a8_adapter(
    _company_state_projection),
    "company_state_investment_intelligence": _a8_adapter(
    _company_state_investment_intelligence),
    "investment_intelligence": _a8_adapter(
    _investment_intelligence),
}
