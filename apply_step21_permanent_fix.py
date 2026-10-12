from pathlib import Path
import ast
import shutil

ROOT = Path(__file__).resolve().parent

STEP21 = ROOT / "src" / "sec_ingestion" / "step_21_company_discovery_screening.py"
STEP21_DQ = ROOT / "src" / "sec_ingestion" / "step_21_1_company_discovery_screening_dq.py"

if not STEP21.exists():
    STEP21 = ROOT / "src" / "sec_ingestion" / "step21_company_discovery_screening.py"
if not STEP21_DQ.exists():
    STEP21_DQ = ROOT / "src" / "sec_ingestion" / "step21_1_company_discovery_screening_dq.py"

def backup(path: Path) -> None:
    bak = path.with_suffix(path.suffix + ".pre_step21_fix.bak")
    if not bak.exists():
        shutil.copy2(path, bak)

def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly 1 match, found {count}")
    return text.replace(old, new, 1)

def patch_step21(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    backup(path)

    old = '''        decision = upper(r.get("financial_decision"))
        trajectory = upper(r.get("financial_trajectory_20"))
        posture = upper(r.get("unified_posture")) or "INSUFFICIENT_DATA"
        coverage = upper(r.get("data_coverage")) or "NO_DATA"
        funding = upper(r.get("funding_status")) or "NO_DATA"
        investor = upper(r.get("investor_coverage_status")) or "NO_DATA"
        ownership = upper(r.get("ownership_status")) or "NO_DATA"
        valuation = upper(r.get("valuation_status")) or "NO_DATA"
        quality = upper(r.get("profile_quality")) or "LIMITED"
        risk = upper(r.get("financial_risk_signal"))
'''
    new = '''        # The merge uses suffixes=("_20", "_16"). Explicit suffixes are
        # mandatory so source ownership cannot silently change.
        decision = upper(r.get("financial_decision_20"))
        trajectory = upper(r.get("financial_trajectory_20"))
        posture = upper(r.get("unified_posture_20")) or "INSUFFICIENT_DATA"
        coverage = upper(r.get("data_coverage_20")) or "NO_DATA"
        funding = upper(r.get("funding_status_20")) or "NO_DATA"
        investor = upper(r.get("investor_coverage_status_20")) or "NO_DATA"
        ownership = upper(r.get("ownership_status_20")) or "NO_DATA"
        valuation = upper(r.get("valuation_status_20")) or "NO_DATA"
        quality = upper(r.get("profile_quality_16")) or "LIMITED"
        risk = upper(r.get("financial_risk_signal_16"))
'''
    text = replace_once(text, old, new, "Step21 source-selection block")

    replacements = {
        'number(r.get("financial_score"))': 'number(r.get("financial_score_20"))',
        'upper(r.get("financial_health"))': 'upper(r.get("financial_health_16"))',
        'upper(r.get("financial_risk_signal"))': 'upper(r.get("financial_risk_signal_16"))',
        'upper(r.get("peer_position"))': 'upper(r.get("peer_position_16"))',
        'upper(r.get("peer_population_class"))': 'upper(r.get("peer_population_class_16"))',
        'upper(r.get("decision_confidence"))': 'upper(r.get("decision_confidence_16"))',
        'integer(r.get("decision_priority"))': 'integer(r.get("decision_priority_16"))',
        'upper(r.get("decision_reason_code"))': 'upper(r.get("decision_reason_code_16"))',
        'upper(r.get("company_archetype"))': 'upper(r.get("company_archetype_16"))',
        'integer(r.get("funding_event_count"))': 'integer(r.get("funding_event_count_20"))',
        'number(r.get("sec_exempt_offering_capital_usd"))': 'number(r.get("sec_exempt_offering_capital_usd_20"))',
        'upper(r.get("funding_activity_status"))': 'upper(r.get("funding_activity_status_20"))',
        'r.get("last_funding_date")': 'r.get("last_funding_date_20")',
        'integer(r.get("named_investor_count"))': 'integer(r.get("named_investor_count_20"))',
        'r.get("ownership_pct_available")': 'r.get("ownership_pct_available_20")',
        'number(r.get("latest_valuation_amount_usd"))': 'number(r.get("latest_valuation_amount_usd_20"))',
        'r.get("latest_valuation_date")': 'r.get("latest_valuation_date_20")',
        'number(r.get("sec_capital_context_usd"))': 'number(r.get("sec_capital_context_usd_20"))',
        'boolean(r.get("sec_capital_treated_as_valuation"))': 'boolean(r.get("sec_capital_treated_as_valuation_20"))',
        'boolean(r.get("valuation_fabrication_prevented"))': 'boolean(r.get("valuation_fabrication_prevented_20"))',
        'text(r.get("profile_version"))': 'text(r.get("profile_version_16"))',
        'text(r.get("step20_version"))': 'text(r.get("step20_version_20"))',
    }
    for old_expr, new_expr in replacements.items():
        text = text.replace(old_expr, new_expr)

    ast.parse(text, filename=str(path))
    path.write_text(text, encoding="utf-8")

def patch_step21_dq(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    backup(path)

    old = '''    required_fields = {
        "cik",
        "company_name",
        "financial_decision",
        "financial_trajectory",
        "financial_score",
        "funding_status",
        "investor_coverage_status",
        "ownership_status",
        "valuation_status",
        "data_coverage",
        "unified_posture",
        "screening_eligibility",
        "is_strong_positive",
        "is_positive",
        "is_improving",
        "is_low_risk",
        "has_funding_data",
        "has_investor_data",
        "has_valuation_data",
        "has_complete_data",
    }
'''
    new = '''    # Step 21 lineage is a field-level provenance register. Its contract is
    # defined by Step 21's lineage() builder, not by physical artifact columns.
    required_fields = {
        "cik",
        "company_name",
        "ticker",
        "industry",
        "founded_year",
        "headquarters",
        "financial_decision",
        "financial_trajectory",
        "financial_score",
        "funding_status",
        "investor_coverage_status",
        "ownership_status",
        "valuation_status",
        "latest_valuation_amount_usd",
        "sec_capital_context_usd",
        "data_coverage",
        "unified_posture",
        "screening_eligibility",
        "is_strong_positive",
        "is_positive",
        "is_improving",
        "is_low_risk",
        "has_funding_data",
        "has_investor_data",
        "has_valuation_data",
        "has_complete_data",
        "company_state_status",
        "company_state_version",
        "observed_funding_event_count",
        "latest_funding_date",
        "latest_total_offering_amount_usd",
        "latest_amount_sold_usd",
        "cumulative_offering_amount_usd",
        "cumulative_amount_sold_usd",
        "investment_signal_count",
        "investment_signal_types",
        "capital_activity_detected",
        "recurring_capital_activity",
        "capital_raise_execution",
        "event_decision_posture",
        "event_decision_confidence",
        "event_decision_id",
        "source_event_id",
        "source_run_id",
        "source_document_id",
        "source_document_sha256",
        "unified_company_view_version",
    }
'''
    text = replace_once(text, old, new, "Step21.1 lineage contract")
    ast.parse(text, filename=str(path))
    path.write_text(text, encoding="utf-8")

for p in (STEP21, STEP21_DQ):
    if not p.exists():
        raise FileNotFoundError(f"Cannot locate target module: {p}")

patch_step21(STEP21)
patch_step21_dq(STEP21_DQ)

final21 = STEP21.read_text(encoding="utf-8")
for forbidden in [
    'r.get("financial_decision")',
    'r.get("profile_quality")',
    'r.get("financial_score")',
    'r.get("financial_health")',
    'r.get("decision_confidence")',
]:
    if forbidden in final21:
        raise RuntimeError(f"Unqualified merged-field access remains: {forbidden}")

print("STEP21 PERMANENT FIX APPLIED")
print(f"Step21   : {STEP21}")
print(f"Step21.1 : {STEP21_DQ}")
print("Backups  : *.pre_step21_fix.bak")
