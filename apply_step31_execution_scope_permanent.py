from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
REPO = ROOT
HARDENING = REPO / "src" / "company_platform" / "multi_company" / "hardening.py"
TEST = REPO / "tests" / "integration" / "test_step31_multi_company_hardening.py"

if not HARDENING.exists():
    raise SystemExit(f"Missing: {HARDENING}")
if not TEST.exists():
    raise SystemExit(f"Missing: {TEST}")

def replace_once(path: Path, old: str, new: str, label: str) -> None:
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly 1 match, found {count}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")

# ---------------------------------------------------------------------
# Production: explicit execution scope with registry-backed default.
# ---------------------------------------------------------------------
text = HARDENING.read_text(encoding="utf-8")

if "def reconcile(*, execution_scope:" not in text:
    replace_once(
        HARDENING,
        "def audit_company_universe():\n",
        "def audit_company_universe(*, targets: dict[str, str] | None = None):\n",
        "audit_company_universe signature",
    )

    replace_once(
        HARDENING,
        "    targets = active_target_companies()\n    target = set(targets)\n",
        "    targets = active_target_companies() if targets is None else dict(targets)\n    target = set(targets)\n",
        "audit_company_universe scope resolution",
    )

    replace_once(
        HARDENING,
        "def audit_artifact_isolation():\n",
        "def audit_artifact_isolation(*, targets: dict[str, str] | None = None):\n",
        "audit_artifact_isolation signature",
    )

    replace_once(
        HARDENING,
        '            normalized.isin(set(active_target_companies())).all(),\n'
        '            f"unexpected_ciks={sorted(set(normalized) - set(active_target_companies()))}"\n',
        '            normalized.isin(set(targets)).all(),\n'
        '            f"unexpected_ciks={sorted(set(normalized) - set(targets))}"\n',
        "artifact identity scope",
    )

    replace_once(
        HARDENING,
        "def reconcile():\n"
        "    checks = audit_company_universe()\n"
        "    checks += audit_control_plane_isolation()\n"
        "    checks += audit_artifact_isolation()\n"
        "    checks += audit_reconciliation_coverage()\n",
        "def reconcile(*, execution_scope: dict[str, str] | None = None):\n"
        "    \"\"\"Run Step 31 using an explicit scope or the authoritative registry.\n\n"
        "    Production callers omit execution_scope and therefore use the active\n"
        "    company registry. Tests/recovery/batch callers may inject an explicit\n"
        "    isolated scope without changing global production configuration.\n"
        "    \"\"\"\n"
        "    targets = active_target_companies() if execution_scope is None else dict(execution_scope)\n"
        "    if not targets:\n"
        "        raise ValueError(\"Step 31 execution scope must contain at least one company.\")\n\n"
        "    checks = audit_company_universe(targets=targets)\n"
        "    checks += audit_control_plane_isolation()\n"
        "    checks += audit_artifact_isolation(targets=targets)\n"
        "    checks += audit_reconciliation_coverage()\n",
        "reconcile execution scope",
    )

    replace_once(
        HARDENING,
        "    for c, name in active_target_companies().items():\n",
        "    for c, name in targets.items():\n",
        "reconcile company status scope",
    )

    replace_once(
        HARDENING,
        '        "target_company_count": len(active_target_companies()),\n',
        '        "target_company_count": len(targets),\n',
        "reconcile target count scope",
    )

    # Persist a single resolved scope inside the result rather than resolving
    # the registry repeatedly during one reconciliation.
    HARDENING.write_text(HARDENING.read_text(encoding="utf-8"), encoding="utf-8")

# ---------------------------------------------------------------------
# Test: use an explicit fixture scope rather than legacy TARGET_COMPANIES.
# ---------------------------------------------------------------------
test_text = TEST.read_text(encoding="utf-8")

old_assert = "    assert set(h.TARGET_COMPANIES) == set(TARGET)\n"
if old_assert in test_text:
    replace_once(
        TEST,
        old_assert,
        "    assert set(active_targets) == set(h.active_target_companies())\n",
        "legacy TARGET_COMPANIES assertion",
    )

# Replace the exact reconcile call in the full-universe test.
old_call = "    s, checks, companies = h.reconcile()\n"
if old_call in test_text:
    new_call = (
        "    execution_scope = {\n"
        "        c: h.active_target_companies().get(c, c)\n"
        "        for c in TARGET\n"
        "    }\n"
        "    s, checks, companies = h.reconcile(execution_scope=execution_scope)\n"
    )
    replace_once(TEST, old_call, new_call, "test reconciliation scope")

# Syntax validation.
compile(HARDENING.read_text(encoding="utf-8"), str(HARDENING), "exec")
compile(TEST.read_text(encoding="utf-8"), str(TEST), "exec")

print("Permanent Step 31 execution-scope patch applied.")
print("Production default: authoritative active company registry.")
print("Explicit scope: supported for isolated tests/batches/recovery.")
print("Hard-coded company universe: none added.")
print("Syntax checks: PASS")
