from __future__ import annotations
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT
HARDENING = REPO / "src" / "company_platform" / "multi_company" / "hardening.py"

if not HARDENING.exists():
    raise SystemExit(f"Missing: {HARDENING}")

text = HARDENING.read_text(encoding="utf-8")

old = """def audit_artifact_isolation(*, targets: dict[str, str] | None = None):
    checks = []
    for name, path in [
"""

new = """def audit_artifact_isolation(*, targets: dict[str, str] | None = None):
    # Standalone callers use the authoritative production scope.
    # Reconciliation callers may inject an explicit isolated scope.
    targets = active_target_companies() if targets is None else dict(targets)
    if not targets:
        raise ValueError("Artifact isolation requires a non-empty execution scope.")

    checks = []
    for name, path in [
"""

if old in text:
    backup = HARDENING.with_suffix(HARDENING.suffix + ".bak_scope_api")
    if not backup.exists():
        backup.write_text(text, encoding="utf-8")
    text = text.replace(old, new, 1)
elif "targets = active_target_companies() if targets is None else dict(targets)" not in text:
    raise RuntimeError("Expected audit_artifact_isolation signature not found.")

compile(text, str(HARDENING), "exec")
HARDENING.write_text(text, encoding="utf-8")

print("Step 31 scope API fix applied.")
print("audit_artifact_isolation(): registry-backed default restored.")
print("Explicit targets remain supported for reconcile() execution scopes.")
print("No company list/count hardcoding added.")
print("Syntax check: PASS")
