from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
REPO = ROOT

UCV = REPO / "src" / "company_platform" / "company_intelligence" / "unified_company_view.py"

if not UCV.exists():
    raise SystemExit(f"UCV file not found: {UCV}")

text = UCV.read_text(encoding="utf-8")

# Remove only consecutive duplicate occurrences of the exact argument line.
line = "        expected_universe_ciks: set[str] | None = None,\n"

count = text.count(line)
if count < 2:
    print(f"UCV already has {count} expected_universe_ciks argument(s); no duplicate removal needed.")
else:
    new_text = re.sub(
        r"(^[ \t]*expected_universe_ciks: set\[str\] \| None = None,\n)([ \t]*expected_universe_ciks: set\[str\] \| None = None,\n)+",
        r"\1",
        text,
        flags=re.MULTILINE,
    )

    if new_text == text:
        raise RuntimeError(
            "Found multiple expected_universe_ciks arguments, but they were not consecutive. "
            "Inspect the function definition before modifying it."
        )

    backup = UCV.with_suffix(UCV.suffix + ".bak_duplicate_arg")
    if not backup.exists():
        backup.write_text(text, encoding="utf-8")

    UCV.write_text(new_text, encoding="utf-8")
    print("Removed duplicate expected_universe_ciks argument.")
    print(f"Backup: {backup}")

# Validate syntax immediately.
compile(UCV.read_text(encoding="utf-8"), str(UCV), "exec")
print("UCV syntax check: PASS")
