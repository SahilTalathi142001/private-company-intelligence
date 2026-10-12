
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT
SRC = REPO / "src" / "sec_ingestion"

FILES = [
    SRC / "step17_funding_capital_intelligence.py",
    SRC / "step17_1_funding_data_quality.py",
    SRC / "step18_1_investor_ownership_data_quality.py",
    SRC / "step19_1_company_valuation_data_quality.py",
    SRC / "step20_unified_investment_intelligence.py",
    SRC / "step20_1_unified_investment_intelligence_dq.py",
]

SCOPE_IMPORT = (
    "from company_platform.registry.active_company_scope import "
    "load_active_company_ciks"
)

DYNAMIC_EXPR = "[str(cik).zfill(10) for cik in load_active_company_ciks()]"

LEGACY_FIVE = {
    "0000320193",
    "0000789019",
    "0001018724",
    "0001045810",
    "0001652044",
}


def add_scope_import(text: str) -> str:
    if "load_active_company_ciks" in text:
        return text

    old = "from company_platform.registry.active_company_scope import (\n    assert_company_scope,\n)"
    if old in text:
        return text.replace(old, SCOPE_IMPORT, 1)

    old = "from company_platform.registry.active_company_scope import assert_company_scope"
    if old in text:
        return text.replace(old, SCOPE_IMPORT, 1)

    # Put the import after the pandas import when this module did not yet
    # import the scope helper (notably Step 18.1).
    marker = "import pandas as pd\n"
    if marker not in text:
        raise RuntimeError("Could not find stable import anchor for scope helper.")
    return text.replace(marker, marker + SCOPE_IMPORT + "\n", 1)


def replace_scope_calls(text: str) -> str:
    text = text.replace("assert_company_scope(50)", DYNAMIC_EXPR)
    return text


def replace_legacy_company_lists(text: str) -> str:
    """
    Replace only top-level COMPANIES assignments whose values are exactly the
    legacy five-company set. This intentionally refuses unrelated lists.
    """
    tree = ast.parse(text)

    replacements = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue

        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(isinstance(t, ast.Name) and t.id == "COMPANIES" for t in targets):
            continue

        value = node.value
        if not isinstance(value, (ast.List, ast.Tuple, ast.Set)):
            continue

        values = []
        ok = True
        for elt in value.elts:
            if not isinstance(elt, ast.Constant) or not isinstance(elt.value, str):
                ok = False
                break
            values.append(elt.value.zfill(10))

        if ok and set(values) == LEGACY_FIVE and len(values) == 5:
            replacements.append((node.lineno, node.end_lineno))

    if not replacements:
        return text

    lines = text.splitlines(keepends=True)
    for start, end in reversed(replacements):
        lines[start - 1:end] = [f"COMPANIES = configured_companies()\n"]

    return "".join(lines)


def ensure_configured_companies(text: str) -> str:
    if "def configured_companies(" in text:
        # Normalize existing implementation after scope-call replacement.
        return text

    # Step 18.1 / Step 19.1 have a direct legacy list and need their own
    # resolver so importing the module always uses the authoritative scope.
    anchor = "DQ_VERSION = "
    idx = text.find(anchor)
    if idx < 0:
        raise RuntimeError("Could not find DQ_VERSION anchor.")

    line_end = text.find("\n", idx)
    if line_end < 0:
        raise RuntimeError("Malformed DQ_VERSION declaration.")

    block = (
        "\n\n"
        "def configured_companies() -> list[str]:\n"
        "    \"\"\"Resolve the authoritative active production company scope.\"\"\"\n"
        "    companies = [str(cik).zfill(10) for cik in load_active_company_ciks()]\n"
        "    if not companies:\n"
        "        raise ValueError(\"Active company execution scope is empty.\")\n"
        "    return sorted(set(companies))\n"
    )
    return text[:line_end + 1] + block + text[line_end + 1:]


def normalize_configured_functions(text: str) -> str:
    # Convert the known hard-coded resolver body to a registry-backed resolver.
    pattern = re.compile(
        r"(def configured_companies\(\)\s*->\s*(?:List\[str\]|list\[str\])\s*:\s*\n"
        r"(?:[ \t]+.*\n)*?[ \t]+)(?:return )?"
        r"assert_company_scope\(50\)",
        re.MULTILINE,
    )
    text = pattern.sub(
        lambda m: m.group(1) + "return " + DYNAMIC_EXPR,
        text,
    )
    return text


def patch_file(path: Path) -> bool:
    if not path.exists():
        raise FileNotFoundError(f"Required production file not found: {path}")

    original = path.read_text(encoding="utf-8")
    text = original

    text = add_scope_import(text)
    text = replace_scope_calls(text)
    text = ensure_configured_companies(text)
    text = normalize_configured_functions(text)
    text = replace_legacy_company_lists(text)

    # Re-run replacement after list transformation.
    text = replace_scope_calls(text)

    ast.parse(text, filename=str(path))

    if "assert_company_scope(50)" in text:
        raise RuntimeError(f"Legacy fixed-count scope remains in {path}")

    if text != original:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def main() -> None:
    changed = []
    for path in FILES:
        if patch_file(path):
            changed.append(str(path.relative_to(REPO)))

    # Final production guard: no fixed 50-company scope in the patched layers.
    violations = []
    for path in FILES:
        if path.exists():
            text = path.read_text(encoding="utf-8")
            if "assert_company_scope(50)" in text:
                violations.append(str(path))
    if violations:
        raise RuntimeError("Fixed-count scope remains:\n" + "\n".join(violations))

    print(f"PATCHED {len(changed)} production modules")
    for path in changed:
        print(path)
    print("SCOPE_POLICY OK")


if __name__ == "__main__":
    main()
