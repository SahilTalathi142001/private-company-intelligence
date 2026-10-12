
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parent
SRC = REPO / "src" / "sec_ingestion"

TARGETS = [
    SRC / "step19_company_valuation_intelligence.py",
    SRC / "step19_1_company_valuation_data_quality.py",
    SRC / "step20_1_unified_investment_intelligence_dq.py",
]

IMPORT_LINE = (
    "from company_platform.registry.active_company_scope import "
    "load_active_company_ciks"
)

RESOLVER = """def configured_companies() -> list[str]:
    """ + '"""Resolve the authoritative active production company scope."""' + """
    ciks = [str(cik).zfill(10) for cik in load_active_company_ciks()]
    if not ciks:
        raise ValueError("Active company execution scope is empty.")
    return sorted(set(ciks))
"""


def ensure_import(text: str) -> str:
    if "load_active_company_ciks" in text:
        return text
    marker = "import pandas as pd\n"
    if marker not in text:
        raise RuntimeError("No stable pandas import anchor.")
    return text.replace(marker, marker + IMPORT_LINE + "\n", 1)


def ensure_resolver(text: str) -> str:
    if "def configured_companies() ->" in text:
        tree = ast.parse(text)
        funcs = [
            n for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef)
            and n.name == "configured_companies"
            and len(n.args.args) == 0
        ]
        if funcs:
            node = funcs[0]
            lines = text.splitlines(keepends=True)
            lines[node.lineno - 1:node.end_lineno] = RESOLVER.splitlines(keepends=True)
            return "".join(lines)
        return text

    marker = "COMPANIES ="
    idx = text.find(marker)
    if idx < 0:
        raise RuntimeError("Could not locate company-scope assignment.")
    return text[:idx] + RESOLVER + "\n" + text[idx:]


def patch(path: Path) -> bool:
    if not path.exists():
        raise FileNotFoundError(path)

    original = path.read_text(encoding="utf-8")
    text = ensure_import(original)
    text = ensure_resolver(text)

    text = text.replace("assert_company_scope(50)", "configured_companies()")
    text = text.replace(
        "COMPANIES = [\n    normalize_cik(cik)\n    for cik in configured_companies()\n]",
        "COMPANIES = configured_companies()",
    )

    ast.parse(text, filename=str(path))

    if "assert_company_scope(50)" in text:
        raise RuntimeError(f"Fixed-count scope remains in {path}")

    if text != original:
        path.write_text(text, encoding="utf-8")
        return True
    return False


def main() -> None:
    changed = []
    for path in TARGETS:
        if patch(path):
            changed.append(str(path.relative_to(REPO)))

    print(f"PATCHED {len(changed)} production modules")
    for item in changed:
        print(item)
    print("STEP19/20.1 DYNAMIC SCOPE OK")


if __name__ == "__main__":
    main()
