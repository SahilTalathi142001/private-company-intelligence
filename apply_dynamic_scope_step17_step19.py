from __future__ import annotations

from pathlib import Path
import re

REPO_ROOT = Path(__file__).resolve().parent
SRC = REPO_ROOT / "src" / "sec_ingestion"


def replace_once(path: Path, pattern: str, replacement: str, *, flags: int = 0) -> bool:
    text = path.read_text(encoding="utf-8")
    new, count = re.subn(pattern, replacement, text, count=1, flags=flags)
    if count != 1:
        return False
    path.write_text(new, encoding="utf-8")
    return True


def ensure_import(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    if "from company_platform.registry.active_company_scope import load_active_company_ciks" in text:
        return

    marker = "import pandas as pd\n"
    if marker not in text:
        raise RuntimeError(f"Cannot locate pandas import marker in {path}")
    text = text.replace(
        marker,
        marker + "from company_platform.registry.active_company_scope import load_active_company_ciks\n",
        1,
    )
    path.write_text(text, encoding="utf-8")


def patch_step19_1() -> None:
    path = SRC / "step19_1_company_valuation_data_quality.py"
    if not path.exists():
        raise FileNotFoundError(path)

    ensure_import(path)
    text = path.read_text(encoding="utf-8")

    # Replace any legacy literal/list company universe with a runtime resolver.
    patterns = [
        r'COMPANIES\s*=\s*\[[\s\S]*?\]\n\nOUTPUT_ROOT',
        r'COMPANIES\s*=\s*assert_company_scope\(\s*\d+\s*\)',
        r'COMPANIES\s*=\s*configured_companies\(\)',
    ]
    replacement = (
        'def configured_companies() -> list[str]:\n'
        '    """Resolve the authoritative active production company universe."""\n'
        '    ciks = [str(c).zfill(10) for c in load_active_company_ciks()]\n'
        '    if not ciks:\n'
        '        raise ValueError("Active company execution scope is empty.")\n'
        '    return sorted(set(ciks))\n\n'
        'COMPANIES = configured_companies()\n\n'
        'OUTPUT_ROOT'
    )

    changed = False
    for pattern in patterns:
        new, count = re.subn(pattern, replacement, text, count=1)
        if count:
            text = new
            changed = True
            break

    if not changed:
        # The file may already contain a dynamic resolver.
        if "COMPANIES = configured_companies()" not in text:
            raise RuntimeError(
                "Could not locate Step 19.1 company-scope declaration; "
                "refusing unsafe blind edit."
            )

    path.write_text(text, encoding="utf-8")


def patch_step17() -> None:
    path = SRC / "step17_funding_capital_intelligence.py"
    if not path.exists():
        raise FileNotFoundError(path)

    ensure_import(path)
    text = path.read_text(encoding="utf-8")

    # Step 17 has had several historical forms. Normalize its configured
    # company resolver without changing ingestion semantics.
    resolver = (
        'def configured_companies() -> list[str]:\n'
        '    """Resolve the authoritative active production company universe."""\n'
        '    ciks = [str(c).zfill(10) for c in load_active_company_ciks()]\n'
        '    if not ciks:\n'
        '        raise ValueError("Active company execution scope is empty.")\n'
        '    return sorted(set(ciks))\n'
    )

    # Replace an existing configured_companies function body.
    pattern = r'def configured_companies\(\)\s*->\s*List\[str\]:[\s\S]*?(?=\n\n(?:SEC_DATA_BASE|COMPANIES\s*=))'
    new, count = re.subn(pattern, resolver.rstrip(), text, count=1)
    if count:
        text = new
    else:
        # If no function exists, replace the direct hard-coded assignment.
        pattern2 = r'COMPANIES\s*=\s*assert_company_scope\(\s*\d+\s*\)'
        new, count = re.subn(pattern2, "COMPANIES = configured_companies()", text, count=1)
        if count == 0:
            raise RuntimeError(
                "Could not locate Step 17 company-scope declaration; "
                "refusing unsafe blind edit."
            )
        text = new

    # Ensure the module-level assignment uses the resolver.
    text = re.sub(
        r'COMPANIES\s*=\s*assert_company_scope\(\s*\d+\s*\)',
        "COMPANIES = configured_companies()",
        text,
    )

    path.write_text(text, encoding="utf-8")


def patch_step20_1() -> None:
    path = SRC / "step20_1_unified_investment_intelligence_dq.py"
    if not path.exists():
        raise FileNotFoundError(path)

    ensure_import(path)
    text = path.read_text(encoding="utf-8")

    text = re.sub(
        r'COMPANIES\s*=\s*\[\s*\]',
        'COMPANIES: list[str] = []',
        text,
        count=1,
    )

    # Main currently resolves scope using assert_company_scope(50). Replace
    # only the executable resolution, preserving explicit scoped-run support.
    text, count = re.subn(
        r'COMPANIES\s*=\s*\[\s*normalize_cik\(cik\)\s*for\s+cik\s+in\s+assert_company_scope\(\s*\d+\s*\)\s*\]',
        'COMPANIES = [str(cik).zfill(10) for cik in load_active_company_ciks()]',
        text,
        count=1,
    )
    if count == 0:
        text, count = re.subn(
            r'COMPANIES\s*=\s*assert_company_scope\(\s*\d+\s*\)',
            'COMPANIES = [str(cik).zfill(10) for cik in load_active_company_ciks()]',
            text,
            count=1,
        )
    if count == 0 and "load_active_company_ciks()" not in text:
        raise RuntimeError(
            "Could not locate Step 20.1 fixed-count scope resolution; "
            "refusing unsafe blind edit."
        )

    text = text.replace(
        'from company_platform.registry.active_company_scope import assert_company_scope\n',
        '',
    )
    path.write_text(text, encoding="utf-8")


def patch_step20() -> None:
    path = SRC / "step20_unified_investment_intelligence.py"
    if not path.exists():
        raise FileNotFoundError(path)

    ensure_import(path)
    text = path.read_text(encoding="utf-8")

    # Step 20 historically resolved a fixed count in main. Replace only that
    # runtime resolution.
    text, count = re.subn(
        r'COMPANIES\s*=\s*assert_company_scope\(\s*\d+\s*\)',
        'COMPANIES = [str(cik).zfill(10) for cik in load_active_company_ciks()]',
        text,
        count=1,
    )
    if count == 0 and "load_active_company_ciks()" not in text:
        raise RuntimeError(
            "Could not locate Step 20 fixed-count scope resolution; "
            "refusing unsafe blind edit."
        )

    text = text.replace(
        'from company_platform.registry.active_company_scope import assert_company_scope\n',
        '',
    )
    path.write_text(text, encoding="utf-8")


def patch_step19_1_all_fixed_assignments() -> None:
    path = SRC / "step19_1_company_valuation_data_quality.py"
    text = path.read_text(encoding="utf-8")
    text = re.sub(
        r'COMPANIES\s*=\s*assert_company_scope\(\s*\d+\s*\)',
        'COMPANIES = configured_companies()',
        text,
    )
    path.write_text(text, encoding="utf-8")


def main() -> None:
    patch_step17()
    patch_step19_1()
    patch_step20()
    patch_step20_1()
    patch_step19_1_all_fixed_assignments()

    print("Permanent execution-scope patch applied.")
    print("Step 17, Step 19.1, Step 20 and Step 20.1 now resolve")
    print("the active company universe from the platform registry.")
    print("No company count, CIK list, or expected row count is hard-coded.")
    print("")
    print("Next validation:")
    print("  python -m compileall src/sec_ingestion")
    print("  python -m pytest tests/integration -q")
    print("  python -m pytest -q")


if __name__ == "__main__":
    main()
