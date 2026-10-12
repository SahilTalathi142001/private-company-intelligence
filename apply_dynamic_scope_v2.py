from __future__ import annotations

import ast
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
SRC = ROOT / 'src' / 'sec_ingestion'
SCOPE_IMPORT = 'from company_platform.registry.active_company_scope import load_active_company_ciks'

FILES = [
    'step17_funding_capital_intelligence.py',
    'step17_1_funding_data_quality.py',
    'step19_1_company_valuation_data_quality.py',
    'step20_unified_investment_intelligence.py',
    'step20_1_unified_investment_intelligence_dq.py',
]


def read(path):
    return path.read_text(encoding='utf-8')


def ensure_import(text, path):
    if 'load_active_company_ciks' in text:
        return text
    marker = 'import pandas as pd\n'
    if marker not in text:
        raise RuntimeError(f'Cannot locate pandas import marker in {path}')
    return text.replace(marker, marker + SCOPE_IMPORT + '\n', 1)


def assignment_spans(text):
    tree = ast.parse(text)
    spans = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id == 'COMPANIES' for t in targets):
                spans.append((node.lineno, node.end_lineno))
    return sorted(spans)


def replace_assignment_rhs(text, replacement):
    spans = assignment_spans(text)
    if not spans:
        return text, False
    lines = text.splitlines(keepends=True)
    # Replace every COMPANIES assignment, preserving indentation and line endings.
    for start, end in reversed(spans):
        original = ''.join(lines[start-1:end])
        indent = re.match(r'^[ \t]*', original).group(0)
        lines[start-1:end] = [indent + replacement + '\n']
    return ''.join(lines), True


def inject_resolver(text):
    if 'def configured_companies(' in text:
        return text
    marker = 'COMPANIES'
    idx = text.find(marker)
    if idx < 0:
        raise RuntimeError('No COMPANIES declaration found.')
    line_start = text.rfind('\n', 0, idx) + 1
    resolver = (
        'def configured_companies() -> list[str]:\n'
        '    """Resolve the authoritative active production company scope."""\n'
        '    ciks = [str(cik).zfill(10) for cik in load_active_company_ciks()]\n'
        '    if not ciks:\n'
        '        raise ValueError("Active company execution scope is empty.")\n'
        '    return sorted(set(ciks))\n\n\n'
    )
    return text[:line_start] + resolver + text[line_start:]


def patch_file(name):
    path = SRC / name
    if not path.exists():
        raise FileNotFoundError(path)
    text = read(path)
    text = ensure_import(text, path)
    text = inject_resolver(text)

    # Replace all COMPANIES assignments with the resolver. This handles
    # literal lists, assert_company_scope(N), typed assignments, etc.
    text, changed = replace_assignment_rhs(text, 'COMPANIES = configured_companies()')
    if not changed:
        raise RuntimeError(f'Could not locate a COMPANIES assignment in {path}; refusing unsafe edit.')

    path.write_text(text, encoding='utf-8')
    ast.parse(text)


def patch_step20_special(name):
    # Step 20/20.1 may already resolve COMPANIES inside main rather than at module scope.
    path = SRC / name
    if not path.exists():
        raise FileNotFoundError(path)
    text = ensure_import(read(path), path)
    text = inject_resolver(text)
    text, changed = replace_assignment_rhs(text, 'COMPANIES = configured_companies()')
    if not changed:
        raise RuntimeError(f'Could not locate COMPANIES assignment in {path}; refusing unsafe edit.')
    path.write_text(text, encoding='utf-8')
    ast.parse(text)


def main():
    for name in FILES:
        patch_file(name)
    print('Dynamic execution-scope patch v2 applied.')
    print('Company scope is resolved from the authoritative active registry.')
    print('No fixed company count or CIK list is introduced.')
    print('')
    print('Validate:')
    print('  python -m compileall src/sec_ingestion')
    print('  python -m pytest tests/integration/test_step31_multi_company_hardening.py tests/integration/test_unified_company_view.py -q')
    print('  python -m pytest -q')

if __name__ == '__main__':
    main()
