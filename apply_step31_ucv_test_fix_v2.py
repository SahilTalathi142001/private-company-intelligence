from __future__ import annotations
from pathlib import Path
import re
import shutil

ROOT = Path(__file__).resolve().parent
REPO = ROOT if (ROOT / 'src').exists() and (ROOT / 'tests').exists() else Path.cwd()
UCV = REPO / 'src' / 'company_platform' / 'company_intelligence' / 'unified_company_view.py'
UCV_TEST = REPO / 'tests' / 'integration' / 'test_unified_company_view.py'
STEP31_TEST = REPO / 'tests' / 'integration' / 'test_step31_multi_company_hardening.py'

def backup(path):
    b = path.with_suffix(path.suffix + '.bak_step31_ucv')
    if not b.exists(): shutil.copy2(path, b)

def patch_ucv():
    text = UCV.read_text(encoding='utf-8')
    changed = False
    sig = '        expected_universe_ciks: set[str] | None = None,\n'
    if sig not in text:
        old = '        company_master_path: Path = COMPANY_MASTER,\n        only_cik: str | None = None,\n'
        if text.count(old) != 1:
            raise RuntimeError('UCV build signature anchor not found exactly once.')
        backup(UCV)
        text = text.replace(old, old + sig, 1)
        changed = True

    if 'if expected_universe_ciks is None:' in text and 'expected_universe_ciks must contain at least one valid CIK.' in text:
        return changed

    # Replace the original registry-only universe resolution without depending
    # on exact blank lines or line endings.
    pattern = re.compile(
        r'(?m)^(?P<indent>\s*)expected_universe\s*=\s*set\(load_active_company_ciks\(\)\)\s*\n'
        r'(?P=indent)master\s*=\s*master\[master\["cik"\]\.isin\(expected_universe\)\]\.copy\(\)\s*\n'
    )
    replacement = '''\\g<indent>if expected_universe_ciks is None:\n'''
    # Build replacement preserving the existing indentation.
    m = pattern.search(text)
    if not m:
        raise RuntimeError('UCV expected-universe block not found. The file may differ from the expected source; no further changes made.')
    indent = m.group('indent')
    block = (
        f'{indent}if expected_universe_ciks is None:\n'
        f'{indent}    expected_universe = set(load_active_company_ciks())\n'
        f'{indent}else:\n'
        f'{indent}    expected_universe = {{\n'
        f'{indent}        normalize_cik(cik)\n'
        f'{indent}        for cik in expected_universe_ciks\n'
        f'{indent}        if normalize_cik(cik)\n'
        f'{indent}    }}\n'
        f'{indent}    if not expected_universe:\n'
        f'{indent}        raise ValueError(\n'
        f'{indent}            "expected_universe_ciks must contain at least one valid CIK."\n'
        f'{indent}        )\n\n'
        f'{indent}master = master[master["cik"].isin(expected_universe)].copy()\n'
    )
    backup(UCV)
    text = text[:m.start()] + block + text[m.end():]
    UCV.write_text(text, encoding='utf-8')
    return True

def patch_ucv_tests():
    text = UCV_TEST.read_text(encoding='utf-8')
    if 'expected_universe_ciks=set(step16["cik"])' in text:
        return False
    pattern = re.compile(
        r'(result = builder\.build\(\n\s*step16_path=step16_path,\n\s*step20_path=step20_path,\n\s*company_master_path=master_path,\n)(\s*\))'
    )
    text2, n = pattern.subn(r'\1            expected_universe_ciks=set(step16["cik"]),\n\2', text)
    if n != 2:
        raise RuntimeError(f'UCV integration tests: expected 2 builder.build calls, found {n}')
    backup(UCV_TEST)
    UCV_TEST.write_text(text2, encoding='utf-8')
    return True

def patch_step31_tests():
    text = STEP31_TEST.read_text(encoding='utf-8')
    changed = False
    old = '    assert len(h.TARGET_COMPANIES) == 5\n'
    new = '    active_targets = h.active_target_companies()\n    assert len(active_targets) > 0\n    assert set(TARGET).issubset(active_targets)\n'
    if old in text:
        if text.count(old) != 1: raise RuntimeError('Step 31 target-scope assertion occurs more than once.')
        backup(STEP31_TEST); text = text.replace(old,new,1); changed=True
    old = '[{"cik":c,"company_name":h.TARGET_COMPANIES[c]} for c in TARGET]'
    new = '[{"cik":c,"company_name":h.active_target_companies().get(c, c)} for c in TARGET]'
    if old in text:
        if text.count(old) != 1: raise RuntimeError('Step 31 target-company lookup occurs more than once.')
        if not changed: backup(STEP31_TEST)
        text = text.replace(old,new,1); changed=True
    STEP31_TEST.write_text(text, encoding='utf-8') if changed else None
    return changed

def main():
    for p in (UCV, UCV_TEST, STEP31_TEST):
        if not p.exists(): raise FileNotFoundError(p)
    print('Repository:', REPO)
    print('UCV patch:', 'changed' if patch_ucv() else 'already applied')
    print('UCV tests:', 'changed' if patch_ucv_tests() else 'already applied')
    print('Step 31 tests:', 'changed' if patch_step31_tests() else 'already applied')
    print('Done. Backups use *.bak_step31_ucv.')
    print('Run targeted tests next:')
    print(r'python -m pytest tests/integration/test_step31_multi_company_hardening.py tests/integration/test_unified_company_view.py -q')

if __name__ == '__main__': main()
