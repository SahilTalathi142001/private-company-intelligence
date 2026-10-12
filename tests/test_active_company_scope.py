from pathlib import Path

import pandas as pd

from company_platform.registry import active_company_scope as scope


def _registry_df(ciks: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "company_id": [f"SEC_{c}" for c in ciks],
            "cik": ciks,
            "status": ["ACTIVE"] * len(ciks),
            "ingestion_enabled": [True] * len(ciks),
        }
    )


def _patch_registry(tmp_path, monkeypatch, ciks):
    registry = tmp_path / "company_registry.parquet"
    registry.touch()
    monkeypatch.setattr(scope, "REGISTRY_PATH", registry)
    monkeypatch.setattr(pd, "read_parquet", lambda path: _registry_df(ciks))
    return registry


def test_execution_scope_uses_registry_not_reference_master(tmp_path, monkeypatch):
    ciks = [str(i).zfill(10) for i in range(1, 51)]
    registry = _patch_registry(tmp_path, monkeypatch, ciks)

    resolved = scope.resolve_execution_scope()

    assert resolved.count == 50
    assert resolved.as_list() == sorted(ciks)
    assert resolved.source == str(registry)


def test_execution_scope_scales_without_code_change(tmp_path, monkeypatch):
    ciks = [str(i).zfill(10) for i in range(1, 21)]
    registry = _patch_registry(tmp_path, monkeypatch, ciks)

    assert scope.assert_company_scope() == sorted(ciks)

    expanded = [str(i).zfill(10) for i in range(1, 101)]
    monkeypatch.setattr(pd, "read_parquet", lambda path: _registry_df(expanded))

    assert scope.assert_company_scope() == sorted(expanded)
    assert registry.exists()


def test_fixed_count_is_legacy_only(tmp_path, monkeypatch):
    ciks = [str(i).zfill(10) for i in range(1, 60)]
    _patch_registry(tmp_path, monkeypatch, ciks)

    assert scope.assert_company_scope() == sorted(ciks)

    try:
        scope.assert_company_scope(50)
    except RuntimeError as exc:
        assert "Expected 50 companies, found 59" in str(exc)
    else:
        raise AssertionError("Fixed-count assertion unexpectedly accepted a 59-company scope")
