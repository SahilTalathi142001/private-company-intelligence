from pathlib import Path


def test_handlers_use_a8_keyword_contract():
    from company_platform.integration_v2.components import HANDLERS
    import inspect
    for name, handler in HANDLERS.items():
        params = inspect.signature(handler).parameters
        assert {"run_id", "component_id", "task_id"}.issubset(params)


def test_step12_adapter_persists_features():
    text = Path(__file__).resolve().parents[3] / "src" / "company_platform" / "integration_v2" / "components.py"
    source = text.read_text()
    assert "save_company_output(cik, features)" in source
