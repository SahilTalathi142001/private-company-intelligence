import os
from types import SimpleNamespace

import pytest

from company_platform.orchestrator import capabilities
from company_platform.integration_v2 import components


NEW_COMPONENTS = {
    "form_d_ingestion",
    "investor_ownership",
    "valuation",
    "investment_intelligence",
    "document_extraction",
    "document_classification",
    "document_understanding",
    "structured_event_persistence",
    "company_state_projection",
    "company_state_investment_intelligence",
}


def test_new_components_have_registered_handlers():
    assert NEW_COMPONENTS.issubset(components.HANDLERS)


def test_new_components_have_valid_capability_metadata():
    for component_id in NEW_COMPONENTS:
        capability = capabilities.get_capability(component_id)
        capability.validate()
        assert capability.component_id == component_id


def test_implemented_components_are_executable():
    assert capabilities.is_executable("company_discovery")
    assert capabilities.is_executable("document_ingestion")
    assert capabilities.is_executable("document_extraction")
    assert capabilities.is_executable("document_classification")
    assert capabilities.is_executable("document_understanding")
    assert capabilities.is_executable("structured_event_persistence")
    assert capabilities.is_executable("company_state_projection")
    assert capabilities.is_executable(
        "company_state_investment_intelligence"
    )


def test_fatal_step_dq_exception_is_not_swallowed(monkeypatch):
    def fail_dq(_modules):
        raise RuntimeError(
            "STEP 17.1 DQ failed. Funding Gold is not DQ-cleared."
        )

    monkeypatch.setattr(components, "_run_step_mains", fail_dq)

    with pytest.raises(RuntimeError, match="STEP 17.1 DQ failed"):
        components._form_d_ingestion(
            context=None,
            component_id="form_d_ingestion",
            task_id="test-task",
        )


def test_financial_decision_runs_calibration_before_step15(monkeypatch, tmp_path):
    calls = []

    def import_module(module_name):
        return SimpleNamespace(
            main=lambda: calls.append(module_name)
        )

    monkeypatch.setattr(components.importlib, "import_module", import_module)
    monkeypatch.setattr(
        components,
        "_path",
        lambda *parts: tmp_path.joinpath(*parts),
    )
    monkeypatch.setattr(
        components.pd,
        "read_parquet",
        lambda _path: components.pd.DataFrame({"cik": ["0001018724"]}),
    )
    monkeypatch.setattr(
        components,
        "_artifact_dq",
        lambda *args, **kwargs: "checked",
    )

    output_root = tmp_path / "data" / "gold" / "financial_decision_intelligence"
    for relative_path in (
        "decision_signals/financial_decision_signals.parquet",
        "company_financial_decision_snapshot/company_financial_decision_snapshot.parquet",
        "financial_decision_summary/financial_decision_summary.parquet",
    ):
        path = output_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()

    result = components._financial_decision(
        SimpleNamespace(cik="0001018724"),
        "financial_decision",
        "test-task",
    )

    assert result == "checked"
    assert calls.index(
        "src.sec_ingestion.step14_1_ranking_calibration"
    ) < calls.index(
        "src.sec_ingestion.step15_financial_decision_intelligence"
    )


def test_step_groups_are_separate(monkeypatch):
    calls = []

    def record(modules):
        calls.append((modules, os.environ.get("STEP20_CIKS")))

    monkeypatch.setattr(components, "_run_step_mains", record)

    # Artifact validation is intentionally bypassed here:
    # this test checks which script group each adapter invokes.
    monkeypatch.setattr(
        components, "_artifact_dq",
        lambda *args, **kwargs: "checked",
    )

    components._form_d_ingestion(None, "form_d_ingestion", "t1")
    components._investor_ownership(None, "investor_ownership", "t2")
    components._valuation(None, "valuation", "t3")
    monkeypatch.setenv("STEP20_CIKS", "test-previous-scope")
    components._investment_intelligence(
        SimpleNamespace(cik="0000320193"), "investment_intelligence", "t4"
    )

    assert len(calls) == 4
    assert calls[0][0][0].endswith("step17_funding_capital_intelligence")
    assert calls[1][0][0].endswith("step18_investor_ownership_intelligence")
    assert calls[2][0][0].endswith("step19_company_valuation_intelligence")
    assert calls[2][0][1].endswith(
        "step19_1_company_valuation_data_quality"
    )
    assert calls[3][0][0].endswith("step20_unified_investment_intelligence")
    assert calls[3][0][1].endswith(
        "step20_1_unified_investment_intelligence_dq"
    )
    assert calls[3][1] is None
    assert os.environ["STEP20_CIKS"] == "test-previous-scope"