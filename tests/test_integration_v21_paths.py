from pathlib import Path
from company_platform.pipeline_model.pipeline_registry import PipelineRegistry
from company_platform.run_control.run_store import RunStore
from company_platform.dq_gates.promotion_store import DQPromotionStore

def test_pipeline_registry_uses_repo_root():
    assert PipelineRegistry().definition_path.name == "pipeline_definition.json"
    assert PipelineRegistry().definition_path.parent.name == "config"

def test_run_store_uses_repo_root():
    assert RunStore().root.parts[-4:] == ("data", "silver", "platform", "run_control")

def test_promotion_store_uses_repo_root():
    assert DQPromotionStore().root.parts[-4:] == ("data", "gold", "platform", "dq_gates")
