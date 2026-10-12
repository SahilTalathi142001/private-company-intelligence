from __future__ import annotations


class IncrementalPlanner:
    """Calculate the smallest dependency closure for a change.

    The returned plan contains only the changed component and its downstream
    closure. Components outside that closure are validated through persisted
    artifacts by the orchestrator rather than executed again.
    """

    def __init__(self, pipeline_registry):
        self.registry = pipeline_registry

    def _topological_subset(self, component_ids):
        selected = set(component_ids)
        indegree = {cid: 0 for cid in selected}
        children = {cid: [] for cid in selected}
        for cid in selected:
            for dep in self.registry.get(cid).depends_on:
                if dep in selected:
                    indegree[cid] += 1
                    children[dep].append(cid)
        ready = sorted(cid for cid, degree in indegree.items() if degree == 0)
        plan = []
        while ready:
            cid = ready.pop(0)
            plan.append(cid)
            for child in sorted(children[cid]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
                    ready.sort()
        if len(plan) != len(selected):
            raise ValueError("Incremental dependency closure contains a cycle")
        return plan

    def plan_from_component(self, component_id):
        selected = [component_id] + self.registry.affected_downstream(component_id)
        return self._topological_subset(selected)

    def plan_for_change(self, change_type, source_id=None, dataset_id=None, document=False):
        if change_type == "NEW_SEC_FILING":
            root = "sec_ingestion"
            reason = "SEC_FILING_AFFECTS_FINANCIAL_AND_DOWNSTREAM_ARTIFACTS"
        elif change_type == "SOURCE_DATA_CHANGED":
            if source_id == "SEC_FORM_D":
                root, reason = "form_d_ingestion", "FORM_D_SOURCE_CHANGE"
            elif source_id == "DOCUMENT":
                root, reason = "document_ingestion", "DOCUMENT_SOURCE_CHANGE"
            else:
                root, reason = "sec_ingestion", "SOURCE_CHANGE_DEFAULTED_TO_SEC"
        elif change_type == "DOCUMENT_RECEIVED":
            root, reason = "document_ingestion", "NEW_DOCUMENT"
        elif change_type == "NEW_COMPANY":
            return {
                "processing_mode": "FULL",
                "root_component_id": "company_registry",
                "affected_components": self.registry.execution_plan(["company_discovery"]),
                "reason_code": "NEW_COMPANY_REQUIRES_INITIAL_ONBOARDING",
            }
        elif change_type in {"MANUAL_REPROCESS", "RETRY"}:
            root, reason = "sec_ingestion", "MANUAL_OR_RETRY_REPROCESS"
        else:
            raise ValueError(f"Unsupported change type: {change_type}")

        return {
            "processing_mode": "INCREMENTAL",
            "root_component_id": root,
            "affected_components": self.plan_from_component(root),
            "reason_code": reason,
        }
