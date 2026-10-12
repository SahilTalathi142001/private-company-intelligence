from pathlib import Path
import json
from typing import Dict,List,Set
from .models import PipelineComponent,RetryPolicy
class PipelineRegistry:
    def __init__(self, definition_path=None):
        # Resolve configuration from repository root, independent of cwd.
        project_root = Path(__file__).resolve().parents[3]
        self.definition_path = (Path(definition_path) if definition_path is not None
                                else project_root / "config" / "pipeline_definition.json")
        self.components={}; self._load()
    def _load(self):
        raw=json.loads(self.definition_path.read_text(encoding="utf-8"))
        for x in raw["components"]:
            c=PipelineComponent(x["component_id"],x["component_type"],x["description"],x["depends_on"],x["trigger_events"],x["execution_mode"],RetryPolicy(**x["retry_policy"]),x.get("dq_gate"),x["outputs"],x["downstream"]); c.validate()
            if c.component_id in self.components: raise ValueError(f"Duplicate component_id: {c.component_id}")
            self.components[c.component_id]=c
    def get(self,cid): return self.components[cid]
    def all(self): return list(self.components.values())
    def execution_plan(self,targets=None):
        selected=set()
        if targets:
            def add(cid):
                if cid in selected:return
                selected.add(cid)
                for d in self.get(cid).depends_on:add(d)
            for t in targets:add(t)
        else:selected=set(self.components)
        indeg={c:0 for c in selected}; children={c:[] for c in selected}
        for c in selected:
            for d in self.get(c).depends_on:
                if d in selected: indeg[c]+=1; children[d].append(c)
        q=sorted([c for c,n in indeg.items() if n==0]); plan=[]
        while q:
            c=q.pop(0); plan.append(c)
            for ch in sorted(children[c]):
                indeg[ch]-=1
                if indeg[ch]==0:q.append(ch)
        if len(plan)!=len(selected):raise ValueError("Pipeline dependency graph contains a cycle")
        return plan
    def affected_downstream(self,cid):
        result=[]; seen=set(); q=list(self.get(cid).downstream)
        while q:
            c=q.pop(0)
            if c in seen:continue
            seen.add(c); result.append(c); q.extend(self.get(c).downstream)
        return result
