from pathlib import Path
from dataclasses import asdict
import pandas as pd

COLUMNS = ["change_id","company_id","processing_mode","root_component_id",
           "affected_components","reason_code","dependency_version"]

class IncrementalDecisionStore:
    def __init__(self, path="data/silver/platform/incremental/processing_decisions.parquet"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _read(self):
        if not self.path.exists():
            return pd.DataFrame(columns=COLUMNS)
        df = pd.read_parquet(self.path)
        for c in COLUMNS:
            if c not in df.columns:
                df[c] = None
        return df[COLUMNS]

    def append(self, decision):
        decision.validate()
        df = self._read()
        if decision.change_id in set(df["change_id"].astype(str)):
            raise ValueError(f"Duplicate change_id: {decision.change_id}")
        row = asdict(decision)
        row["affected_components"] = "|".join(decision.affected_components)
        pd.concat([df, pd.DataFrame([row])], ignore_index=True).to_parquet(self.path, index=False)

    def read(self):
        return self._read()
