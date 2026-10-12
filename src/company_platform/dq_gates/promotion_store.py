from pathlib import Path
from dataclasses import asdict
import pandas as pd

DQ_COLUMNS = ["gate_id","run_id","task_id","component_id","status","severity",
              "check_count","failed_count","fatal_count","warning_count","message"]
PROMO_COLUMNS = ["promotion_id","run_id","component_id","dq_status","decision",
                 "reason_code","decided_at_utc"]

class DQPromotionStore:
    def __init__(self, root=None):
        # Resolve storage from the repository root, never from the caller's cwd.
        project_root = Path(__file__).resolve().parents[3]
        self.root = Path(root) if root is not None else project_root / "data" / "gold" / "platform" / "dq_gates"
        self.root.mkdir(parents=True, exist_ok=True)
        self.dq_path = self.root / "dq_results.parquet"
        self.promotion_path = self.root / "promotion_decisions.parquet"

    def _read(self, path, columns):
        if not path.exists():
            return pd.DataFrame(columns=columns)
        df = pd.read_parquet(path)
        for c in columns:
            if c not in df.columns:
                df[c] = None
        return df[columns]

    def append_dq(self, result):
        result.validate()
        df = self._read(self.dq_path, DQ_COLUMNS)
        if result.gate_id in set(df["gate_id"].astype(str)):
            raise ValueError(f"Duplicate gate_id: {result.gate_id}")
        pd.concat([df, pd.DataFrame([asdict(result)])], ignore_index=True).to_parquet(
            self.dq_path, index=False)

    def append_promotion(self, decision):
        decision.validate()
        df = self._read(self.promotion_path, PROMO_COLUMNS)
        if decision.promotion_id in set(df["promotion_id"].astype(str)):
            raise ValueError(f"Duplicate promotion_id: {decision.promotion_id}")
        pd.concat([df, pd.DataFrame([asdict(decision)])], ignore_index=True).to_parquet(
            self.promotion_path, index=False)

    def dq_results(self):
        return self._read(self.dq_path, DQ_COLUMNS)

    def promotions(self):
        return self._read(self.promotion_path, PROMO_COLUMNS)
