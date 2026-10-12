from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[4]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
from company_platform.dq_gates.gate_engine import DQGateEngine

def main():
    engine = DQGateEngine()
    cases = [
        engine.evaluate("run-pass", "financial_features", 10, 0),
        engine.evaluate("run-warning", "funding", 10, 1, fatal_count=0, warning_count=1),
        engine.evaluate("run-fatal", "company_profile", 10, 2, fatal_count=1, warning_count=1),
    ]
    decisions = [engine.promote(x) for x in cases]
    checks = [
        decisions[0].decision == "PROMOTE",
        decisions[1].decision == "QUARANTINE",
        decisions[2].decision == "BLOCK",
        decisions[0].reason_code == "ALL_DQ_CHECKS_PASSED",
        decisions[1].reason_code == "NON_FATAL_DQ_FAILURE",
        decisions[2].reason_code == "FATAL_DQ_FAILURE",
    ]
    failed = sum(not x for x in checks)
    print("========================================")
    print("A7 - DQ GATES / PROMOTION DQ")
    print("========================================")
    print(f"Validation checks    : {len(checks)}")
    print(f"Failed checks        : {failed}")
    print(f"Promotion            : {'PROMOTE' if failed == 0 else 'QUARANTINE'}")
    if failed:
        raise SystemExit(1)
    print("\nA7 DQ PASS")

if __name__ == "__main__":
    main()
