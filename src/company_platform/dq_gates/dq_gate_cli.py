import argparse, json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[4]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from company_platform.dq_gates.gate_engine import DQGateEngine
from company_platform.dq_gates.promotion_store import DQPromotionStore

def main():
    parser = argparse.ArgumentParser(description="A7 DQ Gates / Promotion")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("evaluate")
    p.add_argument("--run-id", required=True)
    p.add_argument("--component-id", required=True)
    p.add_argument("--checks", type=int, required=True)
    p.add_argument("--failed", type=int, required=True)
    p.add_argument("--fatal", type=int, default=0)
    p.add_argument("--warnings", type=int, default=0)
    p.add_argument("--persist", action="store_true")
    sub.add_parser("list")
    args = parser.parse_args()

    engine = DQGateEngine()
    store = DQPromotionStore(ROOT / "data/gold/platform/dq_gates")

    if args.command == "evaluate":
        dq = engine.evaluate(args.run_id, args.component_id, args.checks,
                             args.failed, args.fatal, args.warnings)
        promo = engine.promote(dq)
        print("DQ RESULT")
        print(json.dumps(dq.__dict__, indent=2))
        print("\nPROMOTION DECISION")
        print(json.dumps(promo.__dict__, indent=2))
        if args.persist:
            store.append_dq(dq)
            store.append_promotion(promo)
            print("\nPersisted.")
    else:
        print("DQ RESULTS")
        d = store.dq_results()
        print(d.to_string(index=False) if not d.empty else "No DQ results.")
        print("\nPROMOTION DECISIONS")
        p = store.promotions()
        print(p.to_string(index=False) if not p.empty else "No promotion decisions.")

if __name__ == "__main__":
    main()
