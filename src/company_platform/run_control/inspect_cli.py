import argparse
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[3]; SRC=ROOT/"src"
if str(SRC) not in sys.path: sys.path.insert(0,str(SRC))
from company_platform.run_control.run_store import RunStore
p=argparse.ArgumentParser(description="Inspect A5 run/task state")
sub=p.add_subparsers(dest="command",required=True)
r=sub.add_parser("runs"); r.add_argument("--run-id")
t=sub.add_parser("tasks"); t.add_argument("--run-id")
a=p.parse_args()
s=RunStore(ROOT/"data/silver/platform/run_control")
if a.command=="runs":
    df=s.runs()
else:
    df=s.tasks()
if a.run_id:
    df=df[df.run_id.astype(str)==str(a.run_id)]
print(df.to_string(index=False) if not df.empty else "No records.")
