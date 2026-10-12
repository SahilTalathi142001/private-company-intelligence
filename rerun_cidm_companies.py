from __future__ import annotations
import argparse, csv, re, subprocess, sys
from datetime import datetime
from pathlib import Path
import pandas as pd

SUCCESS_CIK = "0000320193"

def truthy(v):
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on", "active"}

def normalize_cik(v):
    raw = str(v).strip()
    if raw.endswith(".0"):
        raw = raw[:-2]
    if not raw.isdigit() or len(raw) > 10:
        raise ValueError(f"Invalid CIK in registry: {v!r}")
    return raw.zfill(10)

def main():
    p = argparse.ArgumentParser(description="Rerun active CIDM companies one at a time and capture outcomes.")
    p.add_argument("--repo-root", default=str(Path(__file__).resolve().parent))
    p.add_argument("--include-successful-example", action="store_true")
    p.add_argument("--limit", type=int, default=0, help="Optional maximum number of companies (0=all).")
    p.add_argument(
        "--failed-summary",
        default="",
        help="Prior summary.csv. Only CIKs with run_status FAILED are rerun.",
    )
    args = p.parse_args()
    repo = Path(args.repo_root).resolve()
    reg_path = repo / "data" / "silver" / "platform" / "company_registry.parquet"
    if not reg_path.exists():
        print(f"ERROR: registry not found: {reg_path}", file=sys.stderr); return 2
    try:
        reg = pd.read_parquet(reg_path)
    except Exception as e:
        print(f"ERROR: cannot read registry: {type(e).__name__}: {e}", file=sys.stderr); return 2
    required = {"cik", "status", "ingestion_enabled"}
    if required - set(reg.columns):
        print(f"ERROR: registry missing columns: {sorted(required-set(reg.columns))}", file=sys.stderr); return 2
    active = reg[reg["status"].astype(str).str.strip().str.upper().eq("ACTIVE") & reg["ingestion_enabled"].map(truthy)].copy()
    if "company_name" not in active.columns:
        active["company_name"] = ""
    active["cik"] = active["cik"].map(normalize_cik)
    if active["cik"].duplicated().any():
        print("ERROR: duplicate active CIKs found", file=sys.stderr); return 2
    if not args.include_successful_example:
        active = active[active["cik"] != SUCCESS_CIK]
    if args.failed_summary:
        summary_path = Path(args.failed_summary)
        if not summary_path.is_absolute():
            summary_path = repo / summary_path
        if not summary_path.exists():
            print(f"ERROR: failed summary not found: {summary_path}", file=sys.stderr)
            return 2
        prior = pd.read_csv(summary_path, dtype=str)
        if "cik" not in prior.columns or "run_status" not in prior.columns:
            print("ERROR: summary must contain cik and run_status", file=sys.stderr)
            return 2
        failed = set(prior.loc[prior["run_status"].str.upper().eq("FAILED"), "cik"].map(normalize_cik))
        active = active[active["cik"].isin(failed)]
        print(f"Failed CIKs from summary: {len(failed)}; still active: {len(active)}")
    companies = active[["cik", "company_name"]].to_dict("records")
    if args.limit > 0:
        companies = companies[:args.limit]
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = repo / "logs" / f"company_reruns_{stamp}"
    log_dir.mkdir(parents=True, exist_ok=True)
    summary_path = log_dir / "summary.csv"
    print(f"Selected companies: {len(companies)}; skipped known success {SUCCESS_CIK}: {not args.include_successful_example}")
    print(f"Logs: {log_dir}")
    results = []
    for i, co in enumerate(companies, 1):
        cik, name = co["cik"], str(co.get("company_name", ""))
        log_path = log_dir / f"{cik}.log"
        cmd = [sys.executable, "-m", "company_platform.integration_v2.runner", "--cik", cik]
        print(f"\n[{i}/{len(companies)}] {cik} | {name}", flush=True)
        try:
            proc = subprocess.run(cmd, cwd=repo, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace", check=False)
            output = proc.stdout or ""
            log_path.write_text(output, encoding="utf-8")
            rm = re.search(r"RUN_ID:\s*([0-9a-fA-F-]+)", output)
            sm = re.search(r"RUN_STATUS:\s*(SUCCESS|FAILED|PARTIAL|BLOCKED)", output, re.I)
            status = sm.group(1).upper() if sm else ("FAILED" if proc.returncode else "UNKNOWN")
            errs = [x.strip() for x in output.splitlines() if re.search(r"\b(ERROR|CRITICAL|Traceback|Exception|blocked promotion|fatal DQ)\b", x, re.I)]
            item = {"cik": cik, "company_name": name, "run_id": rm.group(1) if rm else "", "run_status": status, "return_code": proc.returncode, "log_file": str(log_path), "error_summary": " | ".join(errs[-5:])[:4000]}
        except Exception as e:
            log_path.write_text(f"{type(e).__name__}: {e}\n", encoding="utf-8")
            item = {"cik": cik, "company_name": name, "run_id": "", "run_status": "LAUNCH_ERROR", "return_code": -1, "log_file": str(log_path), "error_summary": f"{type(e).__name__}: {e}"}
        results.append(item)
        print(f"CIK={cik} | RUN_ID={item['run_id'] or 'not found'} | RUN_STATUS={item['run_status']} | exit={item['return_code']}", flush=True)
        if item["error_summary"]:
            print(f"Details: {item['error_summary']}", flush=True)
        with summary_path.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(item.keys())); w.writeheader(); w.writerows(results)
    counts = {}
    for x in results: counts[x["run_status"]] = counts.get(x["run_status"], 0) + 1
    print("\n=== FINAL SUMMARY ===")
    print(f"Attempted: {len(results)}")
    print(f"Statuses: {counts}")
    print(f"Summary CSV: {summary_path}")
    print(f"Full logs: {log_dir}")
    print("No DQ gates were disabled; no financial values were synthesized.")
    return 0 if all(x["run_status"] == "SUCCESS" for x in results) else 1

if __name__ == "__main__":
    raise SystemExit(main())
