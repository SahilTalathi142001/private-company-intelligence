from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd


def truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "active"}


def normalize_cik(value) -> str:
    raw = str(value).strip()
    if raw.endswith(".0"):
        raw = raw[:-2]
    if not raw.isdigit() or len(raw) > 10:
        raise ValueError(f"Invalid CIK in registry: {value!r}")
    return raw.zfill(10)


def read_nonempty_parquet(path: Path) -> tuple[bool, str | None]:
    if not path.exists():
        return False, "missing"
    try:
        frame = pd.read_parquet(path)
    except Exception as exc:
        return False, f"unreadable: {type(exc).__name__}: {exc}"
    if frame.empty:
        return False, "empty"
    return True, None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill financial ingestion for active registry companies whose "
            "Silver financial facts or Gold annual financial statements are missing. "
            "Uses the existing planner-driven company pipeline sequentially."
        )
    )
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parent),
        help="Repository root (defaults to the directory containing this script).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the exact companies that would run, without executing the pipeline.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Maximum CIKs per runner invocation (default: 10).",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    data_root = repo_root / "data"
    registry_path = data_root / "silver" / "platform" / "company_registry.parquet"

    if not registry_path.exists():
        print(json.dumps({
            "status": "FAIL",
            "error": "Company registry not found",
            "path": str(registry_path),
        }, indent=2))
        return 2

    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")

    # Prevent confusing a source/configuration issue with missing financial data.
    env_path = repo_root / ".env"
    has_user_agent = bool(os.getenv("SEC_USER_AGENT"))
    if env_path.exists():
        try:
            has_user_agent = has_user_agent or any(
                line.strip().startswith("SEC_USER_AGENT=")
                and line.split("=", 1)[1].strip().strip('"').strip("'")
                for line in env_path.read_text(encoding="utf-8").splitlines()
                if not line.lstrip().startswith("#") and "=" in line
            )
        except OSError:
            pass
    if not has_user_agent:
        print(json.dumps({
            "status": "FAIL",
            "error": "SEC_USER_AGENT is not configured",
            "action": "Set SEC_USER_AGENT in the repository .env or process environment, then rerun.",
        }, indent=2))
        return 2

    try:
        registry = pd.read_parquet(registry_path)
    except Exception as exc:
        print(json.dumps({
            "status": "FAIL",
            "error": "Company registry cannot be read",
            "detail": f"{type(exc).__name__}: {exc}",
        }, indent=2))
        return 2

    required = {"cik", "status", "ingestion_enabled"}
    missing_columns = sorted(required - set(registry.columns))
    if missing_columns:
        print(json.dumps({
            "status": "FAIL",
            "error": "Company registry schema is missing required columns",
            "missing_columns": missing_columns,
        }, indent=2))
        return 2

    active = registry[
        registry["status"].astype(str).str.strip().str.upper().eq("ACTIVE")
        & registry["ingestion_enabled"].map(truthy)
    ].copy()
    if "company_name" not in active.columns:
        active["company_name"] = ""

    active["cik"] = active["cik"].map(normalize_cik)
    if active["cik"].duplicated().any():
        duplicates = sorted(active.loc[active["cik"].duplicated(False), "cik"].unique())
        print(json.dumps({
            "status": "FAIL",
            "error": "Duplicate active CIKs in registry",
            "duplicates": duplicates,
        }, indent=2))
        return 2

    if not len(active):
        print(json.dumps({
            "status": "FAIL",
            "error": "No active, ingestion-enabled companies found",
        }, indent=2))
        return 2

    bronze_root = data_root / "bronze" / "sec" / "companyfacts"
    silver_root = data_root / "silver" / "financial_facts"
    gold_root = data_root / "gold" / "financials"

    todo: list[dict] = []
    already_complete: list[str] = []
    audit_errors: list[dict] = []

    for row in active.to_dict("records"):
        cik = row["cik"]
        silver_candidates = [
            silver_root / f"cik={cik}" / "financial_facts.parquet",
            silver_root / f"cik={cik}" / "facts.parquet",
        ]
        gold_path = gold_root / f"cik={cik}" / "financials.parquet"

        silver_ok = False
        silver_issues = []
        for candidate in silver_candidates:
            ok, issue = read_nonempty_parquet(candidate)
            if ok:
                silver_ok = True
                break
            if issue and issue.startswith("unreadable"):
                silver_issues.append({"path": str(candidate), "issue": issue})

        gold_ok, gold_issue = read_nonempty_parquet(gold_path)
        if gold_issue and gold_issue.startswith("unreadable"):
            audit_errors.append({"cik": cik, "path": str(gold_path), "issue": gold_issue})
        audit_errors.extend({"cik": cik, **item} for item in silver_issues)

        if silver_ok and gold_ok:
            already_complete.append(cik)
            continue

        bronze_path = bronze_root / f"{cik}.json"
        todo.append({
            "cik": cik,
            "company_name": str(row.get("company_name", "")),
            "bronze_companyfacts_exists": bronze_path.exists(),
            "silver_financial_facts_available": silver_ok,
            "gold_annual_financials_available": gold_ok,
            "reason": (
                "missing_or_empty_silver_and_or_gold"
                if not silver_ok or not gold_ok
                else "unknown"
            ),
        })

    plan = {
        "repo_root": str(repo_root),
        "data_root": str(data_root),
        "active_companies": int(len(active)),
        "companies_with_silver_and_gold_financials": len(already_complete),
        "companies_to_process": len(todo),
        "financial_audit_read_errors": audit_errors,
        "companies": todo,
        "mode": "DRY_RUN" if args.dry_run else "EXECUTE",
    }
    print(json.dumps(plan, indent=2))

    if args.dry_run:
        return 0

    if not todo:
        print("No financial backfill required.")
        return 0

    # Existing runner executes CIKs sequentially within each invocation. Batch
    # invocations make failures easier to isolate and avoid oversized commands.
    failed_batches = []
    batch_results = []
    ciks = [item["cik"] for item in todo]
    for start in range(0, len(ciks), args.batch_size):
        batch = ciks[start:start + args.batch_size]
        command = [
            sys.executable,
            "-m",
            "company_platform.integration_v2.runner",
            "--ciks",
            ",".join(batch),
        ]
        print(
            f"\nRunning financial onboarding batch "
            f"{start // args.batch_size + 1}: {len(batch)} companies",
            flush=True,
        )
        print("Command:", " ".join(command), flush=True)
        try:
            completed = subprocess.run(command, cwd=repo_root, check=False)
            result = {
                "batch": start // args.batch_size + 1,
                "ciks": batch,
                "return_code": completed.returncode,
            }
        except Exception as exc:
            result = {
                "batch": start // args.batch_size + 1,
                "ciks": batch,
                "return_code": -1,
                "error": f"{type(exc).__name__}: {exc}",
            }
        batch_results.append(result)
        if result["return_code"] != 0:
            failed_batches.append(result)

    # Recheck outputs after the orchestrator finishes.
    remaining = []
    for row in todo:
        cik = row["cik"]
        silver_ok = any(
            read_nonempty_parquet(path)[0]
            for path in [
                silver_root / f"cik={cik}" / "financial_facts.parquet",
                silver_root / f"cik={cik}" / "facts.parquet",
            ]
        )
        gold_ok = read_nonempty_parquet(
            gold_root / f"cik={cik}" / "financials.parquet"
        )[0]
        if not (silver_ok and gold_ok):
            remaining.append({
                "cik": cik,
                "company_name": row["company_name"],
                "silver_financial_facts_available": silver_ok,
                "gold_annual_financials_available": gold_ok,
            })

    final = {
        "status": "SUCCESS" if not remaining and not failed_batches else "PARTIAL_OR_FAILED",
        "active_companies": int(len(active)),
        "financially_complete_before": len(already_complete),
        "attempted_companies": len(todo),
        "financially_complete_after": len(active) - len(remaining),
        "remaining_incomplete_companies": remaining,
        "failed_batches": failed_batches,
        "batch_results": batch_results,
        "note": (
            "Only actual SEC-backed outputs count as available. Companies with no "
            "published or accessible SEC financial facts remain incomplete; no values "
            "are synthesized."
        ),
    }
    print("\nFINAL BACKFILL SUMMARY")
    print(json.dumps(final, indent=2))
    return 0 if final["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
