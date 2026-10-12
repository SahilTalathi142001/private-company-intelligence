"""
STEP 16 - SCHEDULED PRODUCTION REFRESH

Version: 16.0.0

Responsibilities:
- Execute the existing Step 36 SEC live refresh.
- Prevent overlapping refresh processes.
- Persist scheduler execution audit.
- Support one-shot execution for Windows Task Scheduler.
- Support long-running interval mode.
- Never duplicate SEC refresh logic.
- Never execute pipeline components directly.

Architecture:

Scheduler
    -> Step 36 SEC Live Refresh
        -> EventStore
            -> EventConsumer
                -> Orchestrator / RunStore
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from company_platform.sec_detection.live_refresh import SECLiveRefresh


VERSION = "16.0.0"

ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = ROOT / "data"

AUDIT_DIR = (
    DATA_DIR
    / "gold"
    / "scheduled_refresh"
)

AUDIT_PATH = (
    AUDIT_DIR
    / "refresh_audit.parquet"
)

LOCK_PATH = (
    DATA_DIR
    / "silver"
    / "platform"
    / "scheduled_refresh.lock"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RefreshLock:
    """
    Cross-process lock using atomic file creation.

    Windows-safe and intentionally simple.
    """

    def __init__(self, path: Optional[Path] = None):
        self.path = Path(LOCK_PATH if path is None else path)
        self.acquired = False

    def acquire(self) -> bool:
        self.path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        try:
            fd = os.open(
                str(self.path),
                os.O_CREAT
                | os.O_EXCL
                | os.O_WRONLY,
            )

            payload = {
                "pid": os.getpid(),
                "created_at_utc": utc_now(),
            }

            with os.fdopen(
                fd,
                "w",
                encoding="utf-8",
            ) as handle:
                json.dump(
                    payload,
                    handle,
                    indent=2,
                )

            self.acquired = True
            return True

        except FileExistsError:
            return False

    def release(self) -> None:
        if not self.acquired:
            return

        try:
            self.path.unlink()
        except FileNotFoundError:
            pass

        self.acquired = False

    def __enter__(self):
        if not self.acquire():
            raise RuntimeError(
                f"Scheduled refresh already running: "
                f"{self.path}"
            )

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):
        self.release()


def append_audit(row: dict) -> None:
    AUDIT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    new_row = pd.DataFrame([row])

    if AUDIT_PATH.exists():
        existing = pd.read_parquet(
            AUDIT_PATH
        )

        output = pd.concat(
            [existing, new_row],
            ignore_index=True,
        )
    else:
        output = new_row

    output.to_parquet(
        AUDIT_PATH,
        index=False,
    )


def run_once(
    *,
    forms: Optional[list[str]] = None,
    user_agent: Optional[str] = None,
) -> dict:
    """
    Execute exactly one Step 36 refresh.

    This function does not contain SEC polling logic.
    """

    execution_id = str(uuid.uuid4())

    started_at = utc_now()

    result = {
        "execution_id": execution_id,
        "scheduler_version": VERSION,
        "started_at_utc": started_at,
        "completed_at_utc": None,
        "status": "FAILED",
        "reason": None,
        "refresh_id": None,
        "checked": 0,
        "emitted": 0,
        "duplicates": 0,
        "filtered": 0,
        "failed": 0,
    }

    lock = RefreshLock()

    if not lock.acquire():
        result["status"] = "SKIPPED"
        result["reason"] = "REFRESH_ALREADY_RUNNING"
        result["completed_at_utc"] = utc_now()

        append_audit(result)

        return result

    try:
        controller_kwargs = {}

        if user_agent:
            controller_kwargs["user_agent"] = user_agent

        controller = SECLiveRefresh(
            **controller_kwargs
        )

        refresh_result = controller.refresh_once(
            forms=forms,
            sleep_seconds=0,
        )

        result.update(
            {
                "status": refresh_result.get(
                    "status",
                    "FAILED",
                ),
                "refresh_id": refresh_result.get(
                    "refresh_id"
                ),
                "checked": refresh_result.get(
                    "checked",
                    0,
                ),
                "emitted": refresh_result.get(
                    "emitted",
                    0,
                ),
                "duplicates": refresh_result.get(
                    "duplicates",
                    0,
                ),
                "filtered": refresh_result.get(
                    "filtered",
                    0,
                ),
                "failed": refresh_result.get(
                    "failed",
                    0,
                ),
            }
        )

        if result["failed"] > 0:
            result["reason"] = (
                "ONE_OR_MORE_COMPANIES_FAILED"
            )

        return result

    except Exception as exc:
        result["status"] = "FAILED"
        result["reason"] = (
            f"{type(exc).__name__}: {exc}"
        )

        return result

    finally:
        result["completed_at_utc"] = utc_now()
        append_audit(result)
        lock.release()


def run_scheduler(
    *,
    interval_minutes: int,
    forms: Optional[list[str]] = None,
    user_agent: Optional[str] = None,
) -> None:
    """
    Long-running scheduler mode.

    Runs once immediately and then sleeps for the
    configured interval.
    """

    if interval_minutes <= 0:
        raise ValueError(
            "interval_minutes must be greater than zero"
        )

    while True:
        result = run_once(
            forms=forms,
            user_agent=user_agent,
        )

        print(
            json.dumps(
                result,
                indent=2,
                default=str,
            )
        )

        time.sleep(
            interval_minutes * 60
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Step 16 - Scheduled Production Refresh"
        )
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help=(
            "Execute one scheduled refresh and exit."
        ),
    )

    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=60,
        help=(
            "Interval for long-running scheduler mode."
        ),
    )

    parser.add_argument(
        "--forms",
        nargs="*",
        default=None,
        help=(
            "Optional SEC form filter."
        ),
    )

    parser.add_argument(
        "--user-agent",
        default=None,
        help=(
            "SEC User-Agent. Defaults to "
            "SEC_USER_AGENT environment variable."
        ),
    )

    args = parser.parse_args()

    user_agent = (
        args.user_agent
        or os.getenv("SEC_USER_AGENT")
    )

    if not user_agent:
        print(
            "ERROR: SEC_USER_AGENT is not configured.",
            file=sys.stderr,
        )
        raise SystemExit(2)

    if args.once:
        result = run_once(
            forms=args.forms,
            user_agent=user_agent,
        )

        print(
            "========================================"
        )
        print(
            "STEP 16 - SCHEDULED PRODUCTION REFRESH"
        )
        print(
            "========================================"
        )
        print(
            f"Version       : {VERSION}"
        )
        print(
            f"Execution ID  : {result['execution_id']}"
        )
        print(
            f"Status        : {result['status']}"
        )
        print(
            f"Refresh ID    : {result['refresh_id']}"
        )
        print(
            f"Checked       : {result['checked']}"
        )
        print(
            f"Emitted       : {result['emitted']}"
        )
        print(
            f"Duplicates    : {result['duplicates']}"
        )
        print(
            f"Filtered      : {result['filtered']}"
        )
        print(
            f"Failed        : {result['failed']}"
        )
        print(
            f"Reason        : {result['reason']}"
        )
        print(
            f"Audit         : {AUDIT_PATH}"
        )

        if result["status"] == "FAILED":
            raise SystemExit(1)

        return

    run_scheduler(
        interval_minutes=args.interval_minutes,
        forms=args.forms,
        user_agent=user_agent,
    )


if __name__ == "__main__":
    main()