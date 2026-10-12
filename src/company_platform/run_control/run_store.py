from pathlib import Path
from typing import Optional

import pandas as pd

from .models import (
    PipelineRun,
    TaskRun,
    RUN_STATUSES,
    TASK_STATUSES,
)


RUN_COLUMNS = [
    "run_id",
    "company_id",
    "trigger_type",
    "trigger_event_id",
    "correlation_id",
    "pipeline_version",
    "status",
    "started_at_utc",
    "completed_at_utc",
    "error_message",
]


TASK_COLUMNS = [
    "task_id",
    "run_id",
    "company_id",
    "component_id",
    "task_sequence",
    "status",
    "attempt_number",
    "max_attempts",
    "started_at_utc",
    "completed_at_utc",
    "error_message",
    "dq_status",
    "blocked_by",
]


class RunStore:

    def __init__(self, root=None):
        # Resolve storage from repository root, never from caller cwd.
        project_root = Path(__file__).resolve().parents[3]

        self.root = (
            Path(root)
            if root is not None
            else (
                project_root
                / "data"
                / "silver"
                / "platform"
                / "run_control"
            )
        )

        self.root.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.runs_path = (
            self.root / "pipeline_runs.parquet"
        )

        self.tasks_path = (
            self.root / "task_runs.parquet"
        )

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def _read(self, path, columns):
        if not path.exists():
            return pd.DataFrame(
                columns=columns
            )

        # A zero-byte Parquet file is invalid and cannot be read.
        # Treat it the same as a missing artifact so callers can recover.
        if path.stat().st_size == 0:
            return pd.DataFrame(
                columns=columns
            )

        df = pd.read_parquet(path)

        # Backward compatibility:
        # older Parquet artifacts may not contain newer columns.
        for column in columns:
            if column not in df.columns:
                df[column] = None

        return df[columns]

    def runs(self):
        return self._read(
            self.runs_path,
            RUN_COLUMNS,
        )

    def tasks(self):
        return self._read(
            self.tasks_path,
            TASK_COLUMNS,
        )

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    def create_run(
        self,
        run: PipelineRun,
    ):
        run.validate()

        df = self.runs()

        if run.run_id in set(
            df.run_id.astype(str)
        ):
            raise ValueError(
                f"Duplicate run_id: {run.run_id}"
            )

        # PipelineRun.to_dict() may contain additional fields such as
        # source_id and nested metadata. The Parquet RunStore contract
        # contains only RUN_COLUMNS, so explicitly project to that schema.
        run_data = run.to_dict()

        new_row = pd.DataFrame(
            [
                {
                    column: run_data.get(column)
                    for column in RUN_COLUMNS
                }
            ],
            columns=RUN_COLUMNS,
        )

        result = pd.concat(
            [df, new_row],
            ignore_index=True,
        )

        # Enforce the RunStore schema before writing.
        result = result[RUN_COLUMNS]

        result.to_parquet(
            self.runs_path,
            index=False,
        )

    def create_task(
        self,
        task: TaskRun,
    ):
        task.validate()

        df = self.tasks()

        if task.task_id in set(
            df.task_id.astype(str)
        ):
            raise ValueError(
                f"Duplicate task_id: {task.task_id}"
            )

        pair = (
            df.run_id.astype(str)
            == str(task.run_id)
        ) & (
            df.component_id.astype(str)
            == str(task.component_id)
        )

        if pair.any():
            raise ValueError(
                "Duplicate component task in run: "
                f"{task.run_id}/{task.component_id}"
            )

        # TaskRun.to_dict() may also contain additional fields.
        # Persist only the declared TASK_COLUMNS contract.
        task_data = task.to_dict()

        new_row = pd.DataFrame(
            [
                {
                    column: task_data.get(column)
                    for column in TASK_COLUMNS
                }
            ],
            columns=TASK_COLUMNS,
        )

        result = pd.concat(
            [df, new_row],
            ignore_index=True,
        )

        # Enforce the RunStore schema before writing.
        result = result[TASK_COLUMNS]

        result.to_parquet(
            self.tasks_path,
            index=False,
        )

    # ------------------------------------------------------------------
    # Run updates
    # ------------------------------------------------------------------

    def update_run(
        self,
        run_id,
        status,
        started_at_utc=None,
        completed_at_utc=None,
        error_message=None,
    ):
        if status not in RUN_STATUSES:
            raise ValueError(
                f"Invalid run status: {status}"
            )

        df = self.runs()

        mask = (
            df.run_id.astype(str)
            == str(run_id)
        )

        if not mask.any():
            raise KeyError(
                f"Run not found: {run_id}"
            )

        df.loc[mask, "status"] = status

        if started_at_utc is not None:
            df.loc[
                mask,
                "started_at_utc",
            ] = started_at_utc

        if completed_at_utc is not None:
            df.loc[
                mask,
                "completed_at_utc",
            ] = completed_at_utc

        if error_message is not None:
            df.loc[
                mask,
                "error_message",
            ] = error_message

        # Explicitly preserve the RunStore schema.
        df = df[RUN_COLUMNS]

        df.to_parquet(
            self.runs_path,
            index=False,
        )

    # ------------------------------------------------------------------
    # Task updates
    # ------------------------------------------------------------------

    def update_task(
        self,
        task_id,
        status,
        attempt_number=None,
        started_at_utc=None,
        completed_at_utc=None,
        error_message=None,
        dq_status=None,
        blocked_by=None,
    ):
        if status not in TASK_STATUSES:
            raise ValueError(
                f"Invalid task status: {status}"
            )

        df = self.tasks()

        mask = (
            df.task_id.astype(str)
            == str(task_id)
        )

        if not mask.any():
            raise KeyError(
                f"Task not found: {task_id}"
            )

        if (
            attempt_number is not None
            and attempt_number < 0
        ):
            raise ValueError(
                "attempt_number cannot be negative"
            )

        df.loc[mask, "status"] = status

        # When a task is reset to PENDING, clear stale execution state.
        if status == "PENDING":
            df.loc[
                mask,
                "started_at_utc",
            ] = None

            df.loc[
                mask,
                "completed_at_utc",
            ] = None

            df.loc[
                mask,
                "error_message",
            ] = None

            df.loc[
                mask,
                "dq_status",
            ] = None

            df.loc[
                mask,
                "blocked_by",
            ] = None

        if attempt_number is not None:
            df.loc[
                mask,
                "attempt_number",
            ] = attempt_number

        if started_at_utc is not None:
            df.loc[
                mask,
                "started_at_utc",
            ] = started_at_utc

        if completed_at_utc is not None:
            df.loc[
                mask,
                "completed_at_utc",
            ] = completed_at_utc

        if error_message is not None:
            df.loc[
                mask,
                "error_message",
            ] = error_message

        if dq_status is not None:
            df.loc[
                mask,
                "dq_status",
            ] = dq_status

        if blocked_by is not None:
            df.loc[
                mask,
                "blocked_by",
            ] = blocked_by

        # Explicitly preserve the RunStore schema.
        df = df[TASK_COLUMNS]

        df.to_parquet(
            self.tasks_path,
            index=False,
        )

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_run(
        self,
        run_id,
    ) -> Optional[dict]:

        df = self.runs()

        rows = df[
            df.run_id.astype(str)
            == str(run_id)
        ]

        if rows.empty:
            return None

        return rows.iloc[0].to_dict()

    def get_runs_for_company(
        self,
        company_id,
    ):
        df = self.runs()

        return df[
            df.company_id.astype(str)
            == str(company_id)
        ].reset_index(drop=True)

    def get_tasks_for_run(
        self,
        run_id,
    ):
        df = self.tasks()

        return df[
            df.run_id.astype(str)
            == str(run_id)
        ].reset_index(drop=True)