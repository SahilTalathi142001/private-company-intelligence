from .models import (
    PipelineRun,
    TaskRun,
    utc_now,
)

from .run_store import RunStore


class RunManager:

    def __init__(
        self,
        store=None,
    ):
        self.store = store or RunStore()

    # ------------------------------------------------------------------
    # Run creation
    # ------------------------------------------------------------------

    def create_run_from_plan(
    self,
    company_id,
    correlation_id,
    plan,
    pipeline_version="1.0.0",
    trigger_type="MANUAL",
    trigger_event_id=None,
    source_id=None,
    max_attempts_by_component=None,
    metadata=None
):

        run = PipelineRun.create(
            company_id,
            correlation_id,
            pipeline_version,
            trigger_type,
            trigger_event_id,
            source_id=source_id,
            metadata=metadata
        )

        overrides = max_attempts_by_component or {}
        tasks = [
            TaskRun.create(
                run.run_id, company_id, component_id, i,
                overrides.get(component_id, 3),
            )
            for i, component_id in enumerate(plan, 1)
        ]
        if hasattr(self.store, "create_run_with_tasks"):
            actual_id, created = self.store.create_run_with_tasks(run, tasks)
            if not created:
                existing = self.store.get_run(actual_id)
                # Return a model-shaped object for the existing idempotent run.
                from dataclasses import fields
                run = PipelineRun(**{
                    f.name: existing[f.name]
                    for f in fields(PipelineRun)
                    if f.name in existing
                })
        else:
            self.store.create_run(run)
            for task in tasks:
                self.store.create_task(task)

        return run

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    def start_run(
        self,
        run_id,
    ):
        self.store.update_run(
            run_id,
            "RUNNING",
            started_at_utc=utc_now(),
        )

    def complete_run(
        self,
        run_id,
    ):
        """
        Determine final run state from task states.

        Priority:

            QUARANTINED
                ↓
            FAILED
                ↓
            PARTIAL
                ↓
            SUCCESS

        PARTIAL means the run completed without hard failures,
        but one or more tasks were blocked because their component
        capability is currently unavailable.
        """

        tasks = (
            self.store
            .get_tasks_for_run(run_id)
        )

        if tasks.empty:
            raise ValueError(
                "Cannot complete a run with no tasks"
            )

        statuses = set(
            tasks.status.astype(str)
        )

        # --------------------------------------------------------------
        # Hard DQ quarantine
        # --------------------------------------------------------------

        if "QUARANTINED" in statuses:
            status = "QUARANTINED"

        # --------------------------------------------------------------
        # Hard execution failure
        # --------------------------------------------------------------

        elif "FAILED" in statuses:
            status = "FAILED"

        # --------------------------------------------------------------
        # Capability-limited run
        # --------------------------------------------------------------

        elif (
            "CAPABILITY_BLOCKED"
            in statuses
        ):
            status = "PARTIAL"

        # Existing generic BLOCKED remains a failure because it normally
        # represents a dependency/orchestration failure.
        elif "BLOCKED" in statuses:
            status = "FAILED"

        # --------------------------------------------------------------
        # Fully successful run
        # --------------------------------------------------------------

        elif statuses.issubset(
            {
                "SUCCESS",
                "SKIPPED",
            }
        ):
            status = "SUCCESS"

        else:
            raise ValueError(
                "Run is not complete; "
                f"task statuses={sorted(statuses)}"
            )

        self.store.update_run(
            run_id,
            status,
            completed_at_utc=utc_now(),
        )

        return status

    # ------------------------------------------------------------------
    # Task lifecycle
    # ------------------------------------------------------------------

    def start_task(
        self,
        task_id,
    ):
        df = self.store.tasks()

        row = df[
            df.task_id.astype(str)
            == str(task_id)
        ]

        if row.empty:
            raise KeyError(
                f"Task not found: {task_id}"
            )

        attempt = (
            int(
                row.iloc[0]
                .attempt_number
            )
            + 1
        )

        max_attempts = int(
            row.iloc[0]
            .max_attempts
        )

        if attempt > max_attempts:
            raise ValueError(
                "Maximum attempts exceeded "
                f"for task {task_id}"
            )

        self.store.update_task(
            task_id,
            "RUNNING",
            attempt_number=attempt,
            started_at_utc=utc_now(),
        )

    def succeed_task(
        self,
        task_id,
        dq_status=None,
    ):
        self.store.update_task(
            task_id,
            "SUCCESS",
            completed_at_utc=utc_now(),
            dq_status=dq_status,
        )

    def fail_task(
        self,
        task_id,
        error_message,
        quarantine=False,
        dq_status=None,
    ):
        self.store.update_task(
            task_id,
            (
                "QUARANTINED"
                if quarantine
                else "FAILED"
            ),
            completed_at_utc=utc_now(),
            error_message=error_message,
            dq_status=dq_status,
        )

    def block_task(
        self,
        task_id,
        blocked_by,
    ):
        """
        Generic dependency/orchestration block.

        This remains distinct from capability_block_task().
        """

        self.store.update_task(
            task_id,
            "BLOCKED",
            blocked_by=blocked_by,
            completed_at_utc=utc_now(),
        )

    def capability_block_task(
        self,
        task_id,
        component_id,
        reason=None,
    ):
        """
        Mark a task as CAPABILITY_BLOCKED.

        This is not an execution failure.

        It means the dependency graph contains the component, but
        the platform currently does not have an executable adapter
        for that component.
        """

        message = (
            reason
            or
            f"Component capability unavailable: "
            f"{component_id}"
        )

        self.store.update_task(
            task_id,
            "CAPABILITY_BLOCKED",
            completed_at_utc=utc_now(),
            error_message=message,
            blocked_by=(
                f"CAPABILITY:{component_id}"
            ),
        )

    def skip_task(
        self,
        task_id,
    ):
        self.store.update_task(
            task_id,
            "SKIPPED",
            completed_at_utc=utc_now(),
        )

    # ------------------------------------------------------------------
    # Retry
    # ------------------------------------------------------------------

    def retry_run(
        self,
        run_id,
    ):
        """
        Reset retryable hard failures.

        CAPABILITY_BLOCKED tasks are deliberately NOT retried because
        retrying an unavailable implementation cannot fix the problem.

        Once a component becomes IMPLEMENTED, a new run/reprocessing
        request should be created.
        """

        tasks = (
            self.store
            .get_tasks_for_run(run_id)
        )

        retryable = tasks[
            (
                tasks["status"]
                == "FAILED"
            )
            & (
                tasks["attempt_number"]
                < tasks["max_attempts"]
            )
        ]

        for _, row in retryable.iterrows():

            self.store.update_task(
                row["task_id"],
                "PENDING",
                error_message=None,
                blocked_by=None,
            )

        tasks = (
            self.store
            .get_tasks_for_run(run_id)
        )

        failed_ids = set(
            tasks.loc[
                tasks["status"]
                == "FAILED",
                "component_id",
            ].astype(str)
        )

        # Re-open generic dependency blocks caused by retryable failures.
        for _, row in tasks[
            tasks["status"]
            == "BLOCKED"
        ].iterrows():

            if (
                str(row["blocked_by"])
                in failed_ids
            ):

                self.store.update_task(
                    row["task_id"],
                    "PENDING",
                    blocked_by=None,
                )

        return len(retryable)