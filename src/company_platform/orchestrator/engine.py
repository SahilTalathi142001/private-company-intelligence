from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Callable, Optional, Set
# ---------------------------------------------------------------------------
# Execution outcome
# ---------------------------------------------------------------------------

@dataclass
class ExecutionOutcome:
    """
    Result returned by an A8 component handler.

    status values currently used by the orchestrator:

        SUCCESS
        FAILED
        QUARANTINED
        BLOCKED

    capability_blocked distinguishes an expected platform capability
    gap from an actual component execution failure.
    """

    status: str
    error_message: Optional[str] = None
    dq_failed: bool = False
    quarantine: bool = False
    capability_blocked: bool = False


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class Orchestrator:
    """
    A8 orchestration engine.

    Responsibilities:

    - receive a dependency-driven execution plan
    - create/manage runs through A5
    - execute components in dependency order
    - invoke component handlers
    - enforce component capability states
    - prevent missing handlers from being treated as success
    - invoke A7 when a component raises an exception
    - block true downstream dependencies after hard failures
    - allow independent branches to continue when another branch is
      capability-blocked

    A8 itself contains no Step 1-21 business logic.
    """

    def __init__(
        self,
        pipeline_registry,
        run_manager,
        incremental_planner,
        dq_gate_engine,
        component_handlers=None,
        dependency_artifact_validator=None,
    ):
        self.registry = pipeline_registry
        self.run_manager = run_manager
        self.incremental_planner = incremental_planner
        self.dq_gate_engine = dq_gate_engine
        self.component_handlers = (
            component_handlers or {}
        )
        # Callable contract: validator(company_id, component_id,
        # pipeline_version) -> bool OR (bool, reason). Missing validator
        # means external dependencies cannot be trusted and fail closed.
        self.dependency_artifact_validator = dependency_artifact_validator

    # ------------------------------------------------------------------
    # Planning
    # ------------------------------------------------------------------

    def build_plan(
        self,
        change_type,
        company_id,
        source_id=None,
    ):
        """
        Ask A6 for the dependency-aware processing plan.

        A8 does not construct the dependency graph itself.
        """
        return self.incremental_planner.plan_for_change(
            change_type=change_type,
            source_id=source_id,
        )

    # ------------------------------------------------------------------
    # Run creation
    # ------------------------------------------------------------------

    def create_run(
        self,
        company_id,
        correlation_id,
        change_type,
        pipeline_version="1.0.0",
        trigger_type="EVENT",
        trigger_event_id=None,
        source_id=None,
        metadata=None,
    ):
        """
        Build an A6 plan and create the corresponding A5 run.
        """
        decision = self.build_plan(
            change_type=change_type,
            company_id=company_id,
            source_id=source_id,
        )

        run = self.run_manager.create_run_from_plan(
            company_id=company_id,
            correlation_id=correlation_id,
            plan=decision["affected_components"],
            pipeline_version=pipeline_version,
            trigger_type=trigger_type,
            trigger_event_id=trigger_event_id,
            source_id=source_id,
            metadata=metadata,
        )

        return run, decision

    # ------------------------------------------------------------------
    # Dependency evaluation
    # ------------------------------------------------------------------

    def _dependencies_successful(
        self,
        run_id,
        component_id,
    ):
        """
        Verify in-run task dependencies and validate omitted prerequisites
        against their persisted artifacts. External dependencies fail closed
        unless an artifact validator is configured and confirms validity.

        Returns
        -------
        tuple[bool, Optional[str]]
            (True, None) when dependencies are satisfied.
            (False, dependency_component_id) otherwise.
        """
        task_df = (
            self.run_manager.store
            .get_tasks_for_run(run_id)
        )

        current = task_df[
            task_df["component_id"]
            == component_id
        ]

        if current.empty:
            return False, "TASK_NOT_FOUND"

        component = self.registry.get(
            component_id
        )

        dependencies = component.depends_on

        if not dependencies:
            return True, None

        for dependency in dependencies:

            dependency_rows = task_df[
                task_df["component_id"]
                == dependency
            ]

            # Dependency is outside this incremental run: validate the
            # existing artifact before treating it as satisfied.
            if dependency_rows.empty:
                validator = self.dependency_artifact_validator
                if validator is None:
                    return False, (
                        f"EXTERNAL_ARTIFACT_INVALID:{dependency}:"
                        "ARTIFACT_VALIDATOR_NOT_CONFIGURED"
                    )

                run = self.run_manager.store.get_run(run_id)
                if not run:
                    return False, (
                        f"EXTERNAL_ARTIFACT_INVALID:{dependency}:"
                        "RUN_METADATA_NOT_FOUND"
                    )
                try:
                    result = validator(
                        run.get("company_id"),
                        dependency,
                        run.get("pipeline_version"),
                    )
                    if isinstance(result, tuple):
                        valid = bool(result[0])
                        reason = result[1] if len(result) > 1 else None
                    else:
                        valid = bool(result)
                        reason = None
                except Exception as exc:
                    valid = False
                    reason = f"VALIDATOR_ERROR:{exc}"

                if not valid:
                    return False, (
                        f"EXTERNAL_ARTIFACT_INVALID:{dependency}:"
                        f"{reason or 'VALIDATION_FAILED'}"
                    )
                continue

            dependency_status = str(
                dependency_rows.iloc[0]["status"]
            )

            if dependency_status != "SUCCESS":
                return False, dependency

        return True, None

    # ------------------------------------------------------------------
    # Main execution loop
    # ------------------------------------------------------------------

    def execute_run(
        self,
        run_id,
    ):
        """
        Execute a dependency-driven A5 run.

        The loop is intentionally deterministic at the task-selection
        level by iterating over the task set provided by A5.

        Hard failures block downstream dependents.

        Capability-blocked components do NOT immediately block unrelated
        branches. Their direct dependents will discover the BLOCKED state
        through dependency evaluation.
        """
        self.run_manager.start_run(run_id)

        tasks = (
            self.run_manager.store
            .get_tasks_for_run(run_id)
        )

        if tasks.empty:
            return self.run_manager.complete_run(
                run_id
            )

        # Recovery-safe initialization: only PENDING tasks are schedulable.
        # SUCCESS tasks from an earlier attempt remain satisfied dependencies.
        # RUNNING tasks are intentionally not replayed automatically because a
        # crashed handler may already have committed an external side effect.
        remaining: Set[str] = set(
            tasks.loc[
                tasks["status"].astype(str) == "PENDING",
                "component_id",
            ].astype(str)
        )

        completed: Set[str] = set(
            tasks.loc[
                tasks["status"].astype(str) == "SUCCESS",
                "component_id",
            ].astype(str)
        )

        unresolved_running = tasks[
            tasks["status"].astype(str) == "RUNNING"
        ]
        if not unresolved_running.empty:
            components = ", ".join(
                unresolved_running["component_id"].astype(str).tolist()
            )
            raise RuntimeError(
                "Run contains RUNNING tasks from a prior/active execution: "
                f"{components}. Reconcile their side effects/checkpoints and "
                "explicitly mark them SUCCESS or PENDING before resuming."
            )

        while remaining:

            progress = False

            for component_id in list(remaining):

                # ------------------------------------------------------
                # Check dependencies
                # ------------------------------------------------------

                ok, blocked_by = (
                    self._dependencies_successful(
                        run_id,
                        component_id,
                    )
                )

                if not ok:

                    dependency_rows = tasks[
                        tasks["component_id"]
                        == blocked_by
                    ]

                    # Missing dependency task means the selected run
                    # does not contain the required dependency.
                    if dependency_rows.empty:

                        self._block_task(
                            run_id,
                            component_id,
                            blocked_by,
                        )

                        remaining.remove(
                            component_id
                        )

                        progress = True

                        continue

                    dependency_status = str(
                        dependency_rows.iloc[0][
                            "status"
                        ]
                    )

                    # A hard failure or quarantine means this task
                    # cannot execute.
                    if dependency_status in {
                        "FAILED",
                        "QUARANTINED",
                        "BLOCKED",
                        "CAPABILITY_BLOCKED",
                    }:

                        if dependency_status == "CAPABILITY_BLOCKED":
                            dependency_message = (
                                f"Dependency {blocked_by} is "
                                "capability-blocked"
                            )

                            self.run_manager.capability_block_task(
                                task_id=(
                                    tasks[
                                        tasks["component_id"]
                                        == component_id
                                    ].iloc[0]["task_id"]
                                ),
                                component_id=component_id,
                                reason=dependency_message,
                            )
                        else:
                            self._block_task(
                                run_id,
                                component_id,
                                blocked_by,
                            )

                        remaining.remove(component_id)
                        progress = True

                        continue

                    # Dependency is still pending/running.
                    # Leave this component in remaining.
                    continue

                # ------------------------------------------------------
                # Execute component
                # ------------------------------------------------------

                outcome = self._execute_component(
                    run_id,
                    component_id,
                )

                remaining.remove(
                    component_id
                )

                completed.add(
                    component_id
                )

                progress = True

                # ------------------------------------------------------
                # Hard failures block downstream tasks immediately.
                #
                # Capability BLOCKED does NOT use this eager path.
                # Its direct dependents will see BLOCKED during their
                # normal dependency evaluation.
                # ------------------------------------------------------

                if outcome.status in {
                    "FAILED",
                    "QUARANTINED",
                }:

                    self._block_dependents(
                        run_id,
                        component_id,
                    )

            # Refresh task state after every scheduling pass.
            tasks = (
                self.run_manager.store
                .get_tasks_for_run(run_id)
            )

            # Only pending tasks remain schedulable.
            remaining = {
                component_id
                for component_id in remaining
                if not tasks[
                    tasks["component_id"]
                    == component_id
                ].empty
                and str(
                    tasks[
                        tasks["component_id"]
                        == component_id
                    ].iloc[0]["status"]
                ) == "PENDING"
            }

            # ----------------------------------------------------------
            # Deadlock detection
            # ----------------------------------------------------------

            if not progress:

                for component_id in list(
                    remaining
                ):

                    self._block_task(
                        run_id,
                        component_id,
                        "ORCHESTRATION_DEADLOCK",
                    )

                    remaining.remove(
                        component_id
                    )

        return self.run_manager.complete_run(
            run_id
        )

    # ------------------------------------------------------------------
    # Component execution
    # ------------------------------------------------------------------

    def _execute_component(
        self,
        run_id,
        component_id,
    ) -> ExecutionOutcome:

        tasks = (
            self.run_manager.store
            .get_tasks_for_run(run_id)
        )

        rows = tasks[
            tasks["component_id"]
            == component_id
        ]

        if rows.empty:
            return ExecutionOutcome(
                status="FAILED",
                error_message=(
                    f"Task not found for component: "
                    f"{component_id}"
                ),
            )

        row = rows.iloc[0]

        task_id = row["task_id"]

        # --------------------------------------------------------------
        # Capability state
        # --------------------------------------------------------------

        try:
            from company_platform.orchestrator.capabilities import (
                get_capability,
            )

            capability = get_capability(
                component_id
            )

        except Exception as exc:

            message = (
                f"Capability metadata error for "
                f"{component_id}: {exc}"
            )

            self.run_manager.fail_task(
                task_id,
                message,
                dq_status="FAIL",
            )

            return ExecutionOutcome(
                status="FAILED",
                error_message=message,
            )

        # --------------------------------------------------------------
        # DISABLED
        # --------------------------------------------------------------

        if capability.state == "DISABLED":

            message = (
                f"Component {component_id} is disabled: "
                f"{capability.reason}"
            )

            self.run_manager.capability_block_task(
                task_id=task_id,
                component_id=component_id,
                reason=message,
                 )

            return ExecutionOutcome(
                status="CAPABILITY_BLOCKED",
                error_message=message,
                capability_blocked=True,
            )

        # --------------------------------------------------------------
        # NOT READY
        # --------------------------------------------------------------

        if capability.state == "NOT_READY":

            message = (
                f"Component {component_id} is not ready: "
                f"{capability.reason}"
            )
            self.run_manager.capability_block_task(
                task_id=task_id,
                 component_id=component_id,
                reason=message,
                )

            return ExecutionOutcome(
                status="CAPABILITY_BLOCKED",
                error_message=message,
                capability_blocked=True,
            )

        # --------------------------------------------------------------
        # IMPLEMENTED
        # --------------------------------------------------------------

        handler = self.component_handlers.get(
            component_id
        )

        if handler is None:

            # This is an engineering defect because the component
            # explicitly claims to be IMPLEMENTED.
            message = (
                f"Component {component_id} is marked "
                "IMPLEMENTED but has no registered handler"
            )

            self.run_manager.fail_task(
                task_id,
                message,
                dq_status="FAIL",
            )

            return ExecutionOutcome(
                status="FAILED",
                error_message=message,
            )

        # --------------------------------------------------------------
        # Execute handler
        # --------------------------------------------------------------

        try:

            self.run_manager.start_task(
                task_id
            )

            raw = handler(
                run_id=run_id,
                component_id=component_id,
                task_id=task_id,
            )

            # Handler can return a fully formed outcome.
            if isinstance(
                raw,
                ExecutionOutcome,
            ):
                outcome = raw

            # Explicit False means component failure.
            elif raw is False:

                outcome = ExecutionOutcome(
                    status="FAILED",
                    error_message=(
                        "Component handler "
                        "returned False"
                    ),
                )

            # Any other return value is considered successful
            # according to the existing handler contract.
            else:

                outcome = ExecutionOutcome(
                    status="SUCCESS"
                )

            # ----------------------------------------------------------
            # Persist task result
            # ----------------------------------------------------------

            if outcome.status == "SUCCESS":

                self.run_manager.succeed_task(
                    task_id,
                    dq_status=(
                        "FAIL"
                        if outcome.dq_failed
                        else "PASS"
                    ),
                )

            elif outcome.status == "BLOCKED":

                self.run_manager.block_task(
                    task_id,
                    (
                        "COMPONENT_CAPABILITY_BLOCKED"
                        if outcome.capability_blocked
                        else "COMPONENT_BLOCKED"
                    ),
                )

            elif outcome.quarantine:

                self.run_manager.fail_task(
                    task_id,
                    outcome.error_message
                    or "DQ quarantine",
                    quarantine=True,
                )

            else:

                self.run_manager.fail_task(
                    task_id,
                    outcome.error_message
                    or "Component failed",
                )

            return outcome

        # --------------------------------------------------------------
        # Unexpected component exception
        # --------------------------------------------------------------

        except Exception as exc:

            error_message = (
                f"Component exception: {exc}"
            )

            # A7 DQ recording is best-effort here.
            #
            # The component exception itself remains a hard execution
            # failure and is persisted through A5 below.
            try:

                result = (
                    self.dq_gate_engine.evaluate(
                        run_id=run_id,
                        task_id=task_id,
                        component_id=component_id,
                        check_count=1,
                        failed_count=1,
                        fatal_count=1,
                        warning_count=0,
                        message=error_message,
                    )
                )

                decision = (
                    self.dq_gate_engine.promote(
                        result
                    )
                )

                from company_platform.dq_gates.promotion_store import (
                    DQPromotionStore,
                )

                store = DQPromotionStore()

                store.append_dq(
                    result
                )

                store.append_promotion(
                    decision
                )

            except Exception:
                # Never hide the actual component failure because
                # secondary DQ persistence failed.
                pass

            self.run_manager.fail_task(
                task_id,
                error_message,
                dq_status="FAIL",
            )

            return ExecutionOutcome(
                status="FAILED",
                error_message=error_message,
                dq_failed=True,
            )

    # ------------------------------------------------------------------
    # Blocking
    # ------------------------------------------------------------------

    def _block_task(
        self,
        run_id,
        component_id,
        blocked_by,
    ):
        """
        Mark a task as blocked using the existing A5 interface.

        The reason is carried through blocked_by rather than introducing
        a new RunManager signature.
        """
        tasks = (
            self.run_manager.store
            .get_tasks_for_run(run_id)
        )

        row = tasks[
            tasks["component_id"]
            == component_id
        ]

        if not row.empty:

            self.run_manager.block_task(
                row.iloc[0]["task_id"],
                blocked_by,
            )

    def _block_dependents(
        self,
        run_id,
        failed_component,
    ):
        """
        Eagerly block downstream tasks after a HARD component failure.

        Capability-blocked components intentionally do not call this
        method. Their direct dependents discover the BLOCKED state
        through dependency evaluation.
        """
        tasks = (
            self.run_manager.store
            .get_tasks_for_run(run_id)
        )

        downstream = (
            self.registry
            .affected_downstream(
                failed_component
            )
        )

        for component_id in downstream:

            row = tasks[
                tasks["component_id"]
                == component_id
            ]

            if (
                not row.empty
                and str(
                    row.iloc[0]["status"]
                ) == "PENDING"
            ):

                self.run_manager.block_task(
                    row.iloc[0]["task_id"],
                    failed_component,
                )

    # ------------------------------------------------------------------
    # Retry
    # ------------------------------------------------------------------

    def retry_run(
        self,
        run_id,
    ):
        """
        Resume retryable FAILED tasks and BLOCKED tasks whose dependencies
        are now available.

        Successful tasks are preserved. RUNNING tasks still require explicit
        reconciliation, and capability-blocked tasks remain untouched.
        """
        tasks = (
            self.run_manager.store
            .get_tasks_for_run(run_id)
        )

        retryable = tasks[
            (tasks["status"] == "FAILED")
            & (
                tasks["attempt_number"]
                < tasks["max_attempts"]
            )
        ]
        resumed_components = set()

        for _, row in retryable.iterrows():

            self.run_manager.store.update_task(
                row["task_id"],
                "PENDING",
            )
            resumed_components.add(
                str(row["component_id"])
            )

        tasks = (
            self.run_manager.store
            .get_tasks_for_run(run_id)
        )

        resumed_count = len(resumed_components)
        while True:
            tasks = (
                self.run_manager.store
                .get_tasks_for_run(run_id)
            )
            resumed_this_pass = False

            for _, row in tasks[
                tasks["status"] == "BLOCKED"
            ].iterrows():
                component_id = str(row["component_id"])
                if component_id in resumed_components:
                    continue

                blocked_by = row["blocked_by"]
                can_resume = (
                    str(blocked_by) in resumed_components
                    if blocked_by is not None
                    else False
                )
                if not can_resume:
                    can_resume, _ = self._dependencies_successful(
                        run_id,
                        component_id,
                    )

                if can_resume:
                    self.run_manager.store.update_task(
                        row["task_id"],
                        "PENDING",
                    )
                    resumed_components.add(component_id)
                    resumed_count += 1
                    resumed_this_pass = True

            if not resumed_this_pass:
                return resumed_count