import asyncio
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from activities.terraform import (
        run_terraform_plan,
        run_terraform_apply,
        PlanResult,
        ApplyResult,
    )


DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=5,
    # No non_retryable_error_types: the activities raise ApplicationError with
    # non_retryable=True for failures a retry can't fix (bad config, expired
    # credentials, quota), so those fail on the first attempt.
)

DEFAULT_APPROVAL_TIMEOUT_SECONDS = 24 * 60 * 60


@workflow.defn
class ProvisionModule:
    """
    Reusable child: plan -> (apply if changes) -> return outputs.
    Each call gets its own event history for clean replay + scoping.

    With `require_approval`, a plan with changes waits for the `decide` update
    before it is applied, and the `plan_summary` query shows what would change.
    Rejection or timeout fails the child with a non-retryable error, so the
    parent's saga rollback tears down what earlier modules built. If apply has
    to re-plan (the saved plan file is gone) and the new plan differs from the
    approved one, the module waits for approval again.
    """

    def __init__(self) -> None:
        self._module: str | None = None
        self._env: str | None = None
        self._status = "planning"
        self._summary: dict | None = None
        self._round = 0
        self._awaiting_approval = False
        self._decision: tuple[bool, str] | None = None
        self._reason: str | None = None

    @workflow.query
    def plan_summary(self) -> dict:
        return {
            "module": self._module,
            "env": self._env,
            "status": self._status,
            "round": self._round,
            "summary": self._summary,
            "reason": self._reason,
        }

    @workflow.update
    def decide(self, approved: bool, reason: str) -> str:
        # Two updates can pass the validator in the same workflow task; only
        # the first one decides.
        if not self._awaiting_approval:
            raise ApplicationError(f"{self._module}/{self._env} is not waiting for approval")
        self._awaiting_approval = False
        self._decision = (approved, reason)
        return "approved" if approved else "rejected"

    @decide.validator
    def _validate_decide(self, approved: bool, reason: str) -> None:
        if not self._awaiting_approval:
            raise ValueError(
                f"{self._module}/{self._env} is not waiting for approval (status: {self._status})"
            )

    @workflow.run
    async def run(
        self,
        module: str,
        env: str,
        tf_vars: dict,
        require_approval: bool = False,
        approval_timeout_seconds: int = DEFAULT_APPROVAL_TIMEOUT_SECONDS,
    ) -> dict:
        workflow.logger.info(f"ProvisionModule start: {module}/{env}")
        self._module, self._env = module, env

        plan: PlanResult = await workflow.execute_activity(
            run_terraform_plan,
            # Only ask for a summary (terraform show -json) when someone will read it.
            args=[module, env, tf_vars, True] if require_approval else [module, env, tf_vars],
            # Includes up to 2m waiting on another run's state lock.
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=DEFAULT_RETRY,
        )

        if not plan.has_changes:
            workflow.logger.info(f"{module}: no changes, returning current outputs")
            self._status = "no-changes"
            return plan.current_outputs

        if not require_approval:
            applied = await self._apply(module, env, tf_vars, plan.plan_path)
        else:
            summary, plan_path = plan.summary, plan.plan_path
            while True:
                await self._wait_for_approval(summary, approval_timeout_seconds)
                try:
                    applied = await self._apply(module, env, tf_vars, plan_path, summary)
                    break
                except ActivityError as e:
                    cause = e.cause
                    if not (isinstance(cause, ApplicationError) and cause.type == "PlanChangedError"):
                        raise
                    workflow.logger.warning(f"{module}: re-plan differs from the approved plan; asking again")
                    summary, plan_path = cause.details

        self._status = "applied"
        workflow.logger.info(f"ProvisionModule done: {module}/{env}")
        return applied.outputs

    async def _apply(
        self, module: str, env: str, tf_vars: dict, plan_path: str, approved_summary: dict | None = None
    ) -> ApplyResult:
        self._status = "applying"
        return await workflow.execute_activity(
            run_terraform_apply,
            # tf_vars let a retried apply re-plan instead of reusing a stale plan;
            # approved_summary makes that re-plan prove it matches what was approved.
            args=[module, env, tf_vars, plan_path]
            + ([approved_summary] if approved_summary is not None else []),
            start_to_close_timeout=timedelta(minutes=15),
            heartbeat_timeout=timedelta(seconds=60),
            retry_policy=DEFAULT_RETRY,
            # On cancel, wait for terraform to stop and release the state lock
            # before this child reports cancelled and the parent tears down.
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )

    async def _wait_for_approval(self, summary: dict, timeout_seconds: int) -> None:
        self._summary = summary
        self._round += 1
        self._decision = None
        self._reason = None
        self._status = "awaiting-approval"
        self._awaiting_approval = True
        workflow.logger.info(
            f"{self._module}/{self._env}: waiting up to {timeout_seconds}s for approval "
            f"(+{summary['add']} ~{summary['change']} -{summary['destroy']})"
        )
        try:
            await workflow.wait_condition(
                lambda: self._decision is not None, timeout=timedelta(seconds=timeout_seconds)
            )
        except asyncio.TimeoutError:
            self._status = "timed-out"
            raise ApplicationError(
                f"{self._module}/{self._env}: no approval decision within {timeout_seconds}s",
                type="ApprovalTimeout",
                non_retryable=True,
            )
        finally:
            # Covers cancellation too: once we stop waiting, decide is invalid.
            self._awaiting_approval = False

        approved, self._reason = self._decision
        if not approved:
            self._status = "rejected"
            raise ApplicationError(
                f"{self._module}/{self._env}: plan rejected: {self._reason or 'no reason given'}",
                type="ApprovalRejected",
                non_retryable=True,
            )
        self._status = "approved"
