from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

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


@workflow.defn
class ProvisionModule:
    """
    Reusable child: plan -> (apply if changes) -> return outputs.
    Each call gets its own event history for clean replay + scoping.
    """

    @workflow.run
    async def run(self, module: str, env: str, tf_vars: dict) -> dict:
        workflow.logger.info(f"ProvisionModule start: {module}/{env}")

        plan: PlanResult = await workflow.execute_activity(
            run_terraform_plan,
            args=[module, env, tf_vars],
            # Includes up to 2m waiting on another run's state lock.
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=DEFAULT_RETRY,
        )

        if not plan.has_changes:
            workflow.logger.info(f"{module}: no changes, returning current outputs")
            return plan.current_outputs

        applied: ApplyResult = await workflow.execute_activity(
            run_terraform_apply,
            # tf_vars let a retried apply re-plan instead of reusing a stale plan.
            args=[module, env, tf_vars, plan.plan_path],
            start_to_close_timeout=timedelta(minutes=15),
            heartbeat_timeout=timedelta(seconds=60),
            retry_policy=DEFAULT_RETRY,
            # On cancel, wait for terraform to stop and release the state lock
            # before this child reports cancelled and the parent tears down.
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )

        workflow.logger.info(f"ProvisionModule done: {module}/{env}")
        return applied.outputs
