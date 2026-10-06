from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities.terraform import run_terraform_destroy, read_terraform_outputs


DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=5,
)


@workflow.defn
class TeardownInstance:
    """
    Destroy in reverse dependency order: app -> compute -> network.

    Idempotent across runs: each module's S3 state is read up front, and any
    module whose state is empty (no resources to destroy) is skipped. Re-running
    teardown after a successful run is a no-op, not an error. State files in S3
    are not deleted (preserves audit trail).
    """

    @workflow.run
    async def run(self, env_name: str) -> dict:
        workflow.logger.info(f"TeardownInstance: {env_name}")

        # Read each module's outputs from S3 state up front. Empty outputs mean
        # the state has no resources, so we skip that module's destroy. This is
        # what makes teardown idempotent across runs.
        net_outputs = await workflow.execute_activity(
            read_terraform_outputs,
            args=["network", env_name],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY,
        )
        compute_outputs = await workflow.execute_activity(
            read_terraform_outputs,
            args=["compute", env_name],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY,
        )
        app_outputs = await workflow.execute_activity(
            read_terraform_outputs,
            args=["app", env_name],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=DEFAULT_RETRY,
        )

        subnet_id = net_outputs.get("subnet_id")
        security_group_id = net_outputs.get("security_group_id")

        destroy_plan = [
            ("app", {"app_version": "v1"}, app_outputs),
            ("compute", {
                "subnet_id": subnet_id,
                "security_group_id": security_group_id,
                "app_version": "v1",
            }, compute_outputs),
            ("network", {}, net_outputs),
        ]

        results = {}
        for module, tf_vars, current_outputs in destroy_plan:
            if not current_outputs:
                workflow.logger.info(f"{module}: state empty, skipping destroy")
                results[module] = "skipped (state empty)"
                continue
            workflow.logger.info(f"destroying {module}")
            await workflow.execute_activity(
                run_terraform_destroy,
                args=[module, env_name, tf_vars],
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=DEFAULT_RETRY,
            )
            results[module] = "destroyed"

        return {"env": env_name, "modules": results, "status": "torn-down"}
