import asyncio
import logging
from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logging.getLogger("temporalio").setLevel(logging.ERROR)

from shared.config import TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TASK_QUEUE_NONPROD
from workflows.deploy_test_instance import DeployTestInstance
from workflows.provision_module import ProvisionModule
from workflows.swap_instance import SwapInstance
from workflows.teardown_instance import TeardownInstance
from activities.terraform import (
    run_terraform_plan,
    run_terraform_apply,
    run_terraform_destroy,
    read_terraform_outputs,
    simulate_long_step,
)
from activities.health import http_health_check


async def main():
    runtime = Runtime(
        telemetry=TelemetryConfig(
            metrics=PrometheusConfig(bind_address="0.0.0.0:9090"),
        )
    )
    client = await Client.connect(
        TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE, runtime=runtime
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE_NONPROD,
        workflows=[DeployTestInstance, ProvisionModule, SwapInstance, TeardownInstance],
        activities=[
            run_terraform_plan,
            run_terraform_apply,
            run_terraform_destroy,
            read_terraform_outputs,
            simulate_long_step,
            http_health_check,
        ],
    )
    print(f"Worker on task queue: {TASK_QUEUE_NONPROD}, metrics: http://localhost:9090/metrics")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())