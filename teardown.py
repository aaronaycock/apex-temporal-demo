import asyncio
import sys
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.config import TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TASK_QUEUE_NONPROD
from workflows.teardown_instance import TeardownInstance


async def main(env_name: str):
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    workflow_id = f"teardown-{env_name}"
    try:
        handle = await client.start_workflow(
            TeardownInstance.run,
            env_name,
            id=workflow_id,
            task_queue=TASK_QUEUE_NONPROD,
        )
    except WorkflowAlreadyStartedError:
        print(
            f"Teardown '{workflow_id}' is already running. Wait for it to finish before re-running.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Started teardown: {handle.id}")
    result = await handle.result()
    print(f"Result: {result}")


if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "ephemeral-test-01"
    asyncio.run(main(env))