import asyncio
import sys
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.config import TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TASK_QUEUE_NONPROD
from workflows.swap_instance import SwapInstance


async def main(env_name: str, new_version: str):
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    workflow_id = f"swap-{env_name}-{new_version}"
    try:
        handle = await client.start_workflow(
            SwapInstance.run,
            args=[env_name, new_version],
            id=workflow_id,
            task_queue=TASK_QUEUE_NONPROD,
        )
    except WorkflowAlreadyStartedError:
        print(
            f"Swap '{workflow_id}' is already running. Wait for it to finish before re-running.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Started swap workflow: {handle.id}")
    result = await handle.result()
    print(f"Result: {result}")


if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "ephemeral-test-01"
    ver = sys.argv[2] if len(sys.argv) > 2 else "v2"
    asyncio.run(main(env, ver))