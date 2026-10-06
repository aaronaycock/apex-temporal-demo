import argparse
import asyncio
import sys
from temporalio.client import Client, WorkflowHandle
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError

from shared.config import TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TASK_QUEUE_NONPROD
from workflows.deploy_test_instance import DeployTestInstance

# DeployTestInstance.status phases that mean the deploy is still on its way up.
IN_PROGRESS = {None, "starting", "provisioning-network", "provisioning-compute", "provisioning-app", "health-check"}


async def print_url_when_healthy(handle: WorkflowHandle) -> None:
    """With a TTL, the result only arrives after teardown, so ask the running
    workflow for the URL instead and print it as soon as the instance is up."""
    while True:
        try:
            state = await handle.query(DeployTestInstance.status)
        except RPCError:
            state = {}  # no worker yet, or it's restarting; ask again
        if state.get("phase") not in IN_PROGRESS:
            break
        await asyncio.sleep(5)
    if state["phase"] != "ttl-wait":
        return  # rolled back or already tearing down; handle.result() below reports it
    print(f"Healthy: {state['url']} (tears down at {state['expires_at']})")
    print(f"  extend 10 min: temporal workflow signal --workflow-id {handle.id} --name extend_ttl --input 600")
    print(f"  tear down now: temporal workflow signal --workflow-id {handle.id} --name teardown_now")
    print(f"  status:        temporal workflow query --workflow-id {handle.id} --name status")


async def main(env_name: str, ttl_seconds: int, fail_after_network: bool):
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)
    workflow_id = f"deploy-{env_name}"
    try:
        handle = await client.start_workflow(
            DeployTestInstance.run,
            args=[env_name, ttl_seconds, True, fail_after_network],
            id=workflow_id,
            task_queue=TASK_QUEUE_NONPROD,
        )
    except WorkflowAlreadyStartedError:
        print(
            f"Workflow '{workflow_id}' is already running. "
            f"Wait for it to finish, tear down '{env_name}' first, or pick a different env name.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Started workflow: {handle.id}")
    if ttl_seconds > 0:
        await print_url_when_healthy(handle)
    result = await handle.result()
    print(f"Result: {result}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Provision an ephemeral test environment.")
    parser.add_argument(
        "env", nargs="?", default="ephemeral-test-01",
        help="environment name; names starting with 'chaos-' pause 30s after network for a kill -9 demo",
    )
    parser.add_argument(
        "--ttl", type=int, default=0, metavar="SECONDS",
        help="tear the environment down automatically after this many seconds (default: keep it)",
    )
    parser.add_argument(
        "--fail-after-network", action="store_true",
        help="fail on purpose after the network module to demo saga rollback",
    )
    args = parser.parse_args()
    asyncio.run(main(args.env, args.ttl, args.fail_after_network))
