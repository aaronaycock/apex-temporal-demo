"""Workflows on a local dev server, with the Terraform and HTTP activities
replaced by in-memory mocks registered under the same names. Each test uses a
unique env name and ends by replaying that env's histories against the
current code, which catches non-determinism."""
import asyncio
import uuid
from datetime import datetime, timedelta

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError, WorkflowHandle
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.worker import Replayer, Worker

import starter
from activities.health import HealthResult
from activities.terraform import ApplyResult, PlanResult
from workflows.deploy_test_instance import DeployTestInstance
from workflows.provision_module import ProvisionModule
from workflows.swap_instance import SwapInstance
from workflows.teardown_instance import TeardownInstance

WORKFLOWS = [DeployTestInstance, ProvisionModule, SwapInstance, TeardownInstance]
OUTPUTS = {
    "network": {"subnet_id": "subnet-1", "security_group_id": "sg-1"},
    "compute": {"public_ip": "203.0.113.10"},
    "app": {"app_version": "v1"},
}
URL = "http://203.0.113.10/"

# Fake infrastructure, keyed by env name.
built: dict[str, set[str]] = {}       # modules that currently have resources
log: dict[str, list[str]] = {}        # "apply:network", "destroy:app", ... in order
slow_applies: set[str] = set()        # "env/module" applies that run until cancelled


@activity.defn(name="run_terraform_plan")
async def mock_plan(module: str, env: str, tf_vars: dict) -> PlanResult:
    return PlanResult(module, env, f"/tmp/{module}.bin", True, "", {})


@activity.defn(name="run_terraform_apply")
async def mock_apply(module: str, env: str, tf_vars: dict, plan_path: str) -> ApplyResult:
    built.setdefault(env, set()).add(module)
    log.setdefault(env, []).append(f"apply:{module}")
    if f"{env}/{module}" in slow_applies:
        try:
            while True:
                activity.heartbeat()
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            log[env].append(f"apply-cancelled:{module}")
            raise
    return ApplyResult(module, env, OUTPUTS[module])


@activity.defn(name="read_terraform_outputs")
async def mock_outputs(module: str, env: str) -> dict:
    return OUTPUTS[module] if module in built.get(env, set()) else {}


@activity.defn(name="run_terraform_destroy")
async def mock_destroy(module: str, env: str, tf_vars: dict) -> None:
    built.get(env, set()).discard(module)
    log.setdefault(env, []).append(f"destroy:{module}")


@activity.defn(name="simulate_long_step")
async def mock_long_step(label: str, seconds: int = 30) -> str:
    return f"{label}:done"


@activity.defn(name="http_health_check")
async def mock_health(url: str) -> HealthResult:
    return HealthResult(url, 200)


@pytest.fixture
def client(temporal_env) -> Client:
    return temporal_env.client


@pytest.fixture
async def tq(client):
    """Run a worker with the mocks on a fresh task queue for one test."""
    task_queue = f"test-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=WORKFLOWS,
        activities=[mock_plan, mock_apply, mock_outputs, mock_destroy, mock_long_step, mock_health],
        # Deliver activity cancellation within a second instead of up to 48s
        # (heartbeats are throttled to 80% of the 60s heartbeat timeout).
        max_heartbeat_throttle_interval=timedelta(seconds=1),
        default_heartbeat_throttle_interval=timedelta(seconds=1),
    ):
        yield task_queue


def new_env(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


async def start_deploy(client: Client, tq: str, env: str, ttl: int = 0, fail: bool = False) -> WorkflowHandle:
    return await client.start_workflow(
        DeployTestInstance.run, args=[env, ttl, True, fail], id=f"deploy-{env}", task_queue=tq
    )


async def wait_for_status(handle: WorkflowHandle, predicate, timeout: float = 30) -> dict:
    async with asyncio.timeout(timeout):
        while not predicate(state := await handle.query(DeployTestInstance.status)):
            await asyncio.sleep(0.1)
    return state


async def wait_for_phase(handle: WorkflowHandle, phase: str) -> dict:
    return await wait_for_status(handle, lambda s: s["phase"] == phase)


async def assert_cancelled(handle: WorkflowHandle) -> None:
    with pytest.raises(WorkflowFailureError) as err:
        await asyncio.wait_for(handle.result(), 30)
    assert isinstance(err.value.cause, CancelledError)
    assert (await handle.describe()).status == WorkflowExecutionStatus.CANCELED


async def assert_replays(client: Client, env: str) -> None:
    """Replay every history for this env (parent and children)."""
    replayer = Replayer(workflows=WORKFLOWS)
    replayed = 0
    async for wf in client.list_workflows():
        if env in wf.id:
            history = await client.get_workflow_handle(wf.id, run_id=wf.run_id).fetch_history()
            await replayer.replay_workflow(history)
            replayed += 1
    assert replayed > 0


async def test_ttl_status_query_and_signals(client, tq):
    env = new_env("ttl")
    handle = await start_deploy(client, tq, env, ttl=600)

    state = await wait_for_phase(handle, "ttl-wait")
    assert state["url"] == URL
    assert state["modules"] == ["network", "compute", "app"]

    expires = datetime.fromisoformat(state["expires_at"])
    await handle.signal(DeployTestInstance.extend_ttl, 120)
    await wait_for_status(
        handle, lambda s: datetime.fromisoformat(s["expires_at"]) - expires == timedelta(seconds=120)
    )

    await handle.signal(DeployTestInstance.teardown_now)
    result = await asyncio.wait_for(handle.result(), 30)
    assert "torn_down_at" in result
    assert not built[env]
    assert (await handle.query(DeployTestInstance.status))["phase"] == "torn-down"
    await assert_replays(client, env)


async def test_ttl_expires_on_its_own(client, tq):
    env = new_env("ttl")
    handle = await start_deploy(client, tq, env, ttl=2)
    result = await asyncio.wait_for(handle.result(), 30)
    assert "torn_down_at" in result
    assert not built[env]
    await assert_replays(client, env)


async def test_cancel_during_ttl_wait_tears_down(client, tq):
    env = new_env("cancel-ttl")
    handle = await start_deploy(client, tq, env, ttl=600)
    await wait_for_phase(handle, "ttl-wait")

    await handle.cancel()
    await assert_cancelled(handle)
    assert not built[env]
    assert (await handle.query(DeployTestInstance.status))["phase"] == "rolled-back"
    await assert_replays(client, env)


async def test_cancel_during_apply_tears_down_after_apply_stops(client, tq):
    env = new_env("cancel-apply")
    slow_applies.add(f"{env}/compute")
    handle = await start_deploy(client, tq, env)
    await wait_for_phase(handle, "provisioning-compute")
    async with asyncio.timeout(10):
        while "apply:compute" not in log.get(env, []):
            await asyncio.sleep(0.05)

    await handle.cancel()
    await assert_cancelled(handle)
    assert not built[env], "network and the partial compute should be destroyed"
    events = log[env]
    first_destroy = min(i for i, e in enumerate(events) if e.startswith("destroy:"))
    assert events.index("apply-cancelled:compute") < first_destroy, "teardown must wait for apply to stop"
    await assert_replays(client, env)


async def test_failure_rolls_back_and_fails(client, tq):
    env = new_env("fail")
    handle = await start_deploy(client, tq, env, fail=True)
    with pytest.raises(WorkflowFailureError) as err:
        await asyncio.wait_for(handle.result(), 30)
    assert isinstance(err.value.cause, ApplicationError)
    assert err.value.cause.type == "RuntimeError"
    assert (await handle.describe()).status == WorkflowExecutionStatus.FAILED
    assert log[env] == ["apply:network", "destroy:network"]
    assert (await handle.query(DeployTestInstance.status))["phase"] == "rolled-back"
    await assert_replays(client, env)


async def test_swap(client, tq):
    env = new_env("swap")
    await asyncio.wait_for((await start_deploy(client, tq, env)).result(), 30)
    result = await client.execute_workflow(
        SwapInstance.run, args=[env, "v2"], id=f"swap-{env}-v2", task_queue=tq
    )
    assert result["status"] == "swapped-healthy"
    assert log[env].count("apply:compute") == 2
    await assert_replays(client, env)


async def test_starter_prints_url_during_ttl(client, tq, capsys):
    env = new_env("starter")
    handle = await start_deploy(client, tq, env, ttl=600)
    await asyncio.wait_for(starter.print_url_when_healthy(handle), 30)

    out = capsys.readouterr().out
    assert f"Healthy: {URL}" in out
    assert f"--workflow-id deploy-{env} --name teardown_now" in out
    await handle.signal(DeployTestInstance.teardown_now)
    await asyncio.wait_for(handle.result(), 30)


async def test_starter_prints_nothing_when_deploy_fails(client, tq, capsys):
    env = new_env("starter-fail")
    handle = await start_deploy(client, tq, env, ttl=600, fail=True)
    await asyncio.wait_for(starter.print_url_when_healthy(handle), 30)

    assert capsys.readouterr().out == ""
    with pytest.raises(WorkflowFailureError):
        await handle.result()
