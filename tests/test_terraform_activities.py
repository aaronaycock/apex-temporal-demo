"""Terraform activities against a fake `terraform` (tests/bin/terraform)."""
import asyncio
import dataclasses
import os
from pathlib import Path

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

import activities.terraform as tf

BIN = Path(__file__).parent / "bin"


class FakeTerraform:
    def __init__(self, tmp: Path):
        self.tmp = tmp
        self.log = tmp / "terraform.log"

    def calls(self) -> list[str]:
        return self.log.read_text().splitlines() if self.log.exists() else []

    def subcommands(self) -> list[str]:
        return [c.split()[0] for c in self.calls() if not c.startswith("SIGINT")]

    def clear(self) -> None:
        self.log.unlink(missing_ok=True)


@pytest.fixture
def fake_tf(tmp_path, monkeypatch) -> FakeTerraform:
    """Put the fake terraform first on PATH and keep workspaces in tmp_path."""
    monkeypatch.setenv("PATH", f"{BIN}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_TF_LOG", str(tmp_path / "terraform.log"))
    monkeypatch.setattr(tf, "WORKDIR_BASE", tmp_path / "_workspaces")
    return FakeTerraform(tmp_path)


async def plan(env: str) -> tf.PlanResult:
    return await ActivityEnvironment().run(tf.run_terraform_plan, "network", env, {"env_name": env})


@pytest.mark.parametrize(
    "stderr, error_type, permanent",
    [
        ("Error: Invalid value for variable", "TerraformConfigError", True),
        ("No valid credential sources found", "AwsCredentialsError", True),
        ("api error ExpiredToken: token expired", "AwsCredentialsError", True),
        ("VpcLimitExceeded: The maximum number of VPCs", "AwsQuotaError", True),
        ("RequestLimitExceeded: Request limit exceeded", "TerraformError", False),
        ("Error: Error acquiring the state lock", "TerraformError", False),
        ("Error: Saved plan is stale", "TerraformError", False),
    ],
)
def test_error_classification(stderr, error_type, permanent):
    err = tf._tf_error("apply", stderr)
    assert (err.type, err.non_retryable) == (error_type, permanent)


async def test_plan_writes_its_own_plan_file(fake_tf):
    result = await plan("e1")
    assert result.has_changes
    assert Path(result.plan_path).name.startswith("tfplan-")
    assert Path(result.plan_path).exists()

    calls = fake_tf.calls()
    assert any(c.startswith("init ") and "-reconfigure" in c for c in calls)
    assert any(c.startswith("plan ") and "-lock-timeout=2m" in c for c in calls)
    assert all("TF_CLI_ARGS=-no-color" in c for c in calls)


async def test_first_attempt_applies_saved_plan(fake_tf):
    planned = await plan("e1")
    fake_tf.clear()

    result = await ActivityEnvironment().run(
        tf.run_terraform_apply, "network", "e1", {"env_name": "e1"}, planned.plan_path
    )
    assert fake_tf.subcommands() == ["apply", "output"]
    assert not Path(planned.plan_path).exists(), "plan file should be removed after apply"
    assert result.outputs == {"subnet_id": "subnet-1"}


async def test_retried_apply_replans(fake_tf):
    saved = fake_tf.tmp / "saved-plan.bin"
    saved.touch()  # exists, so only the attempt number triggers the re-plan
    env = ActivityEnvironment()
    env.info = dataclasses.replace(env.info, attempt=2)

    await env.run(tf.run_terraform_apply, "network", "e2", {"env_name": "e2"}, str(saved))

    assert fake_tf.subcommands() == ["init", "plan", "apply", "output"]
    apply_call = next(c for c in fake_tf.calls() if c.startswith("apply "))
    assert str(saved) not in apply_call, "should apply the fresh plan, not the saved one"


async def test_apply_without_plan_file_replans(fake_tf):
    # e.g. apply landed on a different worker host than its plan
    await ActivityEnvironment().run(
        tf.run_terraform_apply, "network", "e3", {"env_name": "e3"}, "/nonexistent/tfplan.bin"
    )
    assert fake_tf.subcommands() == ["init", "plan", "apply", "output"]


@pytest.mark.parametrize(
    "stderr, permanent",
    [("Error: Saved plan is stale", False), ("Error: Unsupported argument", True)],
)
async def test_apply_failure_retryability(fake_tf, monkeypatch, stderr, permanent):
    planned = await plan("e4")
    monkeypatch.setenv("FAKE_TF_APPLY_ERR", stderr)
    with pytest.raises(ApplicationError) as err:
        await ActivityEnvironment().run(
            tf.run_terraform_apply, "network", "e4", {"env_name": "e4"}, planned.plan_path
        )
    assert err.value.non_retryable is permanent


async def test_cancel_interrupts_terraform(fake_tf, monkeypatch):
    planned = await plan("e5")
    monkeypatch.setenv("FAKE_TF_APPLY_SLEEP", "100")  # ~20s unless interrupted
    env = ActivityEnvironment()
    task = asyncio.create_task(
        env.run(tf.run_terraform_apply, "network", "e5", {"env_name": "e5"}, planned.plan_path)
    )
    async with asyncio.timeout(10):
        while not any(c.startswith("apply ") for c in fake_tf.calls()):
            await asyncio.sleep(0.05)

    env.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 10)
    assert "SIGINT received" in fake_tf.calls(), "terraform should be interrupted, not orphaned"
