"""Preflight: verify local tools, AWS access, the state bucket, and Temporal.

Run this before a demo: `python check.py`. Exits non-zero if anything is missing.
"""
import asyncio
import re
import shutil
import subprocess
import sys
from pathlib import Path

from shared.config import (
    TEMPORAL_ADDRESS,
    TEMPORAL_NAMESPACE,
    TF_STATE_BUCKET,
    TF_STATE_REGION,
)

REPO_ROOT = Path(__file__).resolve().parent
failures = 0


def report(ok: bool, label: str, detail: str = "") -> None:
    global failures
    if not ok:
        failures += 1
    mark = "ok  " if ok else "FAIL"
    print(f"[{mark}] {label}" + (f": {detail}" if detail else ""))


def check_terraform() -> None:
    if not shutil.which("terraform"):
        report(False, "terraform on PATH", "install Terraform >= 1.10")
        return
    out = subprocess.run(["terraform", "version"], capture_output=True, text=True).stdout
    first = out.splitlines()[0] if out else ""
    # S3 native state locking (use_lockfile) needs Terraform 1.10+.
    m = re.search(r"v(\d+)\.(\d+)", first)
    ok = bool(m) and (int(m[1]), int(m[2])) >= (1, 10)
    report(ok, "terraform on PATH", first if ok else f"{first or 'unknown version'}; need >= 1.10")


def check_modules() -> None:
    for m in ["network", "compute", "app"]:
        p = REPO_ROOT / "terraform" / "modules" / m / "main.tf"
        report(p.exists(), f"module {m}", "" if p.exists() else f"missing {p}")


def check_aws() -> None:
    try:
        import boto3

        ident = boto3.client("sts").get_caller_identity()
        report(True, "AWS credentials", ident["Arn"])
    except Exception as e:
        report(False, "AWS credentials", str(e))
        return

    if not TF_STATE_BUCKET:
        report(False, "TF_STATE_BUCKET", "not set in .env")
        return
    try:
        boto3.client("s3", region_name=TF_STATE_REGION).head_bucket(Bucket=TF_STATE_BUCKET)
        report(True, "state bucket", f"s3://{TF_STATE_BUCKET}")
    except Exception as e:
        report(False, "state bucket", f"s3://{TF_STATE_BUCKET}: {e}")


async def check_temporal() -> None:
    from temporalio.client import Client

    try:
        await asyncio.wait_for(
            Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE), timeout=5
        )
        report(True, "Temporal server", f"{TEMPORAL_ADDRESS} ({TEMPORAL_NAMESPACE})")
    except Exception as e:
        report(False, "Temporal server", f"{TEMPORAL_ADDRESS}: {e or type(e).__name__}")


if __name__ == "__main__":
    check_terraform()
    check_modules()
    check_aws()
    asyncio.run(check_temporal())
    sys.exit(1 if failures else 0)
