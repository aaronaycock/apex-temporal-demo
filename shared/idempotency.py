import hashlib
from temporalio import activity


def idempotency_key(business_key: str = "") -> str:
    """
    Deterministic per-attempt key: WorkflowRunID + ActivityID + business_key.

    Use inside activities. Pair with downstream tokens (AWS ClientToken,
    plan-file hash) so retries are safe.
    """
    info = activity.info()
    raw = f"{info.workflow_run_id}:{info.activity_id}:{business_key}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]