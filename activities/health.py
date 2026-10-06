import httpx
from dataclasses import dataclass
from temporalio import activity
from temporalio.exceptions import ApplicationError


@dataclass
class HealthResult:
    url: str
    status_code: int


@activity.defn
async def http_health_check(url: str) -> HealthResult:
    activity.logger.info(f"GET {url}")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
    except httpx.ConnectError:
        # Instance not reachable yet (booting, SG, IGW). Slim message so
        # the failure payload doesn't blow Temporal's size limit.
        activity.logger.warning(f"http_health_check attempt {activity.info().attempt}: {url} not reachable yet")
        raise RuntimeError(f"connect: {url} not reachable yet") from None
    except httpx.TimeoutException:
        activity.logger.warning(f"http_health_check attempt {activity.info().attempt}: {url} timeout (>10s)")
        raise RuntimeError(f"timeout: {url} took >10s") from None
    except Exception as e:
        activity.logger.warning(f"unexpected health error: {e}")
        raise RuntimeError(f"health error: {type(e).__name__}: {e}") from None

    # Raise instead of returning a failed result so the retry policy decides.
    # A 5xx can clear up while the instance finishes booting; any other non-2xx
    # (4xx, redirect) won't fix itself, so fail fast.
    if resp.status_code >= 500:
        activity.logger.warning(f"http_health_check attempt {activity.info().attempt}: {url} returned {resp.status_code}")
        raise ApplicationError(f"HTTP {resp.status_code} from {url}", type="HealthCheckServerError")
    if not 200 <= resp.status_code < 300:
        raise ApplicationError(
            f"HTTP {resp.status_code} from {url}", type="HealthCheckBadStatus", non_retryable=True
        )
    return HealthResult(url=url, status_code=resp.status_code)
