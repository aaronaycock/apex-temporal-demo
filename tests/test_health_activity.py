"""http_health_check against a local server that answers with the status
code in the path (GET /503 -> 503)."""
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from activities.health import http_health_check


class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(int(self.path.strip("/")))
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def server():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), StatusHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


async def test_2xx_returns_result(server):
    result = await ActivityEnvironment().run(http_health_check, f"{server}/200")
    assert result.status_code == 200


@pytest.mark.parametrize(
    "status, error_type, permanent",
    [
        (503, "HealthCheckServerError", False),  # may clear up while booting
        (404, "HealthCheckBadStatus", True),
        (301, "HealthCheckBadStatus", True),
    ],
)
async def test_non_2xx_raises(server, status, error_type, permanent):
    with pytest.raises(ApplicationError) as err:
        await ActivityEnvironment().run(http_health_check, f"{server}/{status}")
    assert (err.value.type, err.value.non_retryable) == (error_type, permanent)


async def test_unreachable_is_retryable():
    # A plain RuntimeError, which the retry policy retries.
    with pytest.raises(RuntimeError, match="not reachable"):
        await ActivityEnvironment().run(http_health_check, "http://127.0.0.1:1/")
