"""Readiness reporting for Kynomesh agent servers.

Health reports the agent's readiness to serve. A new Health starts SERVING;
flip it with set_serving(False) during graceful shutdown or backpressure.
A Health may be shared across asyncio tasks (asyncio is single-threaded, so
no lock is needed for the flag itself).
"""

from __future__ import annotations

import threading

from starlette.requests import Request
from starlette.responses import PlainTextResponse

# HEALTH_PATH is the HTTP endpoint kynoprobe hits when invoked with
# --mode=http.
HEALTH_PATH = "/healthz"


class Health:
    """Tracks and reports whether the agent is ready to serve traffic."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._serving = True

    def set_serving(self, serving: bool) -> None:
        """Flips the reported status. True -> SERVING, False -> NOT_SERVING."""
        with self._lock:
            self._serving = serving

    def is_serving(self) -> bool:
        with self._lock:
            return self._serving

    async def http_endpoint(self, request: Request) -> PlainTextResponse:
        """Starlette endpoint reporting the current status as plain text.

        200 when SERVING, 503 otherwise.
        """
        del request
        if self.is_serving():
            return PlainTextResponse("SERVING\n", status_code=200)
        return PlainTextResponse("NOT_SERVING\n", status_code=503)
