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
# --mode=http. The gRPC default (kynoprobe --mode=grpc, empty service)
# is satisfied by the standard grpc.health.v1 service registered on the
# gRPC listener.
HEALTH_PATH = "/healthz"

# _GRPC_HEALTH_SERVICE is the service name kynoprobe checks by default
# (empty string, the overall server status).
_GRPC_HEALTH_SERVICE = ""


class Health:
    """Tracks and reports whether the agent is ready to serve traffic."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._serving = True
        self._grpc_servicer = None  # set by attach_grpc() when start() builds the gRPC server

    def set_serving(self, serving: bool) -> None:
        """Flips the reported status. True -> SERVING, False -> NOT_SERVING.

        Both the gRPC health service and the HTTP /healthz handler observe
        the change.
        """
        with self._lock:
            self._serving = serving
            servicer = self._grpc_servicer
        if servicer is not None:
            self._set_grpc_status(servicer, serving)

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

    def attach_grpc(self, server: "grpc.aio.Server") -> None:  # noqa: F821
        """Binds this Health to server and registers grpc.health.v1.

        Each start() call owns its own gRPC server; sharing one Health
        across concurrent start() calls is not supported.
        """
        from grpc_health.v1 import health, health_pb2_grpc

        servicer = health.HealthServicer()
        with self._lock:
            self._grpc_servicer = servicer
            serving = self._serving
        self._set_grpc_status(servicer, serving)
        health_pb2_grpc.add_HealthServicer_to_server(servicer, server)

    @staticmethod
    def _set_grpc_status(servicer: object, serving: bool) -> None:
        from grpc_health.v1.health_pb2 import HealthCheckResponse

        status = (
            HealthCheckResponse.SERVING if serving else HealthCheckResponse.NOT_SERVING
        )
        servicer.set(_GRPC_HEALTH_SERVICE, status)
