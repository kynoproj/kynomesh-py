"""In-agent A2A server helper for Kynomesh.

start() binds two independent listeners, one for HTTP (AgentCard,
JSON-RPC, REST, /healthz) and one for gRPC (A2A gRPC transport,
grpc.health.v1); locally each defaults to its own TCP port
(127.0.0.1:8088 for HTTP, 127.0.0.1:8089 for gRPC). start() mounts the
transports listed in card.supported_interfaces.
"""

from kynomesh.server.health import HEALTH_PATH, Health
from kynomesh.server.server import (
    Option,
    start,
    with_grpc_address,
    with_health,
    with_http_address,
    with_shutdown_timeout,
    with_task_store,
)

__all__ = [
    "HEALTH_PATH",
    "Health",
    "Option",
    "start",
    "with_grpc_address",
    "with_health",
    "with_http_address",
    "with_shutdown_timeout",
    "with_task_store",
]
