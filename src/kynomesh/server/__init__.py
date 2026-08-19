"""In-agent A2A server helper for Kynomesh.

start() binds a UDS at /var/run/kynomesh/broker.sock in-pod (POD_NAME set)
or 127.0.0.1:8088 locally, and mounts the JSON-RPC and REST transports
listed in card.supported_interfaces.
"""

from kynomesh.server.health import HEALTH_PATH, Health
from kynomesh.server.server import (
    Option,
    start,
    with_address,
    with_health,
    with_shutdown_timeout,
    with_task_store,
)

__all__ = [
    "HEALTH_PATH",
    "Health",
    "Option",
    "start",
    "with_address",
    "with_health",
    "with_shutdown_timeout",
    "with_task_store",
]
