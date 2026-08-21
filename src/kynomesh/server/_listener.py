"""Listener address resolution and server-info publishing.

start() binds two independent listeners, one for HTTP (AgentCard,
JSON-RPC, REST, /healthz) and one for gRPC (A2A gRPC transport,
grpc.health.v1). Each picks a Unix domain socket in-pod (POD_NAME set)
or a local TCP address for development.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from kynomesh.server import serverinfo

_ENV_POD_NAME = "POD_NAME"

BROKER_HTTP_SOCKET_PATH = "/var/run/kynomesh/broker-http.sock"
BROKER_GRPC_SOCKET_PATH = "/var/run/kynomesh/broker-grpc.sock"

DEFAULT_LOCAL_HTTP_ADDR = "127.0.0.1:8088"
DEFAULT_LOCAL_GRPC_ADDR = "127.0.0.1:8089"


@dataclass(frozen=True)
class ListenerConfig:
    network: str  # "tcp" or "unix"
    address: str

    @property
    def is_uds(self) -> bool:
        return self.network == "unix"


def _in_pod() -> bool:
    return bool(os.environ.get(_ENV_POD_NAME))


def resolve_listener(explicit: str | None, uds_default: str, tcp_default: str) -> ListenerConfig:
    """Resolves a single listener's network/address.

    An explicit absolute-path address opens a Unix domain socket; anything
    else supplied is treated as a TCP host:port. With no explicit address,
    picks the UDS default in-pod or the TCP default otherwise.
    """
    if explicit:
        network = "unix" if os.path.isabs(explicit) else "tcp"
        return ListenerConfig(network=network, address=explicit)
    if _in_pod():
        return ListenerConfig(network="unix", address=uds_default)
    return ListenerConfig(network="tcp", address=tcp_default)


def resolve_listeners(
    http_address: str | None, grpc_address: str | None
) -> tuple[ListenerConfig, ListenerConfig]:
    """Resolves the HTTP and gRPC listener targets."""
    http_cfg = resolve_listener(http_address, BROKER_HTTP_SOCKET_PATH, DEFAULT_LOCAL_HTTP_ADDR)
    grpc_cfg = resolve_listener(grpc_address, BROKER_GRPC_SOCKET_PATH, DEFAULT_LOCAL_GRPC_ADDR)
    return http_cfg, grpc_cfg


def prepare_uds_path(path: str) -> None:
    """Creates the socket directory and removes a stale socket file, if any."""
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    if os.path.exists(path):
        os.remove(path)


def write_server_info(http_cfg: ListenerConfig, path: str = serverinfo.DEFAULT_FILE_PATH) -> None:
    """Publishes the agent's metadata so the colocated broker can read it at startup."""
    protocol = serverinfo.PROTOCOL_UDS if http_cfg.is_uds else serverinfo.PROTOCOL_TCP
    serverinfo.write(path, serverinfo.default(protocol))
