"""Listener address resolution and server-info publishing.

Picks a Unix domain socket in-pod (POD_NAME set) or a local TCP address 
for development.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from kynomesh.server import serverinfo

_ENV_POD_NAME = "POD_NAME"
BROKER_SOCKET_PATH = "/var/run/kynomesh/broker.sock"
DEFAULT_LOCAL_ADDR = "127.0.0.1:8088"


@dataclass(frozen=True)
class ListenerConfig:
    network: str  # "tcp" or "unix"
    address: str

    @property
    def is_uds(self) -> bool:
        return self.network == "unix"


def _in_pod() -> bool:
    return bool(os.environ.get(_ENV_POD_NAME))


def resolve_listener(address: str | None) -> ListenerConfig:
    """Resolves the listener network/address for this process.

    An explicit absolute-path address opens a Unix domain socket; anything
    else supplied is treated as a TCP host:port. With no explicit address,
    picks a UDS in-pod or a local TCP address otherwise.
    """
    if address:
        network = "unix" if os.path.isabs(address) else "tcp"
        return ListenerConfig(network=network, address=address)
    if _in_pod():
        return ListenerConfig(network="unix", address=BROKER_SOCKET_PATH)
    return ListenerConfig(network="tcp", address=DEFAULT_LOCAL_ADDR)


def prepare_uds_path(path: str) -> None:
    """Creates the socket directory and removes a stale socket file, if any."""
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    if os.path.exists(path):
        os.remove(path)


def write_server_info(cfg: ListenerConfig, path: str = serverinfo.DEFAULT_FILE_PATH) -> None:
    """Publishes the agent's metadata so the colocated broker can read it at startup."""
    protocol = serverinfo.PROTOCOL_UDS if cfg.is_uds else serverinfo.PROTOCOL_TCP
    serverinfo.write(path, serverinfo.default(protocol))
