"""Topology file parsing.

The broker init container writes a topology file describing which peers
this agent is allowed to call and how to reach them, once, before the
agent container starts. It never changes during the pod's lifetime.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field

# DEFAULT_TOPOLOGY_PATH is the in-pod location the broker init container
# writes to. Keep in sync with kmv1.TopologyFilePath in kynoproj/kynomesh.
DEFAULT_TOPOLOGY_PATH = "/var/run/kynomesh/topology.json"

_KIND_MANAGED = "Managed"

# _topology_path is a test seam; production uses DEFAULT_TOPOLOGY_PATH.
_topology_path = DEFAULT_TOPOLOGY_PATH


class TopologyNotAvailableError(Exception):
    """Raised when the topology file is absent (e.g. running outside a Kynomesh pod)."""


class PeerNotFoundError(Exception):
    """Raised when the named peer is not in this agent's topology.

    Either because it does not exist in the AgentSet, or because the
    routing pattern forbids this agent from reaching it.
    """

    def __init__(self, name: str) -> None:
        super().__init__(f"kynomesh: peer not in topology: {name!r}")
        self.name = name


@dataclass(frozen=True)
class Peer:
    """Mirrors kmv1.Peer's JSON shape."""

    name: str
    kind: str = ""
    url: str = ""

    @property
    def is_managed(self) -> bool:
        """The CRD's default for an empty kind is Managed; "External" is the
        only other kmv1.PeerKind value.
        """
        return self.kind in ("", _KIND_MANAGED)


@dataclass(frozen=True)
class Topology:
    """Mirrors kmv1.Topology's JSON shape."""

    pattern: str = ""
    is_entry: bool = False
    peers: list[Peer] = field(default_factory=list)


def _parse_topology(raw: bytes) -> Topology:
    data = json.loads(raw)
    peers = [
        Peer(name=p["name"], kind=p.get("kind", ""), url=p.get("url", ""))
        for p in data.get("peers", [])
    ]
    return Topology(
        pattern=data.get("pattern", ""),
        is_entry=data.get("isEntry", False),
        peers=peers,
    )


def _read_topology(path: str) -> Topology:
    """Parses the topology file at path.

    Raises TopologyNotAvailableError when the file is absent (typical
    outside a Kynomesh pod).
    """
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        raise TopologyNotAvailableError from None
    return _parse_topology(raw)


# The topology file is written once by the init container before the
# agent container starts, and never changes during the pod's lifetime.
# Cache the parsed value (and any load error) for the process lifetime
# so that peer_names() + a loop of resolve_agent_card() doesn't re-read
# the file on every call.
_lock = threading.Lock()
_loaded = False
_cached_topology: Topology | None = None
_cached_error: Exception | None = None


def _load_topology() -> Topology:
    """Returns the parsed topology, reading the file at most once per process.

    Subsequent calls return the cached result.
    """
    global _loaded, _cached_topology, _cached_error
    with _lock:
        if not _loaded:
            try:
                _cached_topology = _read_topology(_topology_path)
            except Exception as e:  # noqa: BLE001
                _cached_error = e
            _loaded = True
        if _cached_error is not None:
            raise _cached_error
        assert _cached_topology is not None
        return _cached_topology


def _reset_topology_cache() -> None:
    """Drops the cached topology so the next call reloads from _topology_path.

    Test-only; not exposed as a public API.
    """
    global _loaded, _cached_topology, _cached_error
    with _lock:
        _loaded = False
        _cached_topology = None
        _cached_error = None


def lookup_peer(name: str) -> Peer:
    """Returns the peer entry with the given name.

    Raises PeerNotFoundError when no such peer exists in this agent's
    topology, or TopologyNotAvailableError when the topology file is
    absent.
    """
    topology = _load_topology()
    for peer in topology.peers:
        if peer.name == name:
            return peer
    raise PeerNotFoundError(name)


def peer_names() -> list[str]:
    """Returns the names of every peer in this agent's topology."""
    return [p.name for p in _load_topology().peers]
