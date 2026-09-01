"""Peer-discovery helpers for Kynomesh agents.

In-pod, the broker init container writes a topology file describing
which peers this agent is allowed to call and how to reach them:

    from kynomesh import client

    url = client.peer_url("worker-a")
    card = await client.resolve_agent_card("worker-a")
    c = await client.peer_client("worker-a")
"""

from kynomesh.client.client import (
    PeerNotFoundError,
    TopologyNotAvailableError,
    forget_peer,
    peer_client,
    peer_url,
    peers,
    resolve_agent_card,
)

__all__ = [
    "PeerNotFoundError",
    "TopologyNotAvailableError",
    "peer_client",
    "forget_peer",
    "peer_url",
    "peers",
    "resolve_agent_card",
]
