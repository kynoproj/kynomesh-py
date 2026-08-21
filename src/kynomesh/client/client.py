"""Peer-discovery helpers for Kynomesh agents.

In-pod, the broker init container writes a topology file describing
which peers this agent is allowed to call and how to reach them. This
module wraps that file so user code does not need to know its path or
format:

    url = peer_url("worker-a")               # just the URL
    card = await resolve_agent_card("worker-a")
    client = await new_for_peer("worker-a")   # ready-to-use a2a Client
"""

from __future__ import annotations

import httpx
from a2a.client.card_resolver import A2ACardResolver
from a2a.client.client import Client, ClientConfig
from a2a.client.client_factory import ClientFactory
from a2a.types.a2a_pb2 import AgentCard
from a2a.utils.constants import TransportProtocol

from kynomesh.client._managed_tls import managed_grpc_channel, managed_httpx_client
from kynomesh.client._topology import (
    PeerNotFoundError,
    TopologyNotAvailableError,
    lookup_peer,
    peer_names as _peer_names,
)

__all__ = [
    "PeerNotFoundError",
    "TopologyNotAvailableError",
    "peer_url",
    "peers",
    "resolve_agent_card",
    "new_for_peer",
]

# default_httpx_client is used to fetch AgentCards and build clients for
# non-Managed (External) peers, which are expected to present real,
# CA-verifiable certificates.
default_httpx_client = httpx.AsyncClient(timeout=30.0)


def peer_url(name: str) -> str:
    """Returns the broker URL of the named peer.

    Raises PeerNotFoundError if the peer is not in this agent's
    topology, or TopologyNotAvailableError when running outside a
    Kynomesh pod.
    """
    peer = lookup_peer(name)
    if not peer.url:
        raise ValueError(f"kynomesh: peer {name!r} has no URL in topology")
    return peer.url


def peers() -> list[str]:
    """Returns the names of every peer this agent is allowed to discover.

    Raises TopologyNotAvailableError when running outside a Kynomesh pod.
    """
    return _peer_names()


async def resolve_agent_card(name: str) -> AgentCard:
    """Fetches the AgentCard of the named peer from its well-known location.

    The peer URL is looked up in the topology. For Managed peers, TLS
    verification is skipped on the card fetch.
    """
    peer = lookup_peer(name)
    if not peer.url:
        raise ValueError(f"kynomesh: peer {name!r} has no URL in topology")

    httpx_client = managed_httpx_client if peer.is_managed else default_httpx_client
    resolver = A2ACardResolver(httpx_client, peer.url)
    return await resolver.get_agent_card()


async def new_for_peer(name: str, config: ClientConfig | None = None) -> Client:
    """Returns an a2a Client wired to the named peer.

    Performs the full peer-discovery flow: look up the peer URL in the
    topology, resolve its AgentCard, and construct a client over one of
    the interfaces the card advertises. For Managed peers, TLS
    verification is skipped for both the AgentCard fetch and the client
    transport, unless config overrides those defaults.
    """
    peer = lookup_peer(name)
    if not peer.url:
        raise ValueError(f"kynomesh: peer {name!r} has no URL in topology")

    config = config or ClientConfig()
    if peer.is_managed:
        if config.httpx_client is None:
            config.httpx_client = managed_httpx_client
        if config.grpc_channel_factory is None:
            config.grpc_channel_factory = managed_grpc_channel
        if not config.supported_protocol_bindings:
            config.supported_protocol_bindings = [
                TransportProtocol.JSONRPC,
                TransportProtocol.HTTP_JSON,
                TransportProtocol.GRPC,
            ]

    resolver = A2ACardResolver(config.httpx_client or default_httpx_client, peer.url)
    card = await resolver.get_agent_card()

    factory = ClientFactory(config)
    return factory.create(card)
