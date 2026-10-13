"""Peer-discovery helpers for Kynomesh agents.

In-pod, the broker init container writes a topology file describing
which peers this agent is allowed to call and how to reach them. This
module wraps that file so user code does not need to know its path or
format:

    url = peer_url("worker-a")               # just the URL
    card = await resolve_agent_card("worker-a")
    client = await peer_client("worker-a")    # ready-to-use a2a Client, cached per peer
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping

import httpx
from a2a.client.card_resolver import A2ACardResolver
from a2a.client.client import Client, ClientConfig
from a2a.client.client_factory import ClientFactory
from a2a.types.a2a_pb2 import AgentCard
from a2a.utils.constants import TransportProtocol

from kynomesh.client._cache import AsyncKeyedCache
from kynomesh.client._hash import record_peer_hash
from kynomesh.client._headers import validate_headers
from kynomesh.client._managed_tls import managed_grpc_channel, managed_httpx_client
from kynomesh.client._topology import (
    Peer,
    PeerNotFoundError,
    TopologyNotAvailableError,
    lookup_peer,
)
from kynomesh.client._topology import (
    peer_names as _peer_names,
)

__all__ = [
    "PeerNotFoundError",
    "TopologyNotAvailableError",
    "peer_url",
    "peers",
    "resolve_agent_card",
    "peer_client",
    "forget_peer",
]

# default_httpx_client is used to fetch AgentCards and build clients for
# non-Managed (External) peers, which are expected to present real,
# CA-verifiable certificates.
default_httpx_client = httpx.AsyncClient(timeout=30.0)

_logger = logging.getLogger(__name__)

INSECURE_TLS_PEERS_ENV = "KYNOMESH_INSECURE_TLS_PEERS"
"""Comma-separated names of External peers whose TLS certificate is not verified.

An External peer is normally expected to present a CA-verifiable certificate. The
exception is an agent in another AgentSet, referenced via `externalAgents`: its
broker presents a self-signed certificate, which can never verify. Naming such a
peer here gives it the same treatment Managed peers get (encrypted, but the peer
is not authenticated). Managed peers need no entry.
"""


def _insecure_tls_peers() -> frozenset[str]:
    raw = os.environ.get(INSECURE_TLS_PEERS_ENV, "")
    return frozenset(name.strip() for name in raw.split(",") if name.strip())


def _skips_tls_verify(peer: Peer) -> bool:
    """Whether TLS verification is skipped for the peer.

    Always for Managed peers; for External peers only when named in
    KYNOMESH_INSECURE_TLS_PEERS. Read on every call so it can change at runtime.
    """
    return peer.is_managed or peer.name in _insecure_tls_peers()


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


async def resolve_agent_card(name: str, *, headers: Mapping[str, str] | None = None) -> AgentCard:
    """Fetches the AgentCard of the named peer from its well-known location.

    The peer URL is looked up in the topology. For Managed peers, TLS
    verification is skipped on the card fetch.

    `headers` are sent on the card request only, for a peer whose card
    requires auth. Redirects are not followed, so they are never re-sent to
    another host. Raises ValueError if a header is malformed.
    """
    peer = lookup_peer(name)
    if not peer.url:
        raise ValueError(f"kynomesh: peer {name!r} has no URL in topology")

    httpx_client = managed_httpx_client if _skips_tls_verify(peer) else default_httpx_client
    resolver = A2ACardResolver(httpx_client, peer.url)
    return await resolver.get_agent_card(**_card_request_kwargs(headers))


def _card_request_kwargs(headers: Mapping[str, str] | None) -> dict[str, dict[str, object]]:
    """Keyword arguments for `get_agent_card` that carry `headers`, if any.

    Redirects are refused whatever the HTTP client's default: httpx would re-send
    custom headers to the redirect target, which could be another host.
    """
    if not headers:
        return {}
    return {"http_kwargs": {"headers": validate_headers(headers), "follow_redirects": False}}


async def _new_for_peer(
    name: str,
    config: ClientConfig | None = None,
    *,
    card_headers: Mapping[str, str] | None = None,
) -> Client:
    """Returns an a2a Client wired to the named peer.

    Performs the full peer-discovery flow: look up the peer URL in the
    topology, resolve its AgentCard, and construct a client over one of
    the interfaces the card advertises. For Managed peers, TLS
    verification is skipped for both the AgentCard fetch and the client
    transport, unless config overrides those defaults. The same applies to
    External peers named in KYNOMESH_INSECURE_TLS_PEERS.

    Unexported: this always builds a fresh client (a full AgentCard
    resolve + construction), never reuses a cached one. peer_client is
    the public, cached entry point built on top of this.

    `card_headers`, if given, are sent on the AgentCard fetch only, never on
    calls made through the returned client.

    Also records a hash of the resolved AgentCard for drift detection,
    on a best-effort basis: a failure to record the hash must not fail
    the client build the caller actually asked for.
    """
    peer = lookup_peer(name)
    if not peer.url:
        raise ValueError(f"kynomesh: peer {name!r} has no URL in topology")

    config = config or ClientConfig()
    if not peer.is_managed and _skips_tls_verify(peer):
        _logger.warning(
            "kynomesh: TLS verification disabled for external peer %r (listed in %s)",
            name,
            INSECURE_TLS_PEERS_ENV,
        )
    if _skips_tls_verify(peer):
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

    if card_headers and _skips_tls_verify(peer):
        _logger.warning(
            "kynomesh: sending card headers to peer %r without TLS verification; "
            "they can be read by anyone on the network path",
            name,
        )

    resolver = A2ACardResolver(config.httpx_client or default_httpx_client, peer.url)
    card = await resolver.get_agent_card(**_card_request_kwargs(card_headers))

    try:
        record_peer_hash(name, card)
    except Exception:  # noqa: BLE001
        pass

    return ClientFactory(config).create(card)


# _peer_client_cache lazily builds and caches one Client per peer name
# for the life of the process. Concurrency scope is explicitly
# single-event-loop asyncio; see peer_client's docstring.
_peer_client_cache: AsyncKeyedCache[Client] = AsyncKeyedCache(_new_for_peer)


async def peer_client(name: str, *, card_headers: Mapping[str, str] | None = None) -> Client:
    """Returns the cached a2a Client for the named peer, building it on
    first use and reusing it on every subsequent call for the same
    peer name.

    Construction is lazy: a peer never gets a client built or its
    AgentCard resolved until the first peer_client call for that name.
    It is also safe under concurrent first-use of the same peer from
    multiple coroutines: only one caller builds the client, and the
    rest await its result.

    Concurrency scope is explicitly single-event-loop asyncio, not
    cross-thread. Do not share a cached peer client across OS threads
    or multiple event loops.

    `card_headers` (for example `{"Authorization": "Bearer ..."}`) are sent
    on the AgentCard fetch only, for a peer that protects its card. They are
    NOT attached to calls made through the client: authenticate each call
    yourself, with a `ClientCallContext(service_parameters=...)` per call or
    an interceptor in `ClientConfig`. They are applied only when the client
    is first built; once it is cached, later `card_headers` are ignored. Call
    forget_peer(name) first to rebuild with new ones. Raises ValueError if a
    header is malformed.

    Callers should not call close() on the returned client: it is
    shared and reused by later callers. Use forget_peer(name) to drop
    the cached entry instead.
    """
    return await _peer_client_cache.get(name, card_headers=card_headers)


def forget_peer(name: str) -> None:
    """Drops the cached client for the named peer, if any.

    The next peer_client call for that name resolves the AgentCard and
    builds a fresh client. Safe to call for a peer with no cached
    entry.
    """
    _peer_client_cache.forget(name)


def _reset_peer_client_cache() -> None:
    """Drops every cached peer client. Test-only; not exposed as a public API."""
    _peer_client_cache.reset()
