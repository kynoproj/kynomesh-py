"""TLS-verification policy for peer clients: who skips it, and how to opt in."""

import json
import logging

import httpx
import pytest
from a2a.client.client import ClientConfig
from a2a.types.a2a_pb2 import AgentCard

from kynomesh.client import _topology
from kynomesh.client import client as kynomesh_client
from kynomesh.client._managed_tls import managed_grpc_channel, managed_httpx_client
from kynomesh.client.client import (
    INSECURE_TLS_PEERS_ENV,
    _new_for_peer,
    _reset_peer_client_cache,
    default_httpx_client,
    resolve_agent_card,
)

PEER_URL = "https://calc.example:8490"


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    monkeypatch.delenv(INSECURE_TLS_PEERS_ENV, raising=False)
    _topology._reset_topology_cache()
    _reset_peer_client_cache()
    yield
    _topology._reset_topology_cache()
    _topology._topology_path = _topology.DEFAULT_TOPOLOGY_PATH
    _reset_peer_client_cache()


@pytest.fixture
def topology(tmp_path):
    def write(*peers):
        path = tmp_path / "topology.json"
        path.write_text(json.dumps({"peers": list(peers)}))
        _topology._topology_path = str(path)

    return write


@pytest.fixture
def recorded(monkeypatch):
    """Records the httpx client the card resolver gets and the config the factory gets."""
    seen = {}

    class FakeResolver:
        def __init__(self, httpx_client, base_url):
            seen["resolver_httpx"] = httpx_client

        async def get_agent_card(self):
            return AgentCard(name="calc")

    class FakeFactory:
        def __init__(self, config):
            seen["config"] = config

        def create(self, card):
            return object()

    monkeypatch.setattr(kynomesh_client, "A2ACardResolver", FakeResolver)
    monkeypatch.setattr(kynomesh_client, "ClientFactory", FakeFactory)
    monkeypatch.setattr(kynomesh_client, "record_peer_hash", lambda name, card: None)
    return seen


EXTERNAL = {"name": "calc", "kind": "External", "url": PEER_URL}
MANAGED = {"name": "calc", "kind": "Managed", "url": PEER_URL}


async def test_external_peer_verifies_tls_by_default(topology, recorded):
    topology(EXTERNAL)

    await _new_for_peer("calc")

    assert recorded["resolver_httpx"] is default_httpx_client
    assert recorded["config"].httpx_client is None
    assert recorded["config"].grpc_channel_factory is None


async def test_managed_peer_skips_tls_verification(topology, recorded):
    topology(MANAGED)

    await _new_for_peer("calc")

    assert recorded["resolver_httpx"] is managed_httpx_client
    assert recorded["config"].httpx_client is managed_httpx_client
    assert recorded["config"].grpc_channel_factory is managed_grpc_channel


async def test_external_peer_named_in_env_skips_tls_verification(topology, recorded, monkeypatch):
    topology(EXTERNAL)
    monkeypatch.setenv(INSECURE_TLS_PEERS_ENV, "calc")

    await _new_for_peer("calc")

    assert recorded["resolver_httpx"] is managed_httpx_client
    assert recorded["config"].httpx_client is managed_httpx_client
    assert recorded["config"].grpc_channel_factory is managed_grpc_channel
    assert recorded["config"].supported_protocol_bindings


@pytest.mark.parametrize("value", [" calc , other ", "other,,calc", "calc,"])
async def test_env_list_tolerates_whitespace_and_empty_entries(
    topology, recorded, monkeypatch, value
):
    topology(EXTERNAL)
    monkeypatch.setenv(INSECURE_TLS_PEERS_ENV, value)

    await _new_for_peer("calc")

    assert recorded["resolver_httpx"] is managed_httpx_client


@pytest.mark.parametrize("value", ["", "other", "calculator", "CALC"])
async def test_external_peer_not_named_in_env_still_verifies(
    topology, recorded, monkeypatch, value
):
    topology(EXTERNAL)
    monkeypatch.setenv(INSECURE_TLS_PEERS_ENV, value)

    await _new_for_peer("calc")

    assert recorded["resolver_httpx"] is default_httpx_client


async def test_only_the_named_external_peer_is_affected(topology, recorded, monkeypatch):
    topology(EXTERNAL, {"name": "search", "kind": "External", "url": "https://search.example"})
    monkeypatch.setenv(INSECURE_TLS_PEERS_ENV, "calc")

    await _new_for_peer("search")

    assert recorded["resolver_httpx"] is default_httpx_client


async def test_resolve_agent_card_honors_the_env(topology, recorded, monkeypatch):
    topology(EXTERNAL)

    await resolve_agent_card("calc")
    assert recorded["resolver_httpx"] is default_httpx_client

    monkeypatch.setenv(INSECURE_TLS_PEERS_ENV, "calc")
    await resolve_agent_card("calc")
    assert recorded["resolver_httpx"] is managed_httpx_client


async def test_caller_config_overrides_still_win(topology, recorded, monkeypatch):
    topology(EXTERNAL)
    monkeypatch.setenv(INSECURE_TLS_PEERS_ENV, "calc")
    custom = httpx.AsyncClient()

    await _new_for_peer("calc", ClientConfig(httpx_client=custom))

    assert recorded["config"].httpx_client is custom
    assert recorded["resolver_httpx"] is custom


async def test_warns_when_verification_is_disabled_for_an_external_peer(
    topology, recorded, monkeypatch, caplog
):
    topology(EXTERNAL)
    monkeypatch.setenv(INSECURE_TLS_PEERS_ENV, "calc")

    with caplog.at_level(logging.WARNING, logger="kynomesh.client.client"):
        await _new_for_peer("calc")

    assert "TLS verification disabled for external peer 'calc'" in caplog.text


async def test_no_warning_for_managed_peers_or_verified_external_peers(
    topology, recorded, monkeypatch, caplog
):
    with caplog.at_level(logging.WARNING, logger="kynomesh.client.client"):
        topology(MANAGED)
        monkeypatch.setenv(INSECURE_TLS_PEERS_ENV, "calc")
        await _new_for_peer("calc")

        _topology._reset_topology_cache()
        topology(EXTERNAL)
        monkeypatch.delenv(INSECURE_TLS_PEERS_ENV)
        await _new_for_peer("calc")

    assert "TLS verification disabled" not in caplog.text
