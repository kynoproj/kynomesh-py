"""Card headers for peers that protect their AgentCard: sent on the card fetch only."""

import json
import uuid

import httpx
import pytest
from a2a.client.client import ClientCallContext, ClientConfig
from a2a.client.errors import AgentCardResolutionError
from a2a.types.a2a_pb2 import (
    AgentCard,
    AgentInterface,
    Message,
    Part,
    Role,
    SendMessageRequest,
)
from a2a.utils.constants import TransportProtocol
from google.protobuf.json_format import MessageToDict

from kynomesh.client import _topology
from kynomesh.client import client as kynomesh_client
from kynomesh.client._headers import validate_headers
from kynomesh.client.client import _new_for_peer, _reset_peer_client_cache, resolve_agent_card

PEER_URL = "https://calc.example:8490"
AUTH = {"Authorization": "Bearer secret"}
CARD_KWARGS = {"http_kwargs": {"headers": AUTH, "follow_redirects": False}}
EXTERNAL = {"name": "calc", "kind": "External", "url": PEER_URL}


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    monkeypatch.delenv(kynomesh_client.INSECURE_TLS_PEERS_ENV, raising=False)
    _topology._reset_topology_cache()
    _reset_peer_client_cache()
    yield
    _topology._reset_topology_cache()
    _topology._topology_path = _topology.DEFAULT_TOPOLOGY_PATH
    _reset_peer_client_cache()


@pytest.fixture
def topology(tmp_path):
    path = tmp_path / "topology.json"
    path.write_text(json.dumps({"peers": [EXTERNAL]}))
    _topology._topology_path = str(path)


@pytest.fixture
def recorded(monkeypatch):
    """Records what the card resolver and the client factory are given."""
    seen = {"resolves": [], "interceptors": None, "builds": 0}

    class FakeResolver:
        def __init__(self, httpx_client, base_url):
            pass

        async def get_agent_card(self, **kwargs):
            seen["resolves"].append(kwargs)
            return AgentCard(name="calc")

    class FakeFactory:
        def __init__(self, config):
            pass

        def create(self, card, interceptors=None):
            seen["builds"] += 1
            seen["interceptors"] = interceptors
            return object()

    monkeypatch.setattr(kynomesh_client, "A2ACardResolver", FakeResolver)
    monkeypatch.setattr(kynomesh_client, "ClientFactory", FakeFactory)
    monkeypatch.setattr(kynomesh_client, "record_peer_hash", lambda name, card: None)
    return seen


# --- validate_headers -------------------------------------------------------


def test_validate_headers_returns_an_independent_copy():
    source = {"X-Api-Key": "k"}

    copy = validate_headers(source)
    source["X-Api-Key"] = "changed"

    assert copy == {"X-Api-Key": "k"}


@pytest.mark.parametrize(
    "bad",
    [
        {"": "v"},
        {"  ": "v"},
        {"Authorization": 1},
        {1: "v"},
        {"Authorization": "a\r\nX-Injected: 1"},
        {"X-A: b\r\nX-Evil": "1"},
        {"X A": "v"},
        {"X-A:": "v"},
        {"X+Key": "v"},
        {"grpc-timeout": "1S"},
        {"Grpc-Encoding": "gzip"},
        {"Authorization": "nul\x00byte"},
        {"Authorization": "caf\u00e9"},
        {"Authorization": "a", "authorization": "b"},
    ],
)
def test_validate_headers_rejects_malformed_entries(bad):
    with pytest.raises(ValueError):
        validate_headers(bad)


def test_validate_headers_error_does_not_echo_values():
    with pytest.raises(ValueError) as err:
        validate_headers({"Authorization": "Bearer secret\n"})

    assert "secret" not in str(err.value)


# --- resolve_agent_card / _new_for_peer -------------------------------------


async def test_resolve_agent_card_sends_headers_on_the_card_fetch(topology, recorded):
    await resolve_agent_card("calc", headers=AUTH)

    assert recorded["resolves"] == [CARD_KWARGS]


async def test_resolve_agent_card_without_headers_is_unchanged(topology, recorded):
    await resolve_agent_card("calc")

    assert recorded["resolves"] == [{}]


async def test_new_for_peer_sends_card_headers_on_the_card_fetch_only(topology, recorded):
    await _new_for_peer("calc", card_headers=AUTH)

    assert recorded["resolves"] == [CARD_KWARGS]
    assert recorded["interceptors"] is None


async def test_new_for_peer_rejects_malformed_headers_before_any_request(topology, recorded):
    with pytest.raises(ValueError):
        await _new_for_peer("calc", card_headers={"Authorization": 1})

    assert recorded["resolves"] == []


# --- peer_client caching ----------------------------------------------------


async def test_peer_client_applies_card_headers_when_it_first_builds_the_client(topology, recorded):
    await kynomesh_client.peer_client("calc", card_headers=AUTH)

    assert recorded["resolves"] == [CARD_KWARGS]


async def test_peer_client_headers_apply_on_first_build_only(topology, recorded):
    first = await kynomesh_client.peer_client("calc", card_headers=AUTH)
    second = await kynomesh_client.peer_client(
        "calc", card_headers={"Authorization": "Bearer other"}
    )

    assert second is first
    assert recorded["builds"] == 1


async def test_forget_peer_lets_new_headers_take_effect(topology, recorded):
    await kynomesh_client.peer_client("calc", card_headers=AUTH)
    kynomesh_client.forget_peer("calc")
    await kynomesh_client.peer_client("calc", card_headers={"Authorization": "Bearer other"})

    assert recorded["resolves"][-1] == {
        "http_kwargs": {"headers": {"Authorization": "Bearer other"}, "follow_redirects": False}
    }


async def test_failed_build_with_headers_is_not_cached(topology, monkeypatch, recorded):
    with pytest.raises(ValueError):
        await kynomesh_client.peer_client("calc", card_headers={"Authorization": 1})

    await kynomesh_client.peer_client("calc", card_headers=AUTH)

    assert recorded["builds"] == 1


# --- through the real a2a client -------------------------------------------


async def test_card_headers_are_not_sent_on_real_a2a_calls(topology, monkeypatch):
    card_auths, call_auths = [], []

    def handler(request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("authorization")
        if request.url.path.endswith("agent-card.json"):
            card_auths.append(auth)
            card = AgentCard(
                name="calc",
                supported_interfaces=[
                    AgentInterface(
                        url=f"{PEER_URL}/a2a/jsonrpc",
                        protocol_binding=TransportProtocol.JSONRPC.value,
                    )
                ],
                default_input_modes=["text"],
                default_output_modes=["text"],
            )
            return httpx.Response(200, json=MessageToDict(card))
        call_auths.append(auth)
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": json.loads(request.content)["id"],
                "result": {
                    "message": {"messageId": "m", "role": "ROLE_AGENT", "parts": [{"text": "pong"}]}
                },
            },
        )

    mock = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(kynomesh_client, "default_httpx_client", mock)
    monkeypatch.setattr(
        kynomesh_client,
        "ClientConfig",
        lambda: ClientConfig(
            streaming=False,
            httpx_client=mock,
            supported_protocol_bindings=[TransportProtocol.JSONRPC],
        ),
    )
    monkeypatch.setattr(kynomesh_client, "record_peer_hash", lambda name, card: None)

    a2a_client = await kynomesh_client.peer_client("calc", card_headers=AUTH)
    request = SendMessageRequest(
        message=Message(
            message_id=str(uuid.uuid4()), role=Role.ROLE_USER, parts=[Part(text="ping")]
        )
    )
    async for _ in a2a_client.send_message(request):
        pass
    async for _ in a2a_client.send_message(
        request, context=ClientCallContext(service_parameters={"Authorization": "Bearer fresh"})
    ):
        pass

    assert card_auths == ["Bearer secret"]
    assert call_auths == [None, "Bearer fresh"]


# --- credentials only go where the peer was configured ----------------------


async def test_warns_when_headers_go_to_a_peer_without_tls_verification(
    topology, recorded, monkeypatch, caplog
):
    monkeypatch.setenv(kynomesh_client.INSECURE_TLS_PEERS_ENV, "calc")

    with caplog.at_level("WARNING", logger="kynomesh.client.client"):
        await _new_for_peer("calc", card_headers=AUTH)

    assert "card headers" in caplog.text and "'calc'" in caplog.text
    assert "secret" not in caplog.text


async def test_no_tls_warning_when_the_peer_is_verified(topology, recorded, caplog):
    with caplog.at_level("WARNING", logger="kynomesh.client.client"):
        await _new_for_peer("calc", card_headers=AUTH)

    assert "without TLS verification" not in caplog.text


async def test_headers_reach_the_wire_on_the_real_card_fetch(topology, monkeypatch):
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(200, json={"name": "calc", "description": "d", "version": "1"})

    monkeypatch.setattr(
        kynomesh_client,
        "default_httpx_client",
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    card = await resolve_agent_card("calc", headers=AUTH)
    await resolve_agent_card("calc")

    assert card.name == "calc"
    assert seen == ["Bearer secret", None]


def test_peer_http_clients_never_follow_redirects():
    # A followed redirect would re-send the headers to whatever host it names.
    assert kynomesh_client.default_httpx_client.follow_redirects is False
    assert kynomesh_client.managed_httpx_client.follow_redirects is False


async def test_card_fetch_refuses_redirects_even_if_the_http_client_follows_them(
    topology, monkeypatch
):
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.host, request.headers.get("authorization")))
        if request.url.host == "calc.example":
            return httpx.Response(302, headers={"location": "https://elsewhere.example/card"})
        return httpx.Response(200, json={"name": "evil"})

    monkeypatch.setattr(
        kynomesh_client,
        "default_httpx_client",
        httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True),
    )

    with pytest.raises(AgentCardResolutionError):
        await resolve_agent_card("calc", headers=AUTH)

    assert [host for host, _ in seen] == ["calc.example"]
