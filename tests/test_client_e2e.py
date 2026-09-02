"""End-to-end test of the peer-discovery flow against a real kynomesh server."""

import asyncio
import json
import os
import uuid

import pytest
from a2a.client.card_resolver import A2ACardResolver
from a2a.server.agent_execution.agent_executor import AgentExecutor
from a2a.types.a2a_pb2 import (
    AgentCard,
    AgentInterface,
    Message,
    Part,
    Role,
    SendMessageRequest,
)
from a2a.utils.constants import TransportProtocol

from kynomesh import client as kynomesh_client
from kynomesh import server as kynomesh_server
from kynomesh.client import _hash, _topology
from kynomesh.client.client import _reset_peer_client_cache


class _EchoExecutor(AgentExecutor):
    async def execute(self, context, event_queue) -> None:
        message = Message(
            message_id=str(uuid.uuid4()),
            role=Role.ROLE_AGENT,
            parts=[Part(text="pong")],
        )
        await event_queue.enqueue_event(message)

    async def cancel(self, context, event_queue) -> None:
        del context, event_queue


def _card(http_url: str) -> AgentCard:
    return AgentCard(
        name="echo",
        description="test echo agent",
        version="0.0.1",
        supported_interfaces=[
            AgentInterface(
                url=f"{http_url}/a2a/jsonrpc",
                protocol_binding=TransportProtocol.JSONRPC.value,
            ),
        ],
        default_input_modes=["text"],
        default_output_modes=["text"],
    )


@pytest.fixture(autouse=True)
def _reset_topology_cache():
    _topology._reset_topology_cache()
    _reset_peer_client_cache()
    yield
    _topology._reset_topology_cache()
    _topology._topology_path = _topology.DEFAULT_TOPOLOGY_PATH
    _reset_peer_client_cache()


async def test_peer_client_end_to_end(tmp_path, unused_tcp_port_factory):
    http_port = unused_tcp_port_factory()
    grpc_port = unused_tcp_port_factory()
    http_url = f"http://127.0.0.1:{http_port}"

    topo_path = tmp_path / "topology.json"
    topo_path.write_text(
        json.dumps(
            {
                "peers": [
                    {"name": "echo", "kind": "External", "url": http_url},
                ]
            }
        )
    )
    _topology._topology_path = str(topo_path)

    serve_task = asyncio.ensure_future(
        kynomesh_server.start(
            _EchoExecutor(),
            _card(http_url),
            kynomesh_server.with_http_address(f"127.0.0.1:{http_port}"),
            kynomesh_server.with_grpc_address(f"127.0.0.1:{grpc_port}"),
        )
    )
    try:
        for _ in range(100):
            try:
                card = await kynomesh_client.resolve_agent_card("echo")
                break
            except Exception:  # noqa: BLE001
                await asyncio.sleep(0.05)
        else:
            pytest.fail("server never became reachable")

        assert card.name == "echo"

        assert kynomesh_client.peer_url("echo") == http_url
        assert kynomesh_client.peers() == ["echo"]

        a2a_client = await kynomesh_client.peer_client("echo")
        request = SendMessageRequest(
            message=Message(
                message_id=str(uuid.uuid4()),
                role=Role.ROLE_USER,
                parts=[Part(text="ping")],
            )
        )
        responses = [r async for r in a2a_client.send_message(request)]
        assert responses[0].message.parts[0].text == "pong"
    finally:
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass


async def _run_echo_server(tmp_path, unused_tcp_port_factory, peer_name="echo"):
    """Starts a real kynomesh server and writes a topology pointing at it.

    Returns the asyncio task running the server; caller must cancel it.
    """
    http_port = unused_tcp_port_factory()
    grpc_port = unused_tcp_port_factory()
    http_url = f"http://127.0.0.1:{http_port}"

    topo_path = tmp_path / "topology.json"
    topo_path.write_text(
        json.dumps({"peers": [{"name": peer_name, "kind": "External", "url": http_url}]})
    )
    _topology._topology_path = str(topo_path)

    serve_task = asyncio.ensure_future(
        kynomesh_server.start(
            _EchoExecutor(),
            _card(http_url),
            kynomesh_server.with_http_address(f"127.0.0.1:{http_port}"),
            kynomesh_server.with_grpc_address(f"127.0.0.1:{grpc_port}"),
        )
    )

    for _ in range(100):
        try:
            await kynomesh_client.resolve_agent_card(peer_name)
            break
        except Exception:  # noqa: BLE001
            await asyncio.sleep(0.05)
    else:
        serve_task.cancel()
        pytest.fail("server never became reachable")

    return serve_task


async def _stop_server(serve_task) -> None:
    serve_task.cancel()
    try:
        await serve_task
    except asyncio.CancelledError:
        pass


async def test_peer_client_caches_across_calls(tmp_path, unused_tcp_port_factory):
    serve_task = await _run_echo_server(tmp_path, unused_tcp_port_factory)
    try:
        hits = 0
        original_get_card = A2ACardResolver.get_agent_card

        async def counting_get_card(self):
            nonlocal hits
            hits += 1
            return await original_get_card(self)

        A2ACardResolver.get_agent_card = counting_get_card
        try:
            c1 = await kynomesh_client.peer_client("echo")
            c2 = await kynomesh_client.peer_client("echo")
        finally:
            A2ACardResolver.get_agent_card = original_get_card

        assert c1 is c2
        assert hits == 1
    finally:
        await _stop_server(serve_task)


async def test_peer_client_lazy_does_not_build_uncalled_peer(tmp_path, unused_tcp_port_factory):
    serve_task = await _run_echo_server(tmp_path, unused_tcp_port_factory)
    try:
        hits = 0
        original_get_card = A2ACardResolver.get_agent_card

        async def counting_get_card(self):
            nonlocal hits
            hits += 1
            return await original_get_card(self)

        A2ACardResolver.get_agent_card = counting_get_card
        try:
            # No peer_client call for "echo" here: merely being present
            # in the topology must not trigger a card fetch or build.
            pass
        finally:
            A2ACardResolver.get_agent_card = original_get_card

        assert hits == 0
    finally:
        await _stop_server(serve_task)


async def test_peer_client_concurrent_first_use_builds_once(tmp_path, unused_tcp_port_factory):
    serve_task = await _run_echo_server(tmp_path, unused_tcp_port_factory)
    try:
        hits = 0
        original_get_card = A2ACardResolver.get_agent_card

        async def counting_get_card(self):
            nonlocal hits
            hits += 1
            return await original_get_card(self)

        A2ACardResolver.get_agent_card = counting_get_card
        try:
            clients = await asyncio.gather(
                *(kynomesh_client.peer_client("echo") for _ in range(20))
            )
        finally:
            A2ACardResolver.get_agent_card = original_get_card

        assert all(c is clients[0] for c in clients)
        assert hits == 1
    finally:
        await _stop_server(serve_task)


async def test_forget_peer_forces_rebuild(tmp_path, unused_tcp_port_factory):
    serve_task = await _run_echo_server(tmp_path, unused_tcp_port_factory)
    try:
        hits = 0
        original_get_card = A2ACardResolver.get_agent_card

        async def counting_get_card(self):
            nonlocal hits
            hits += 1
            return await original_get_card(self)

        A2ACardResolver.get_agent_card = counting_get_card
        try:
            await kynomesh_client.peer_client("echo")
            kynomesh_client.forget_peer("echo")
            await kynomesh_client.peer_client("echo")
        finally:
            A2ACardResolver.get_agent_card = original_get_card

        assert hits == 2
    finally:
        await _stop_server(serve_task)


async def test_peer_client_retries_after_failure(tmp_path, unused_tcp_port_factory):
    # No topology entry for "echo" at all: first call fails with
    # PeerNotFoundError. Write the topology afterward and confirm a
    # later call succeeds instead of replaying the cached error forever.
    topo_path = tmp_path / "topology.json"
    topo_path.write_text(json.dumps({"peers": []}))
    _topology._topology_path = str(topo_path)

    with pytest.raises(kynomesh_client.PeerNotFoundError):
        await kynomesh_client.peer_client("echo")

    _topology._reset_topology_cache()
    serve_task = await _run_echo_server(tmp_path, unused_tcp_port_factory)
    try:
        await kynomesh_client.peer_client("echo")
    finally:
        await _stop_server(serve_task)


@pytest.fixture(autouse=True)
def _reset_hash_state(tmp_path):
    prev_path = _hash._peer_hashes_path
    _hash._peer_hashes_path = str(tmp_path / "peer-hashes.json")
    _hash._reset_peer_hashes_state()
    yield
    _hash._reset_peer_hashes_state()
    _hash._peer_hashes_path = prev_path


async def test_peer_client_records_hash_on_first_build(
    tmp_path, unused_tcp_port_factory, monkeypatch
):
    monkeypatch.setenv("POD_NAME", "test-pod")
    serve_task = await _run_echo_server(tmp_path, unused_tcp_port_factory)
    try:
        await kynomesh_client.peer_client("echo")
        hashes = _hash._read_peer_hashes(_hash._peer_hashes_path)
        assert "echo" in hashes
    finally:
        await _stop_server(serve_task)


async def test_peer_client_does_not_record_hash_for_uncalled_peer(
    tmp_path, unused_tcp_port_factory, monkeypatch
):
    monkeypatch.setenv("POD_NAME", "test-pod")
    serve_task = await _run_echo_server(tmp_path, unused_tcp_port_factory)
    try:
        # No peer_client call here: merely being reachable must not
        # trigger a hash write.
        hashes = _hash._read_peer_hashes(_hash._peer_hashes_path)
        assert hashes == {}
    finally:
        await _stop_server(serve_task)


async def test_peer_client_does_not_record_hash_outside_pod(
    tmp_path, unused_tcp_port_factory, monkeypatch
):
    monkeypatch.delenv("POD_NAME", raising=False)
    serve_task = await _run_echo_server(tmp_path, unused_tcp_port_factory)
    try:
        await kynomesh_client.peer_client("echo")
        assert not os.path.exists(_hash._peer_hashes_path)
    finally:
        await _stop_server(serve_task)
