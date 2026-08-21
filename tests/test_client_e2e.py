"""End-to-end test of the peer-discovery flow against a real kynomesh server."""

import asyncio
import json
import uuid

import pytest
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
from kynomesh.client import _topology


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
    yield
    _topology._reset_topology_cache()
    _topology._topology_path = _topology.DEFAULT_TOPOLOGY_PATH


async def test_new_for_peer_end_to_end(tmp_path, unused_tcp_port_factory):
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

        a2a_client = await kynomesh_client.new_for_peer("echo")
        request = SendMessageRequest(
            message=Message(
                message_id=str(uuid.uuid4()),
                role=Role.ROLE_USER,
                parts=[Part(text="ping")],
            )
        )
        responses = [r async for r in a2a_client.send_message(request)]
        assert responses[0].message.parts[0].text == "pong"
        await a2a_client.close()
    finally:
        serve_task.cancel()
        try:
            await serve_task
        except asyncio.CancelledError:
            pass
