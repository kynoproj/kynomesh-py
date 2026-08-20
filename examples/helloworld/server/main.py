"""Kynomesh port of the upstream a2a-python helloworld server example."""

from __future__ import annotations

import asyncio
import logging
import signal
import uuid

from a2a.server.agent_execution.agent_executor import AgentExecutor
from a2a.server.agent_execution.context import RequestContext
from a2a.server.events.event_queue_v2 import EventQueue
from a2a.types.a2a_pb2 import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
    Message,
    Part,
    Role,
)
from a2a.utils.constants import TransportProtocol

from kynomesh import server

logging.basicConfig(level=logging.INFO)


class HelloWorldAgentExecutor(AgentExecutor):
    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        del context
        message = Message(
            message_id=str(uuid.uuid4()),
            role=Role.ROLE_AGENT,
            parts=[Part(text="Hello, world!")],
        )
        await event_queue.enqueue_event(message)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        del context, event_queue


def hello_world_card() -> AgentCard:
    # The URLs below are ignored when running in a K8s cluster; supplying
    # the local-dev HTTP (8088) and gRPC (8089) ports is helpful when
    # doing local dev testing.
    return AgentCard(
        name="Hello World Agent",
        description="Just a hello world agent",
        version="0.0.1",
        supported_interfaces=[
            AgentInterface(
                url="http://127.0.0.1:8088",
                protocol_binding=TransportProtocol.JSONRPC.value,
            ),
            AgentInterface(
                url="http://127.0.0.1:8088",
                protocol_binding=TransportProtocol.HTTP_JSON.value,
            ),
            AgentInterface(
                url="127.0.0.1:8089",
                protocol_binding=TransportProtocol.GRPC.value,
            ),
        ],
        default_input_modes=["text"],
        default_output_modes=["text"],
        capabilities=AgentCapabilities(streaming=True),
        skills=[
            AgentSkill(
                id="hello_world",
                name="Hello, world!",
                description="Returns a 'Hello, world!'",
                tags=["hello world"],
                examples=["hi", "hello"],
            )
        ],
    )


async def main() -> None:
    loop = asyncio.get_running_loop()
    serve_task = asyncio.ensure_future(
        server.start(HelloWorldAgentExecutor(), hello_world_card())
    )

    def _cancel() -> None:
        serve_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _cancel)

    try:
        await serve_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
