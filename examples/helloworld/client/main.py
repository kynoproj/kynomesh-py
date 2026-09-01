"""Kynomesh port of the upstream a2a-python helloworld client example.

Assumes this process runs inside a Kynomesh AgentDeploy pod, where the
broker init container has written /var/run/kynomesh/topology.json. Peer
discovery is done via kynomesh.client; no URLs are hard-coded.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import uuid

from a2a.types.a2a_pb2 import Message, Part, Role, SendMessageRequest

from kynomesh import client

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


async def main(peer_name: str) -> None:
    # Look up the peer's URL, fetch its AgentCard, and build an a2a
    # client over one of its advertised transports — once per process
    # per peer name.
    a2a_client = await client.peer_client(peer_name)
    request = SendMessageRequest(
        message=Message(
            message_id=str(uuid.uuid4()),
            role=Role.ROLE_USER,
            parts=[Part(text="Hello, world")],
        )
    )
    async for response in a2a_client.send_message(request):
        _logger.info("Server responded with: %s", response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--peer",
        default="hello-world",
        help="Name of the peer to call. Must appear in this agent's topology.",
    )
    args = parser.parse_args()
    asyncio.run(main(args.peer))
