import asyncio
import ssl
import uuid

import grpc
import pytest
import trustme
from a2a.types.a2a_pb2 import Message, Part, Role, SendMessageResponse
from a2a.types.a2a_pb2_grpc import (
    A2AServiceServicer,
    A2AServiceStub,
    add_A2AServiceServicer_to_server,
)

from kynomesh.client._managed_tls import _fetch_peer_cert_pem, managed_grpc_channel


class _EchoServicer(A2AServiceServicer):
    async def SendMessage(self, request, context):
        return SendMessageResponse(
            message=Message(
                message_id=str(uuid.uuid4()),
                role=Role.ROLE_AGENT,
                parts=[Part(text="pong")],
            )
        )


@pytest.fixture
async def tls_grpc_server():
    ca = trustme.CA()
    cert = ca.issue_cert("127.0.0.1")
    server_key_pem = cert.private_key_pem.bytes()
    server_cert_pem = cert.cert_chain_pems[0].bytes()

    server = grpc.aio.server()
    add_A2AServiceServicer_to_server(_EchoServicer(), server)
    creds = grpc.ssl_server_credentials([(server_key_pem, server_cert_pem)])
    port = server.add_secure_port("127.0.0.1:0", creds)
    await server.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        await server.stop(None)


async def test_fetch_peer_cert_pem_returns_pem(tls_grpc_server):
    target = tls_grpc_server
    host, port = target.split(":")

    pem = _fetch_peer_cert_pem(host, int(port))

    assert pem.startswith(b"-----BEGIN CERTIFICATE-----")


async def test_managed_grpc_channel_completes_rpc(tls_grpc_server):
    channel = managed_grpc_channel(tls_grpc_server)
    try:
        stub = A2AServiceStub(channel)
        from a2a.types.a2a_pb2 import SendMessageRequest

        request = SendMessageRequest(
            message=Message(
                message_id=str(uuid.uuid4()),
                role=Role.ROLE_USER,
                parts=[Part(text="ping")],
            )
        )
        response = await stub.SendMessage(request)
        assert response.message.parts[0].text == "pong"
    finally:
        await channel.close()


async def test_managed_grpc_channel_rejects_wrong_cert(tls_grpc_server):
    """Sanity check: the pinning still enforces the cert it fetched, not "anything"."""
    host, port = tls_grpc_server.split(":")
    other_ca = trustme.CA()
    other_cert = other_ca.issue_cert("127.0.0.1")
    bogus_pem = other_cert.cert_chain_pems[0].bytes()

    credentials = grpc.ssl_channel_credentials(root_certificates=bogus_pem)
    options = [("grpc.ssl_target_name_override", host)]
    channel = grpc.aio.secure_channel(f"{host}:{port}", credentials, options=options)
    try:
        stub = A2AServiceStub(channel)
        from a2a.types.a2a_pb2 import SendMessageRequest

        request = SendMessageRequest(
            message=Message(message_id="x", role=Role.ROLE_USER, parts=[Part(text="ping")])
        )
        with pytest.raises(grpc.aio.AioRpcError):
            await stub.SendMessage(request, timeout=5)
    finally:
        await channel.close()
