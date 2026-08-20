"""In-agent A2A server helper for Kynomesh.

start() binds two independent listeners, one for HTTP (AgentCard,
JSON-RPC, REST, /healthz) and one for gRPC (A2A gRPC transport,
grpc.health.v1); locally each defaults to its own TCP port
(127.0.0.1:8088 for HTTP, 127.0.0.1:8089 for gRPC). start() mounts the
transports listed in card.supported_interfaces.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass

import grpc
import grpc.aio
import uvicorn
from a2a.server.agent_execution.agent_executor import AgentExecutor
from a2a.server.request_handlers.default_request_handler_v2 import (
    DefaultRequestHandlerV2,
)
from a2a.server.request_handlers.grpc_handler import GrpcHandler
from a2a.server.routes import (
    add_a2a_routes_to_fastapi,
    create_agent_card_routes,
    create_jsonrpc_routes,
    create_rest_routes,
)
from a2a.server.tasks.inmemory_task_store import InMemoryTaskStore
from a2a.types.a2a_pb2 import AgentCard
from a2a.types.a2a_pb2_grpc import add_A2AServiceServicer_to_server
from a2a.utils.constants import TransportProtocol
from fastapi import FastAPI
from starlette.routing import Route

from kynomesh.server import _listener
from kynomesh.server.health import HEALTH_PATH, Health

_logger = logging.getLogger(__name__)

_JSONRPC_PATH = "/a2a/jsonrpc"
_REST_PATH_PREFIX = "/a2a/rest"

_DEFAULT_SHUTDOWN_TIMEOUT = 10.0


@dataclass
class _Options:
    http_address: str | None = None
    grpc_address: str | None = None
    shutdown_timeout: float = _DEFAULT_SHUTDOWN_TIMEOUT
    health: Health | None = None
    task_store: object | None = None


class Option:
    """A configuration knob applied by start(). Build with the with_* helpers."""

    def __init__(self, apply: "callable[[_Options], None]") -> None:
        self._apply = apply

    def apply(self, options: _Options) -> None:
        self._apply(options)


def with_http_address(address: str) -> Option:
    """Overrides the HTTP listener address.

    An absolute path opens a Unix domain socket; anything else is treated
    as a TCP host:port.
    """
    return Option(lambda o: setattr(o, "http_address", address))


def with_grpc_address(address: str) -> Option:
    """Overrides the gRPC listener address.

    An absolute path opens a Unix domain socket; anything else is treated
    as a TCP host:port.
    """
    return Option(lambda o: setattr(o, "grpc_address", address))


def with_shutdown_timeout(seconds: float) -> Option:
    return Option(lambda o: setattr(o, "shutdown_timeout", seconds))


def with_health(health: Health) -> Option:
    """Installs a caller-owned Health handle so the agent can flip readiness.

    If omitted, start() uses an internal Health that stays SERVING for the
    lifetime of the process. The gRPC health service and HTTP /healthz are
    always mounted.
    """
    return Option(lambda o: setattr(o, "health", health))


def with_task_store(task_store: object) -> Option:
    """Overrides the task store used by the request handler.

    Defaults to an in-memory store, which does not survive process restarts.
    """
    return Option(lambda o: setattr(o, "task_store", task_store))


def _build_http_app(
    handler: DefaultRequestHandlerV2, card: AgentCard, health: Health
) -> tuple[FastAPI, list[str]]:
    app = FastAPI()
    app.add_route(HEALTH_PATH, health.http_endpoint, methods=["GET"])

    mounted: list[str] = []
    jsonrpc_routes: list[Route] = []
    rest_routes: list[Route] = []
    for iface in card.supported_interfaces:
        if iface.protocol_binding == TransportProtocol.JSONRPC.value:
            jsonrpc_routes = create_jsonrpc_routes(handler, rpc_url=_JSONRPC_PATH)
            mounted.append("jsonrpc")
        elif iface.protocol_binding == TransportProtocol.HTTP_JSON.value:
            rest_routes = create_rest_routes(handler, path_prefix=_REST_PATH_PREFIX)
            mounted.append("rest")

    add_a2a_routes_to_fastapi(
        app,
        agent_card_routes=create_agent_card_routes(card),
        jsonrpc_routes=jsonrpc_routes,
        rest_routes=rest_routes,
    )
    return app, mounted


def _build_grpc_server(
    handler: DefaultRequestHandlerV2, card: AgentCard, health: Health
) -> tuple[grpc.aio.Server, list[str]]:
    """Builds the gRPC server.

    The server is always created so the standard grpc.health.v1 service
    can answer kynoprobe regardless of which A2A transports the card
    advertises; the A2A gRPC handler is mounted only when the card lists
    gRPC.
    """
    grpc_server = grpc.aio.server()
    health.attach_grpc(grpc_server)

    mounted: list[str] = []
    for iface in card.supported_interfaces:
        if iface.protocol_binding == TransportProtocol.GRPC.value:
            add_A2AServiceServicer_to_server(GrpcHandler(handler), grpc_server)
            mounted.append("grpc")
            break
    return grpc_server, mounted


def _uvicorn_config(app: FastAPI, cfg: "_listener.ListenerConfig") -> uvicorn.Config:
    uv_config = uvicorn.Config(app, log_level="info")
    if cfg.is_uds:
        uv_config.uds = cfg.address
    else:
        host, _, port = cfg.address.rpartition(":")
        uv_config.host = host or "127.0.0.1"
        uv_config.port = int(port)
    return uv_config


def _bind_grpc(grpc_server: grpc.aio.Server, cfg: "_listener.ListenerConfig") -> None:
    address = f"unix://{cfg.address}" if cfg.is_uds else cfg.address
    grpc_server.add_insecure_port(address)
    if cfg.is_uds:
        os.chmod(cfg.address, 0o660)


async def start(
    executor: AgentExecutor, card: AgentCard, *options: Option
) -> None:
    """Starts an A2A server for executor, serving until cancelled.

    Resolves the HTTP and gRPC listeners (Unix domain sockets in-pod, 
    local TCP ports otherwise), mounts the transports listed in 
    card.supported_interfaces, and advertises the agent to the broker 
    when running in-pod. Raises if executor or card is missing, or on 
    listener or server startup failure. Returns when the enclosing 
    task is cancelled, after a graceful shutdown of both listeners.
    """
    if executor is None:
        raise ValueError("kynomesh server: executor is required")
    if card is None:
        raise ValueError("kynomesh server: agent card is required")

    opts = _Options()
    for option in options:
        option.apply(opts)
    health = opts.health or Health()

    http_cfg, grpc_cfg = _listener.resolve_listeners(
        opts.http_address, opts.grpc_address
    )

    task_store = opts.task_store or InMemoryTaskStore()
    handler = DefaultRequestHandlerV2(
        agent_executor=executor, task_store=task_store, agent_card=card
    )
    http_app, http_mounted = _build_http_app(handler, card, health)
    grpc_server, grpc_mounted = _build_grpc_server(handler, card, health)
    mounted = http_mounted + grpc_mounted

    if http_cfg.is_uds:
        _listener.prepare_uds_path(http_cfg.address)
    if grpc_cfg.is_uds:
        _listener.prepare_uds_path(grpc_cfg.address)

    # Advertise to the broker only when colocated in the same pod; in
    # local dev there is no broker reading this file.
    if http_cfg.is_uds:
        _listener.write_server_info(http_cfg)

    uv_config = _uvicorn_config(http_app, http_cfg)
    http_server = uvicorn.Server(uv_config)

    _bind_grpc(grpc_server, grpc_cfg)

    _logger.info(
        "Kynomesh server starting",
        extra={
            "agent": card.name,
            "version": card.version,
            "httpNetwork": http_cfg.network,
            "httpAddress": http_cfg.address,
            "grpcNetwork": grpc_cfg.network,
            "grpcAddress": grpc_cfg.address,
            "transports": mounted,
            "health": ["grpc", "http " + HEALTH_PATH],
        },
    )

    http_task = asyncio.ensure_future(http_server.serve())
    grpc_task = asyncio.ensure_future(_serve_grpc(grpc_server))

    if http_cfg.is_uds:
        # uvicorn binds the UDS during startup; chmod once it's listening.
        while not http_server.started and not http_task.done():
            await asyncio.sleep(0.01)
        if os.path.exists(http_cfg.address):
            os.chmod(http_cfg.address, 0o660)

    try:
        await asyncio.shield(asyncio.gather(http_task, grpc_task))
    except asyncio.CancelledError:
        # Flip readiness first so kynoprobe pulls this replica out of
        # rotation before the listeners close.
        _logger.info("Kynomesh server shutting down")
        health.set_serving(False)
        await _shutdown(http_server, http_task, grpc_server, grpc_task, opts.shutdown_timeout)
        _logger.info("Kynomesh server stopped")
        raise


async def _serve_grpc(grpc_server: grpc.aio.Server) -> None:
    await grpc_server.start()
    await grpc_server.wait_for_termination()


async def _shutdown(
    http_server: uvicorn.Server,
    http_task: "asyncio.Task[None]",
    grpc_server: grpc.aio.Server,
    grpc_task: "asyncio.Task[None]",
    shutdown_timeout: float,
) -> None:
    http_server.should_exit = True
    await grpc_server.stop(shutdown_timeout)
    try:
        await asyncio.wait_for(
            asyncio.gather(http_task, grpc_task), timeout=shutdown_timeout
        )
    except asyncio.TimeoutError:
        http_server.force_exit = True
        await grpc_server.stop(0)
        await asyncio.gather(http_task, grpc_task)
