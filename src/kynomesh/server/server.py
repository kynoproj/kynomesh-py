"""In-agent A2A server helper for Kynomesh.

start() binds a UDS at /var/run/kynomesh/broker.sock in-pod (POD_NAME set)
or 127.0.0.1:8088 locally, and mounts the JSON-RPC and REST transports
listed in card.supported_interfaces.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass

import uvicorn
from a2a.server.agent_execution.agent_executor import AgentExecutor
from a2a.server.request_handlers.default_request_handler_v2 import (
    DefaultRequestHandlerV2,
)
from a2a.server.routes import (
    add_a2a_routes_to_fastapi,
    create_agent_card_routes,
    create_jsonrpc_routes,
    create_rest_routes,
)
from a2a.server.tasks.inmemory_task_store import InMemoryTaskStore
from a2a.types.a2a_pb2 import AgentCard
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
    address: str | None = None
    shutdown_timeout: float = _DEFAULT_SHUTDOWN_TIMEOUT
    health: Health | None = None
    task_store: object | None = None


class Option:
    """A configuration knob applied by start(). Build with the with_* helpers."""

    def __init__(self, apply: "callable[[_Options], None]") -> None:
        self._apply = apply

    def apply(self, options: _Options) -> None:
        self._apply(options)


def with_address(address: str) -> Option:
    """Overrides the listener address.

    An absolute path opens a Unix domain socket; anything else is treated
    as a TCP host:port.
    """
    return Option(lambda o: setattr(o, "address", address))


def with_shutdown_timeout(seconds: float) -> Option:
    return Option(lambda o: setattr(o, "shutdown_timeout", seconds))


def with_health(health: Health) -> Option:
    """Installs a caller-owned Health handle so the agent can flip readiness.

    If omitted, start() uses an internal Health that stays SERVING for the
    lifetime of the process. HTTP /healthz is always mounted.
    """
    return Option(lambda o: setattr(o, "health", health))


def with_task_store(task_store: object) -> Option:
    """Overrides the task store used by the request handler.

    Defaults to an in-memory store, which does not survive process restarts.
    """
    return Option(lambda o: setattr(o, "task_store", task_store))


def _build_app(
    executor: AgentExecutor, card: AgentCard, health: Health, options: _Options
) -> tuple[FastAPI, list[str]]:
    task_store = options.task_store or InMemoryTaskStore()
    handler = DefaultRequestHandlerV2(
        agent_executor=executor, task_store=task_store, agent_card=card
    )

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
        elif iface.protocol_binding == TransportProtocol.GRPC.value:
            _logger.warning(
                "kynomesh server: gRPC transport requested but not yet "
                "supported by kynomesh-py; skipping"
            )

    add_a2a_routes_to_fastapi(
        app,
        agent_card_routes=create_agent_card_routes(card),
        jsonrpc_routes=jsonrpc_routes,
        rest_routes=rest_routes,
    )
    return app, mounted


async def start(
    executor: AgentExecutor, card: AgentCard, *options: Option
) -> None:
    """Starts an A2A server for executor, serving until cancelled.

    Mirrors kynomesh-go's server.Start: resolves a listener (Unix domain
    socket in-pod, local TCP otherwise), mounts the transports listed in
    card.supported_interfaces, and advertises the agent to the broker when
    running in-pod. Raises if executor or card is missing, or on listener
    or server startup failure. Returns when the enclosing task is
    cancelled, after a graceful shutdown.
    """
    if executor is None:
        raise ValueError("kynomesh server: executor is required")
    if card is None:
        raise ValueError("kynomesh server: agent card is required")

    opts = _Options()
    for option in options:
        option.apply(opts)
    health = opts.health or Health()

    cfg = _listener.resolve_listener(opts.address)
    app, mounted = _build_app(executor, card, health, opts)

    if cfg.is_uds:
        _listener.prepare_uds_path(cfg.address)
        _listener.write_server_info(cfg)

    uv_config = uvicorn.Config(app, log_level="info")
    if cfg.is_uds:
        uv_config.uds = cfg.address
    else:
        host, _, port = cfg.address.rpartition(":")
        uv_config.host = host or "127.0.0.1"
        uv_config.port = int(port)

    server = uvicorn.Server(uv_config)

    _logger.info(
        "Kynomesh server starting",
        extra={
            "agent": card.name,
            "version": card.version,
            "network": cfg.network,
            "address": cfg.address,
            "transports": mounted,
            "health": ["http " + HEALTH_PATH],
        },
    )

    serve_task = asyncio.ensure_future(server.serve())
    if cfg.is_uds:
        # uvicorn binds the UDS during startup; chmod once it's listening.
        while not server.started and not serve_task.done():
            await asyncio.sleep(0.01)
        if os.path.exists(cfg.address):
            os.chmod(cfg.address, 0o660)

    try:
        await asyncio.shield(serve_task)
    except asyncio.CancelledError:
        # Flip readiness first so kynoprobe pulls this replica out of
        # rotation before the listener closes.
        _logger.info("Kynomesh server shutting down")
        health.set_serving(False)
        server.should_exit = True
        try:
            await asyncio.wait_for(serve_task, timeout=opts.shutdown_timeout)
        except asyncio.TimeoutError:
            server.force_exit = True
            await serve_task
        _logger.info("Kynomesh server stopped")
        raise
