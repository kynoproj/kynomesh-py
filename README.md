# Kynomesh Python SDK

Python SDK for [Kynomesh](https://github.com/kynoproj/kynomesh) — a
Kubernetes-native platform for orchestrating distributed multi-agent systems
based on the [A2A protocol](https://a2a-protocol.org/).

## What this SDK does

In Kynomesh, your agent code runs alongside an injected broker that handles peer
discovery, transport, and external A2A traffic. This SDK gives you a thin layer
over the upstream
[a2aproject/a2a-python](https://github.com/a2aproject/a2a-python) SDK (`a2a-sdk`
on PyPI) that handles the Kynomesh-specific wiring:

- **`kynomesh.server`** — start an A2A agent that the broker can reach. Picks
  the right listeners automatically and advertises the agent to the broker.
- **`kynomesh.client`** — call other agents in the same `AgentSet` by name. No
  URLs, no transports, no AgentCard plumbing in your code.

Everything else — `AgentCard`, `Message`, executors, request handlers — comes
straight from `a2a-sdk`. This SDK does not wrap or replace those types.

## Install

```bash
pip install pykynomesh
```

## Server: write an agent

Implement the upstream `a2a.server.agent_execution.AgentExecutor` interface,
build an `AgentCard`, and call `kynomesh.server.start`. The
`supported_interfaces` URLs on the card are placeholders for local dev —
Kynomesh rewrites them to the externally reachable endpoint at serve time.

```python
import asyncio
import signal
import uuid

from a2a.server.agent_execution.agent_executor import AgentExecutor
from a2a.types.a2a_pb2 import AgentCard, AgentInterface, Message, Part, Role
from a2a.utils.constants import TransportProtocol

from kynomesh import server


class HelloWorldAgentExecutor(AgentExecutor):
    async def execute(self, context, event_queue) -> None:
        await event_queue.enqueue_event(
            Message(
                message_id=str(uuid.uuid4()),
                role=Role.ROLE_AGENT,
                parts=[Part(text="Hello, world!")],
            )
        )

    async def cancel(self, context, event_queue) -> None:
        pass


def hello_world_card() -> AgentCard:
    return AgentCard(
        name="Hello World Agent",
        version="0.0.1",
        supported_interfaces=[
            AgentInterface(
                url="http://127.0.0.1:8088/a2a/jsonrpc",
                protocol_binding=TransportProtocol.JSONRPC.value,
            ),
            AgentInterface(
                url="127.0.0.1:8089",
                protocol_binding=TransportProtocol.GRPC.value,
            ),
        ],
        default_input_modes=["text"],
        default_output_modes=["text"],
    )


async def main() -> None:
    loop = asyncio.get_running_loop()
    serve_task = asyncio.ensure_future(server.start(HelloWorldAgentExecutor(), hello_world_card()))
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, serve_task.cancel)
    try:
        await serve_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
```

What `server.start` does for you:

- Picks the right listeners for the runtime — in-cluster this is a pair of UDS
  sockets under `/var/run/kynomesh`; in local dev it's `127.0.0.1:8088` for HTTP
  (JSON-RPC, REST, AgentCard, `/healthz`) and `127.0.0.1:8089` for gRPC.
- Mounts JSON-RPC, REST, and gRPC transports based on
  `card.supported_interfaces`.
- Advertises the agent so peers can discover it.

Full example: [examples/helloworld/server](examples/helloworld/server).

## Health checks

`server.start` always mounts two health endpoints, one per listener, so
Kynomesh's `kynoprobe` can drive readiness and liveness probes regardless of
which A2A transports the card advertises:

- **gRPC** — the standard `grpc.health.v1.Health/Check` service (matches
  `kynoprobe --mode=grpc`, the default the controller uses).
- **HTTP** — `GET /healthz` returns `200 SERVING` or `503 NOT_SERVING` (matches
  `kynoprobe --mode=http --path=/healthz`).

By default the agent reports `SERVING` for its lifetime and flips to
`NOT_SERVING` automatically when `start` begins shutting down — that's enough
for most agents and needs no extra code.

### Writing a customized health check

Out of the box, the agent always reports SERVING — which is misleading once your
agent depends on something it can't guarantee, like an LLM endpoint, a database
connection, or a model file loaded at startup. In those cases, "ready" is a
property of those dependencies, not of the process itself.

Pass a caller-owned `kynomesh.server.Health` via `with_health`, run your own
checks against the things the agent needs, and call `set_serving(True|False)` to
publish the result. `kynoprobe` picks up the change on its next poll.

```python
import asyncio

from kynomesh import server


async def watch_health(health: server.Health, check) -> None:
    while True:
        try:
            await check()
            health.set_serving(True)
        except Exception:
            health.set_serving(False)
        await asyncio.sleep(5)


async def main() -> None:
    health = server.Health()
    asyncio.ensure_future(watch_health(health, check_llm))

    serve_task = asyncio.ensure_future(
        server.start(
            HelloWorldAgentExecutor(),
            hello_world_card(),
            server.with_health(health),
        )
    )
    ...
```

`Health` is safe to share across asyncio tasks, and the same state is observed
by both the gRPC and HTTP surfaces — `kynoprobe` sees the flip on its next tick
regardless of which mode it runs in.

Pick the check to match what "ready" actually means for your agent:

- LLM/API-backed agent → ping the provider.
- Agent that needs a model file on disk → check the loaded flag.
- Agent with a bounded work queue → flip on depth thresholds.

Keep the check cheap and bounded — it runs on every poll, and a slow check just
delays the next status update.

## Client: call a peer agent

Within an `AgentSet`, every agent has a set of peers it is allowed to call,
derived from the AgentSet's routing pattern. `kynomesh.client.peer_client`
collapses the whole peer-lookup + AgentCard-resolution + client-construction
flow into one call, and caches the result: a peer's client is built at most
once per process and reused on every later call for that peer name,
including under concurrent first use.

```python
import asyncio
import uuid

from a2a.types.a2a_pb2 import Message, Part, Role, SendMessageRequest

from kynomesh import client


async def main() -> None:
    # Discover the peer, fetch its AgentCard, and build an a2a
    # client — once per process per peer name; later calls for the
    # same peer reuse the cached client. For Managed peer agents
    # reached over gRPC, the first build pins the broker's certificate
    # for the hop (TLS-encrypted but unauthenticated, like the Go
    # SDK's InsecureSkipVerify).
    a2a_client = await client.peer_client("worker-a")
    request = SendMessageRequest(
        message=Message(
            message_id=str(uuid.uuid4()),
            role=Role.ROLE_USER,
            parts=[Part(text="Hello, world")],
        )
    )
    async for response in a2a_client.send_message(request):
        print(response)


asyncio.run(main())
```

`peer_client` is lazy — a peer never gets a client built or its AgentCard
resolved until the first call for that peer name. Because the client is
shared and reused, callers must not call `close()` on it. To drop a peer's
cached client and force a rebuild on the next call (e.g. after the peer's
AgentCard changes):

```python
client.forget_peer("worker-a")
```

Concurrency scope is single-event-loop asyncio: don't share a cached peer
client across OS threads or multiple event loops.

Need a client that isn't cached (e.g. to always resolve the current
AgentCard)? `client.new_for_peer("worker-a")` runs the same flow but always
builds a fresh client; the caller owns it and is responsible for calling
`await a2a_client.close()`.

Lower-level helpers when you don't want the full client:

```python
url = client.peer_url("worker-a")  # just the URL
card = await client.resolve_agent_card("worker-a")  # just the AgentCard
names = client.peers()  # every reachable peer
```

Error types for `except`/`isinstance` checks:

- `client.PeerNotFoundError` — the peer is not reachable from this agent.
- `client.TopologyNotAvailableError` — peer discovery is not available (e.g.
  running outside a Kynomesh deployment).

Peer information is loaded once per process and cached for its lifetime.

Full example: [examples/helloworld/client](examples/helloworld/client).

## Resources

- [Kynomesh project](https://github.com/kynoproj/kynomesh)
- [Core concepts](https://github.com/kynoproj/kynomesh/blob/main/docs/core-concepts/overview.md)
- [A2A protocol](https://a2a-protocol.org/)
- [a2aproject/a2a-python](https://github.com/a2aproject/a2a-python)
- [kynoproj/kynomesh-go](https://github.com/kynoproj/kynomesh-go) — the Go SDK
  this project mirrors

## License

Apache 2.0 — see [LICENSE](LICENSE).
