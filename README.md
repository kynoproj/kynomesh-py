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
pip install kynomesh
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

### Exposing multiple transports

`supported_interfaces` is a list — list every transport you want callers to be
able to use. Most agents only need one, but you can advertise more than one and
let each caller pick:

| Transport        | `TransportProtocol` constant  | Wire string |
| ---------------- | ----------------------------- | ----------- |
| JSON-RPC         | `TransportProtocol.JSONRPC`   | `JSONRPC`   |
| REST (HTTP+JSON) | `TransportProtocol.HTTP_JSON` | `HTTP+JSON` |
| gRPC             | `TransportProtocol.GRPC`      | `GRPC`      |

```python
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
                url="http://127.0.0.1:8088/a2a/rest",
                protocol_binding=TransportProtocol.HTTP_JSON.value,
            ),
            AgentInterface(
                url="127.0.0.1:8089",
                protocol_binding=TransportProtocol.GRPC.value,
            ),
        ],
        default_input_modes=["text"],
        default_output_modes=["text"],
    )
```

`server.start` reads each interface's `protocol_binding` to decide which
transports to mount — `HTTP_JSON` mounts the REST path shown above, separate
from the JSON-RPC path, both on the same HTTP listener. The `url` on each
interface is only used locally to pick the transport's path or address; it isn't
used to bind the listener itself. Running in-cluster, Kynomesh's broker sidecar
fronts your agent and rewrites these URLs to the externally reachable address
before advertising it to peers, so you don't need to construct real addresses
yourself.

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
flow into one call, and caches the result: a peer's client is built at most once
per process and reused on every later call for that peer name, including under
concurrent first use.

```python
import asyncio
import uuid

from a2a.types.a2a_pb2 import Message, Part, Role, SendMessageRequest

from kynomesh import client


async def main() -> None:
    # Discover the peer, fetch its AgentCard, and build an a2a
    # client — once per process per peer name; later calls for the
    # same peer reuse the cached client. For Managed peer agents
    # reached over TLS, the build skips certificate verification for the
    # hop. See "External peers and TLS" below.
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
resolved until the first call for that peer name. Because the client is shared
and reused, callers must not call `close()` on it. To drop a peer's cached
client and force a rebuild on the next call (e.g. after the peer's AgentCard
changes):

```python
client.forget_peer("worker-a")
```

Concurrency scope is single-event-loop asyncio: don't share a cached peer client
across OS threads or multiple event loops.

### External peers and TLS

Peers in the same `AgentSet` (Managed) are reached over the broker's self-signed
TLS certificate, so the SDK skips certificate verification for them (the hop is
encrypted, the peer is not authenticated). External peers, declared in
`externalAgents` (see the
[core concepts](https://github.com/kynoproj/kynomesh/blob/main/docs/core-concepts/overview.md)),
are expected to present a real, CA-verifiable certificate, so the SDK verifies
them normally.

The exception is an External peer that is itself an agent in **another
AgentSet**. Its broker presents a self-signed certificate too, so verification
fails with `CERTIFICATE_VERIFY_FAILED`. Name such peers in
`KYNOMESH_INSECURE_TLS_PEERS` to give them the same treatment as Managed peers:

```yaml
env:
  - name: KYNOMESH_INSECURE_TLS_PEERS
    value: calculator # comma-separated peer names
```

Nothing changes in your code: `peer_client("calculator")` works the same. The
setting only applies to the named External peers and logs a warning when it
takes effect. It is read each time a peer's client is built, and clients are
cached, so after changing it call `client.forget_peer("calculator")` to rebuild.
Leave it unset for a real external agent with a CA-signed certificate.

### Peers that protect their AgentCard

Some peers require a header, such as an `Authorization` header or an API key,
just to read their AgentCard. Pass `card_headers` to `peer_client`. The headers
are sent on the AgentCard request only:

```python
import os

a2a_client = await client.peer_client(
    "fraud-check",
    card_headers={"Authorization": f"Bearer {os.environ['FRAUD_CHECK_TOKEN']}"},
)
```

They are **not** attached to calls made through the client. Authenticate each
call yourself:

```python
from a2a.client.client import ClientCallContext

context = ClientCallContext(service_parameters={"Authorization": f"Bearer {fresh_token}"})
async for response in a2a_client.send_message(request, context=context):
    ...
```

- Headers are applied when the client is first built. Clients are cached, so
  `card_headers` passed on later calls are ignored. After a token changes, call
  `client.forget_peer("fraud-check")` and then `peer_client(...)` again.
- **Redirects are not followed** on the card request, so the headers are never
  re-sent to another host.
- Malformed headers raise `ValueError` before any request is made: a name with
  anything other than letters, digits, `-`, `_` and `.` (or starting with
  `grpc-`), a value that is not printable ASCII (including line breaks), or the
  same name given twice in different cases. The error never includes the value.
- To fetch a card on its own, use
  `client.resolve_agent_card(name, headers=...)`.

### Lower-level helpers

Use these when you don't want the full client:

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

- [Kynomesh](https://github.com/kynoproj/kynomesh)
- [Core concepts](https://github.com/kynoproj/kynomesh/blob/main/docs/core-concepts/overview.md)
- [A2A protocol](https://a2a-protocol.org/)
- [a2aproject/a2a-python](https://github.com/a2aproject/a2a-python)

## License

Apache 2.0 — see [LICENSE](LICENSE).
