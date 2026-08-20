import grpc
from grpc_health.v1.health_pb2 import HealthCheckResponse

from kynomesh.server.health import Health


def test_new_health_is_serving():
    health = Health()
    assert health.is_serving()


def test_set_serving_false():
    health = Health()
    health.set_serving(False)
    assert not health.is_serving()


async def test_http_endpoint_serving_returns_200():
    health = Health()
    response = await health.http_endpoint(None)
    assert response.status_code == 200
    assert response.body == b"SERVING\n"


async def test_http_endpoint_not_serving_returns_503():
    health = Health()
    health.set_serving(False)
    response = await health.http_endpoint(None)
    assert response.status_code == 503
    assert response.body == b"NOT_SERVING\n"


def _grpc_status(servicer) -> int:
    return servicer._server_status[""]


async def test_attach_grpc_registers_current_status():
    health = Health()
    health.set_serving(False)
    grpc_server = grpc.aio.server()

    health.attach_grpc(grpc_server)

    assert health._grpc_servicer is not None
    assert _grpc_status(health._grpc_servicer) == HealthCheckResponse.NOT_SERVING


async def test_set_serving_after_attach_updates_grpc_status():
    health = Health()
    grpc_server = grpc.aio.server()
    health.attach_grpc(grpc_server)

    health.set_serving(False)

    assert _grpc_status(health._grpc_servicer) == HealthCheckResponse.NOT_SERVING
