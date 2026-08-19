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
