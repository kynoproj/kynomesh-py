import os

from kynomesh.server import _listener


def test_resolve_listener_explicit_tcp():
    cfg = _listener.resolve_listener("localhost:9090", "/var/run/x-uds", "127.0.0.1:1")
    assert cfg.network == "tcp"
    assert cfg.address == "localhost:9090"
    assert not cfg.is_uds


def test_resolve_listener_explicit_uds(tmp_path):
    addr = str(tmp_path / "broker.sock")
    cfg = _listener.resolve_listener(addr, "/var/run/x-uds", "127.0.0.1:1")
    assert cfg.network == "unix"
    assert cfg.address == addr
    assert cfg.is_uds


def test_resolve_listener_default_local(monkeypatch):
    monkeypatch.delenv("POD_NAME", raising=False)
    cfg = _listener.resolve_listener(None, "/var/run/x-uds", "127.0.0.1:1")
    assert cfg == _listener.ListenerConfig(network="tcp", address="127.0.0.1:1")


def test_resolve_listener_in_pod(monkeypatch):
    monkeypatch.setenv("POD_NAME", "agent-0")
    cfg = _listener.resolve_listener(None, "/var/run/x-uds", "127.0.0.1:1")
    assert cfg == _listener.ListenerConfig(network="unix", address="/var/run/x-uds")


def test_resolve_listeners_defaults_local(monkeypatch):
    monkeypatch.delenv("POD_NAME", raising=False)
    http_cfg, grpc_cfg = _listener.resolve_listeners(None, None)
    assert http_cfg == _listener.ListenerConfig(
        network="tcp", address=_listener.DEFAULT_LOCAL_HTTP_ADDR
    )
    assert grpc_cfg == _listener.ListenerConfig(
        network="tcp", address=_listener.DEFAULT_LOCAL_GRPC_ADDR
    )


def test_resolve_listeners_defaults_in_pod(monkeypatch):
    monkeypatch.setenv("POD_NAME", "agent-0")
    http_cfg, grpc_cfg = _listener.resolve_listeners(None, None)
    assert http_cfg == _listener.ListenerConfig(
        network="unix", address=_listener.BROKER_HTTP_SOCKET_PATH
    )
    assert grpc_cfg == _listener.ListenerConfig(
        network="unix", address=_listener.BROKER_GRPC_SOCKET_PATH
    )


def test_prepare_uds_path_creates_dir_and_removes_stale_socket(tmp_path):
    sock_dir = tmp_path / "nested"
    sock_path = str(sock_dir / "broker.sock")
    sock_dir.mkdir()
    (sock_dir / "broker.sock").write_text("stale")

    _listener.prepare_uds_path(sock_path)

    assert sock_dir.is_dir()
    assert not os.path.exists(sock_path)


def test_write_server_info_uds(tmp_path):
    info_path = str(tmp_path / "server-info")
    cfg = _listener.ListenerConfig(network="unix", address="/tmp/x.sock")

    _listener.write_server_info(cfg, path=info_path)

    with open(info_path) as f:
        content = f.read()
    assert '"protocol": "uds"' in content


def test_write_server_info_tcp(tmp_path):
    info_path = str(tmp_path / "server-info")
    cfg = _listener.ListenerConfig(network="tcp", address="127.0.0.1:8088")

    _listener.write_server_info(cfg, path=info_path)

    with open(info_path) as f:
        content = f.read()
    assert '"protocol": "tcp"' in content
