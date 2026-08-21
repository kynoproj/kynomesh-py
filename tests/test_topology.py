import json

import pytest

from kynomesh.client import _topology


@pytest.fixture(autouse=True)
def _reset_cache():
    _topology._reset_topology_cache()
    yield
    _topology._reset_topology_cache()
    _topology._topology_path = _topology.DEFAULT_TOPOLOGY_PATH


def _write_topology(path, data) -> None:
    path.write_text(json.dumps(data))


def test_lookup_peer_found(tmp_path):
    topo_path = tmp_path / "topology.json"
    _write_topology(
        topo_path,
        {
            "pattern": "star",
            "peers": [{"name": "worker-a", "url": "http://worker-a:8088"}],
        },
    )
    _topology._topology_path = str(topo_path)

    peer = _topology.lookup_peer("worker-a")

    assert peer.name == "worker-a"
    assert peer.url == "http://worker-a:8088"
    assert peer.is_managed


def test_lookup_peer_not_found(tmp_path):
    topo_path = tmp_path / "topology.json"
    _write_topology(topo_path, {"peers": []})
    _topology._topology_path = str(topo_path)

    with pytest.raises(_topology.PeerNotFoundError):
        _topology.lookup_peer("missing")


def test_lookup_peer_topology_not_available(tmp_path):
    _topology._topology_path = str(tmp_path / "does-not-exist.json")

    with pytest.raises(_topology.TopologyNotAvailableError):
        _topology.lookup_peer("worker-a")


def test_peer_names(tmp_path):
    topo_path = tmp_path / "topology.json"
    _write_topology(
        topo_path,
        {
            "peers": [
                {"name": "worker-a", "url": "http://worker-a:8088"},
                {"name": "worker-b", "url": "http://worker-b:8088"},
            ]
        },
    )
    _topology._topology_path = str(topo_path)

    assert _topology.peer_names() == ["worker-a", "worker-b"]


def test_external_peer_is_not_managed(tmp_path):
    topo_path = tmp_path / "topology.json"
    _write_topology(
        topo_path,
        {"peers": [{"name": "ext", "kind": "External", "url": "https://ext.example.com"}]},
    )
    _topology._topology_path = str(topo_path)

    peer = _topology.lookup_peer("ext")

    assert not peer.is_managed


def test_topology_read_once(tmp_path, monkeypatch):
    topo_path = tmp_path / "topology.json"
    _write_topology(topo_path, {"peers": [{"name": "worker-a"}]})
    _topology._topology_path = str(topo_path)

    calls = []
    original_open = open

    def counting_open(*args, **kwargs):
        calls.append(args)
        return original_open(*args, **kwargs)

    monkeypatch.setattr("builtins.open", counting_open)

    _topology.lookup_peer("worker-a")
    _topology.peer_names()

    assert len(calls) == 1
