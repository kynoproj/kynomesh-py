import json
import os

import pytest
import rfc8785
from a2a.types.a2a_pb2 import AgentCard

from kynomesh.client import _hash


@pytest.fixture(autouse=True)
def _reset_state(tmp_path):
    prev_path = _hash._peer_hashes_path
    _hash._peer_hashes_path = str(tmp_path / "peer-hashes.json")
    _hash._reset_peer_hashes_state()
    yield
    _hash._reset_peer_hashes_state()
    _hash._peer_hashes_path = prev_path


def test_hash_agent_card_deterministic():
    card = AgentCard(name="worker-a", version="0.0.1")
    assert _hash.hash_agent_card(card) == _hash.hash_agent_card(card)


def test_hash_agent_card_differs_for_different_content():
    h1 = _hash.hash_agent_card(AgentCard(name="worker-a", version="0.0.1"))
    h2 = _hash.hash_agent_card(AgentCard(name="worker-a", version="0.0.2"))
    assert h1 != h2


def test_canonicalize_key_order_independent():
    in_order = json.loads('{"name":"worker-a","version":"0.0.1"}')
    reordered = json.loads('{"version":"0.0.1","name":"worker-a"}')
    assert rfc8785.dumps(in_order) == rfc8785.dumps(reordered)


def test_record_peer_hash_noop_outside_pod(monkeypatch):
    monkeypatch.delenv("POD_NAME", raising=False)
    _hash.record_peer_hash("worker-a", AgentCard(name="worker-a"))
    assert not os.path.exists(_hash._peer_hashes_path)


def test_record_peer_hash_writes_entry_in_pod(monkeypatch):
    monkeypatch.setenv("POD_NAME", "test-pod")
    card = AgentCard(name="worker-a", version="0.0.1")
    _hash.record_peer_hash("worker-a", card)

    hashes = _hash._read_peer_hashes(_hash._peer_hashes_path)
    assert hashes == {"worker-a": _hash.hash_agent_card(card)}


def test_record_peer_hash_accumulates_across_peers(monkeypatch):
    monkeypatch.setenv("POD_NAME", "test-pod")
    card_a = AgentCard(name="worker-a", version="0.0.1")
    card_b = AgentCard(name="worker-b", version="0.0.1")

    _hash.record_peer_hash("worker-a", card_a)
    _hash.record_peer_hash("worker-b", card_b)

    hashes = _hash._read_peer_hashes(_hash._peer_hashes_path)
    assert hashes == {
        "worker-a": _hash.hash_agent_card(card_a),
        "worker-b": _hash.hash_agent_card(card_b),
    }


def test_record_peer_hash_clears_stale_file_on_first_use(monkeypatch):
    monkeypatch.setenv("POD_NAME", "test-pod")
    _hash._write_peer_hashes(_hash._peer_hashes_path, {"stale-peer": "deadbeef"})

    card = AgentCard(name="worker-a", version="0.0.1")
    _hash.record_peer_hash("worker-a", card)

    hashes = _hash._read_peer_hashes(_hash._peer_hashes_path)
    assert "stale-peer" not in hashes
    assert hashes["worker-a"] == _hash.hash_agent_card(card)


def test_record_peer_hash_does_not_reclear_on_second_call(monkeypatch):
    monkeypatch.setenv("POD_NAME", "test-pod")
    card_a = AgentCard(name="worker-a", version="0.0.1")
    card_b = AgentCard(name="worker-b", version="0.0.1")

    _hash.record_peer_hash("worker-a", card_a)
    # A stale-looking file written between the two calls (simulating
    # something else touching the file) must not be wiped by the
    # second record_peer_hash call: the clear-once guard only fires
    # once per process.
    _hash._write_peer_hashes(_hash._peer_hashes_path, {"worker-a": "deadbeef"})
    _hash.record_peer_hash("worker-b", card_b)

    hashes = _hash._read_peer_hashes(_hash._peer_hashes_path)
    assert hashes["worker-b"] == _hash.hash_agent_card(card_b)
