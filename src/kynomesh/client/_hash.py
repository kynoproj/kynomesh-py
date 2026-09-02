"""Peer AgentCard hash recording, for drift detection.

Each time this agent builds a peer's client, it records a hash of the
AgentCard the client was built from to a well-known file. The broker
reads that file to compare against a peer's live AgentCard and detect
drift between what a caller's already-built client is using and what
the peer currently advertises.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading

import rfc8785
from a2a.types.a2a_pb2 import AgentCard
from google.protobuf.json_format import MessageToDict

# _ENV_POD_NAME mirrors kynomesh.server's in-pod signal: set by the
# Kynomesh pod spec, absent in local dev.
_ENV_POD_NAME = "POD_NAME"


def _in_cluster() -> bool:
    return bool(os.environ.get(_ENV_POD_NAME))


# DEFAULT_PEER_HASHES_PATH is the in-pod location the broker reads to
# compare against a peer's live state (e.g. its AgentCard) for drift
# detection. Keep in sync with kmv1.PeerHashesFilePath in
# kynoproj/kynomesh.
DEFAULT_PEER_HASHES_PATH = "/var/run/kynomesh/peer-hashes.json"

# _peer_hashes_path is a test seam; production uses DEFAULT_PEER_HASHES_PATH.
_peer_hashes_path = DEFAULT_PEER_HASHES_PATH

# _init_lock/_initialized guard clearing the peer-hashes file exactly
# once per process, the first time any peer client is built — before
# that peer's hash (the first entry ever written) is recorded. A stale
# file from a previous process incarnation must never be read as
# current by the broker.
_init_lock = threading.Lock()
_initialized = False

# _hashes_lock serializes read-modify-write updates to the peer-hashes
# file; concurrent first-use of different peers can race to record
# their hash concurrently.
_hashes_lock = threading.Lock()


def hash_agent_card(card: AgentCard) -> str:
    """Returns the hex-encoded SHA-256 digest of card's JSON encoding,
    canonicalized per JCS (RFC 8785) before hashing.

    Using JCS because the hash should be consistent across different
    languages.
    """
    canonical = rfc8785.dumps(MessageToDict(card))
    return hashlib.sha256(canonical).hexdigest()


def record_peer_hash(name: str, card: AgentCard) -> None:
    """Hashes card and records it for name in the peer-hashes file,
    creating or updating only that peer's entry. The first call per
    process clears any pre-existing file first, so a stale entry from a
    prior process incarnation never lingers.

    A no-op outside a Kynomesh pod (e.g. local dev, tests hand-rolling a
    topology file): there is no broker there to read the file, and
    writing it would just leave a stray file with no consumer.

    Errors are raised to the caller, which treats this as best-effort
    and swallows them: a failure to record the hash must not fail the
    client build the caller actually asked for.
    """
    if not _in_cluster():
        return

    hash_ = hash_agent_card(card)

    global _initialized
    with _init_lock:
        if not _initialized:
            try:
                os.remove(_peer_hashes_path)
            except FileNotFoundError:
                pass
            _initialized = True

    with _hashes_lock:
        hashes = _read_peer_hashes(_peer_hashes_path)
        hashes[name] = hash_
        _write_peer_hashes(_peer_hashes_path, hashes)


def _read_peer_hashes(path: str) -> dict[str, str]:
    """Returns the current peer name -> hash map, or an empty map if the
    file does not exist yet.
    """
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        return {}
    return json.loads(raw)


def _write_peer_hashes(path: str, hashes: dict[str, str]) -> None:
    """Serializes hashes as JSON and writes it atomically to path, so a
    concurrent reader (the broker) never observes a half-written file.
    """
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    data = json.dumps(hashes).encode()

    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".peer-hashes-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass
        raise


def _reset_peer_hashes_state() -> None:
    """Resets the clear-once guard and removes any file at
    _peer_hashes_path. Test-only; not exposed as a public API.
    """
    global _initialized
    with _init_lock:
        _initialized = False
    with _hashes_lock:
        try:
            os.remove(_peer_hashes_path)
        except FileNotFoundError:
            pass
