import json

import pytest

from kynomesh.server import serverinfo


def test_write_creates_parent_dir_and_content(tmp_path):
    path = str(tmp_path / "nested" / "server-info")
    info = serverinfo.ServerInfo(protocol=serverinfo.PROTOCOL_TCP, sdk_version="1.2.3")

    serverinfo.write(path, info)

    with open(path) as f:
        data = json.load(f)
    assert data == {"protocol": "tcp", "language": "python", "version": "1.2.3"}


def test_write_includes_metadata_when_present(tmp_path):
    path = str(tmp_path / "server-info")
    info = serverinfo.ServerInfo(
        protocol=serverinfo.PROTOCOL_UDS, metadata={"foo": "bar"}
    )

    serverinfo.write(path, info)

    with open(path) as f:
        data = json.load(f)
    assert data["metadata"] == {"foo": "bar"}


def test_write_requires_path():
    with pytest.raises(ValueError):
        serverinfo.write("", serverinfo.default(serverinfo.PROTOCOL_TCP))


def test_default_sets_language_and_protocol():
    info = serverinfo.default(serverinfo.PROTOCOL_UDS)
    assert info.protocol == serverinfo.PROTOCOL_UDS
    assert info.language == serverinfo.LANGUAGE_PYTHON
