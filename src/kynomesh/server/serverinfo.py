"""Writes the agent server-info file that the broker reads at startup.

The file is a JSON document with the protocol, SDK language, SDK version, and
free-form metadata.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version

# DEFAULT_FILE_PATH is the in-pod location the broker reads at startup.
# Keep in sync with kmv1.ServerInfoFilePath in kynoproj/kynomesh.
DEFAULT_FILE_PATH = "/var/run/kynomesh/server-info"

_PACKAGE_NAME = "kynomesh"

LANGUAGE_PYTHON = "python"

PROTOCOL_UDS = "uds"
PROTOCOL_TCP = "tcp"


@dataclass(frozen=True)
class ServerInfo:
    """Information about the agent server that the broker consumes at startup.

    Field names match the broker's definition in kynoproj/kynomesh
    pkg/broker/serverinfo (camelCase on the wire).
    """

    protocol: str
    language: str = LANGUAGE_PYTHON
    sdk_version: str = ""
    metadata: dict[str, str] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            "protocol": self.protocol,
            "language": self.language,
            "version": self.sdk_version,
        }
        if self.metadata:
            data["metadata"] = self.metadata
        return data


def sdk_version() -> str:
    """Returns the version of this SDK as recorded in installed package metadata.

    Empty when the SDK is not installed as a package (e.g. running from a
    source checkout without an editable install).
    """
    try:
        return version(_PACKAGE_NAME)
    except PackageNotFoundError:
        return ""


def default(protocol: str) -> ServerInfo:
    """Returns a ServerInfo populated with this SDK's language and version."""
    return ServerInfo(protocol=protocol, sdk_version=sdk_version())


def write(path: str, info: ServerInfo) -> None:
    """Serializes info as JSON and writes it atomically to path.

    The parent directory is created if missing. Atomic write avoids the
    broker reading a half-written file.
    """
    if not path:
        raise ValueError("serverinfo: path is required")

    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)

    data = json.dumps(info.to_json_dict()).encode("utf-8")

    fd, tmp_path = tempfile.mkstemp(prefix=".server-info-", dir=parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, path)
    except OSError:
        os.remove(tmp_path)
        raise
