"""Headers (such as auth) to send on a peer's AgentCard fetch.

The headers go on the card request only, never on calls made through the client.
A caller authenticates each call itself, for example with a `ClientCallContext`
or an interceptor passed in `ClientConfig`.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

# The subset of RFC 9110 tokens that gRPC metadata also accepts (it lower-cases keys
# and allows only letters, digits, '-', '_' and '.'); printable ASCII, space and tab
# for values (no control characters).
_NAME = re.compile(r"[0-9A-Za-z_.-]+")
_VALUE = re.compile(r"[\t\x20-\x7e]*")


def validate_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Returns an independent copy of `headers`, or raises ValueError.

    Fails fast on malformed entries so a bad header never reaches the wire. The
    error names the header but never echoes its value, which may be a secret.
    """
    checked: dict[str, str] = {}
    seen: set[str] = set()
    for name, value in headers.items():
        if (
            not isinstance(name, str)
            or not _NAME.fullmatch(name)
            or name.lower().startswith("grpc-")
        ):
            raise ValueError(
                "kynomesh: header names may contain only letters, digits, '-', '_' and '.', "
                "and must not start with 'grpc-'"
            )
        if not isinstance(value, str):
            raise ValueError(f"kynomesh: value of header {name!r} must be a string")
        if not _VALUE.fullmatch(value):
            raise ValueError(f"kynomesh: value of header {name!r} must be printable ASCII")
        if name.lower() in seen:
            raise ValueError(f"kynomesh: header {name!r} is given more than once")
        seen.add(name.lower())
        checked[name] = value
    return checked
