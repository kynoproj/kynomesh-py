"""TLS handling for Managed-peer connections.

Managed peers are reached through their broker's externally-facing
listener, which always terminates real TLS with a broker-issued
self-signed certificate — the broker's cert is not meant to be checked
against a public CA. This mirrors Go's managedTLSConfig
(tls.Config{InsecureSkipVerify: true}): encrypt the hop, but skip
certificate validation, since the cert is self-signed and unpinned by
design.

Python's grpc package exposes no verify-skip hook (unlike Go's
crypto/tls), so gRPC connections to Managed peers fetch the broker's
certificate once per peer and pin it as the trusted root, with
hostname verification disabled via ssl_target_name_override. This is
TLS-encrypted but, like Go's InsecureSkipVerify, does not authenticate
the peer beyond "whatever certificate is presented on the first
connection."
"""

from __future__ import annotations

import socket
import ssl
from urllib.parse import urlsplit

import grpc
import httpx

# managed_httpx_client is an HTTP client that skips TLS verification (but
# keeps TLS encryption), used for Managed peers.
managed_httpx_client = httpx.AsyncClient(verify=False, timeout=30.0)


def _split_host_port(url: str) -> tuple[str, int]:
    parts = urlsplit(url if "//" in url else f"//{url}")
    host = parts.hostname
    if host is None:
        raise ValueError(f"kynomesh: cannot parse host from gRPC target {url!r}")
    port = parts.port or 443
    return host, port


def _fetch_peer_cert_pem(host: str, port: int, timeout: float = 5.0) -> bytes:
    """Fetches the PEM-encoded certificate presented by host:port.

    Does not validate the certificate; the caller pins whatever is
    returned as the trusted root for the real connection.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=timeout) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as tls_sock:
            der_cert = tls_sock.getpeercert(binary_form=True)
    if der_cert is None:
        raise ssl.SSLError(f"kynomesh: no certificate presented by {host}:{port}")
    return ssl.DER_cert_to_PEM_cert(der_cert).encode("ascii")


def managed_grpc_channel(target: str) -> grpc.aio.Channel:
    """Builds a gRPC channel to target, a Managed peer's gRPC interface URL.

    Fetches and pins the certificate the peer presents, then connects
    with hostname verification disabled so the pinned certificate is
    accepted regardless of the dialed host or IP.
    """
    host, port = _split_host_port(target)
    pem = _fetch_peer_cert_pem(host, port)
    credentials = grpc.ssl_channel_credentials(root_certificates=pem)
    options = [("grpc.ssl_target_name_override", host)]
    return grpc.aio.secure_channel(f"{host}:{port}", credentials, options=options)
