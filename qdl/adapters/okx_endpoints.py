"""OKX WebSocket endpoints: the one owner of the URLs and of their admission rule.

Purpose. Every Data Layer component that names an OKX WebSocket service takes
the URL from here: the production catalog generator, the L2 demand projection,
the Python supervisor used by certification scripts, and the acquisition-plan
validator. Before this module the same literal was copied into four places.

Boundary. These are *transport* endpoints. They are not part of any data
identity: no catalog revision, acquisition revision, binding ID, checkpoint or
canonical record is derived from them. Moving an endpoint must therefore never
require an identity change (see ``StableAcquisitionPlan.identity_digest``).

Venue fact (OKX API changelog, "WebSocket port 8443 discontinuation", last
updated 2026-09-30): OKX stops accepting WebSocket connections on port 8443 on
2026-10-31. Port 443 already serves every affected domain; the migration is to
drop ``:8443`` and keep host and path. The rule covers ``/ws/v5/public``,
``/ws/v5/private``, ``/ws/v5/business`` and ``/ws/v5/public-sbe``.
"""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

OKX_PUBLIC_WS = "wss://ws.okx.com/ws/v5/public"
OKX_BUSINESS_WS = "wss://ws.okx.com/ws/v5/business"

OKX_RETIRED_WS_PORT = 8443
OKX_RETIRED_WS_PORT_CUTOVER = "2026-10-31"
_SERVICES = frozenset({"public", "private", "business", "public-sbe"})


def require_okx_websocket_url(value: str | None, service: str) -> str:
    """Admit an OKX WebSocket URL for ``service`` or raise ``ValueError``.

    A URL that still names port 8443 is refused rather than tolerated: after
    the cutover it fails at connect time, after a role has already been
    recreated, which is the costliest place to discover it.
    """

    if service not in _SERVICES:
        raise ValueError(f"unknown OKX WebSocket service: {service}")
    parsed = urlsplit(value or "")
    host = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("OKX WebSocket URL has an invalid port") from error
    if parsed.scheme != "wss" or not (host == "okx.com" or host.endswith(".okx.com")):
        raise ValueError("OKX WebSocket URL must be wss on an okx.com host")
    if port == OKX_RETIRED_WS_PORT:
        raise ValueError(
            "OKX WebSocket port 8443 is discontinued on "
            f"{OKX_RETIRED_WS_PORT_CUTOVER}; drop the port (wss defaults to 443)"
        )
    if port not in (None, 443):
        raise ValueError("OKX WebSocket URL must use the default wss port")
    if parsed.path.rstrip("/") != f"/ws/v5/{service}" or parsed.query or parsed.fragment:
        raise ValueError(f"OKX WebSocket URL must be the /ws/v5/{service} endpoint")
    return str(value)


def without_retired_port(value: str) -> str:
    """Return ``value`` with an explicit ``:8443`` removed; host and path kept.

    Any other URL is returned unchanged, so applying this twice is a no-op.
    """

    parsed = urlsplit(value)
    if parsed.port != OKX_RETIRED_WS_PORT or parsed.hostname is None:
        return value
    netloc = parsed.hostname
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("OKX WebSocket URL must not carry credentials")
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
