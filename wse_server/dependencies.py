# =============================================================================
# WSE — WebSocket Event System
# =============================================================================

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import WebSocket

log = logging.getLogger("wse.dependencies")


def get_pubsub_bus(websocket: WebSocket) -> Any:
    """Resolve the PubSubBus from the FastAPI app state.

    This helper is intended for use as a FastAPI ``Depends()`` callable
    inside custom routers that need direct access to the bus outside of
    the main ``/wse`` endpoint.

    Parameters
    ----------
    websocket:
        The current WebSocket (automatically injected by FastAPI).

    Returns
    -------
    The PubSubBus instance stored in ``app.state.pubsub_bus``.

    Raises
    ------
    RuntimeError
        If the PubSubBus is not available in app state.
    """
    app = websocket.scope.get("app")
    if not app:
        request = websocket.scope.get("request")
        if request and hasattr(request, "app"):
            app = request.app

    if not app:
        raise RuntimeError(
            "Cannot access FastAPI app from WebSocket scope.  "
            "Ensure the WebSocket is being served by a FastAPI application."
        )

    bus = getattr(app.state, "pubsub_bus", None)
    if bus is None:
        raise RuntimeError(
            "PubSubBus not found in app.state.  "
            "Make sure to assign it during application lifespan/startup."
        )

    return bus


def get_app_service(websocket: WebSocket, attr_name: str) -> Any | None:
    """Resolve an arbitrary service from ``app.state`` by attribute name.

    Returns ``None`` when the attribute does not exist (fail-soft).

    Parameters
    ----------
    websocket:
        The current WebSocket.
    attr_name:
        Name of the attribute on ``app.state`` to retrieve.

    Returns
    -------
    The service instance or ``None``.
    """
    app = websocket.scope.get("app")
    if not app:
        request = websocket.scope.get("request")
        if request and hasattr(request, "app"):
            app = request.app

    if not app or not hasattr(app, "state"):
        return None

    return getattr(app.state, attr_name, None)
