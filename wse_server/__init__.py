"""WSE server -- real-time WebSocket engine with Rust acceleration.

Embed into a FastAPI app as a router::

    from fastapi import FastAPI
    from wse_server import create_wse_router, WSEConfig

    app = FastAPI()
    wse = create_wse_router(WSEConfig())
    app.include_router(wse, prefix="/wse")

See :class:`WSEConfig` for all configuration options (auth, Redis,
compression, encryption) and :class:`SnapshotProvider` for delivering
initial state to clients on connect.
"""

from .router import SnapshotProvider, WSEConfig, create_wse_router

__version__ = "1.4.4"
__all__ = ["create_wse_router", "WSEConfig", "SnapshotProvider"]
