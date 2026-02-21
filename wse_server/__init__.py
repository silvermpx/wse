"""WSE -- WebSocket Engine for real-time applications."""

from .router import SnapshotProvider, WSEConfig, create_wse_router

__version__ = "1.1.1"
__all__ = ["create_wse_router", "WSEConfig", "SnapshotProvider"]
