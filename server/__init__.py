"""WSE -- WebSocket Event System for real-time applications."""

from .router import WSEConfig, SnapshotProvider, create_wse_router

__version__ = "1.0.0"
__all__ = ["create_wse_router", "WSEConfig", "SnapshotProvider"]
