"""Tests for WSE router factory and config.

These are pure unit tests -- they do NOT start a server, do NOT create
WebSocket connections, and do NOT require Redis.  They test the factory
function, configuration defaults, and protocol definitions.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wse_server.router import (
    PROTOCOL_VERSION,
    SnapshotProvider,
    WSEConfig,
    create_wse_router,
)

# =========================================================================
# PROTOCOL_VERSION
# =========================================================================


class TestProtocolVersion:
    def test_version_is_2(self):
        assert PROTOCOL_VERSION == 2


# =========================================================================
# WSEConfig defaults
# =========================================================================


class TestWSEConfigDefaults:
    def test_default_auth_handler(self):
        config = WSEConfig()
        assert config.auth_handler is None

    def test_default_snapshot_provider(self):
        config = WSEConfig()
        assert config.snapshot_provider is None

    def test_default_topics_empty(self):
        config = WSEConfig()
        assert config.default_topics == []

    def test_default_allowed_origins_empty(self):
        config = WSEConfig()
        assert config.allowed_origins == []

    def test_default_max_message_size(self):
        config = WSEConfig()
        assert config.max_message_size == 1_048_576  # 1 MB

    def test_default_heartbeat_interval(self):
        config = WSEConfig()
        assert config.heartbeat_interval == 15.0

    def test_default_idle_timeout(self):
        config = WSEConfig()
        assert config.idle_timeout == 90.0

    def test_default_compression(self):
        config = WSEConfig()
        assert config.enable_compression is True

    def test_default_debug(self):
        config = WSEConfig()
        assert config.enable_debug is False

    def test_default_redis_client(self):
        config = WSEConfig()
        assert config.redis_client is None


# =========================================================================
# WSEConfig custom values
# =========================================================================


class TestWSEConfigCustom:
    def test_custom_topics(self):
        config = WSEConfig(default_topics=["orders", "prices"])
        assert config.default_topics == ["orders", "prices"]

    def test_custom_allowed_origins(self):
        config = WSEConfig(allowed_origins=["https://example.com"])
        assert config.allowed_origins == ["https://example.com"]

    def test_custom_heartbeat(self):
        config = WSEConfig(heartbeat_interval=30.0)
        assert config.heartbeat_interval == 30.0

    def test_custom_idle_timeout(self):
        config = WSEConfig(idle_timeout=120.0)
        assert config.idle_timeout == 120.0

    def test_custom_max_message_size(self):
        config = WSEConfig(max_message_size=2_097_152)
        assert config.max_message_size == 2_097_152

    def test_custom_auth_handler(self):
        async def my_auth(ws):
            return "user_123"

        config = WSEConfig(auth_handler=my_auth)
        assert config.auth_handler is my_auth

    def test_enable_debug(self):
        config = WSEConfig(enable_debug=True)
        assert config.enable_debug is True


# =========================================================================
# WSEConfig field independence
# =========================================================================


class TestWSEConfigFieldIndependence:
    def test_default_topics_not_shared(self):
        """Ensure default_factory creates independent lists."""
        c1 = WSEConfig()
        c2 = WSEConfig()
        c1.default_topics.append("foo")
        assert "foo" not in c2.default_topics

    def test_allowed_origins_not_shared(self):
        c1 = WSEConfig()
        c2 = WSEConfig()
        c1.allowed_origins.append("https://evil.com")
        assert "https://evil.com" not in c2.allowed_origins


# =========================================================================
# SnapshotProvider protocol
# =========================================================================


class TestSnapshotProvider:
    def test_protocol_defines_get_snapshot(self):
        """SnapshotProvider protocol should require get_snapshot method."""
        assert hasattr(SnapshotProvider, "get_snapshot")

    def test_compliant_class_satisfies_protocol(self):
        class MySnapshotService:
            async def get_snapshot(self, user_id: str, topics: list) -> dict:
                return {"topic_a": {"data": "snapshot"}}

        provider = MySnapshotService()
        assert isinstance(provider, SnapshotProvider)

    def test_non_compliant_class_fails_protocol(self):
        class BadProvider:
            pass

        provider = BadProvider()
        assert not isinstance(provider, SnapshotProvider)


# =========================================================================
# create_wse_router factory
# =========================================================================


class TestCreateWSERouter:
    def test_returns_api_router(self):
        from fastapi import APIRouter

        config = WSEConfig()
        router = create_wse_router(config)
        assert isinstance(router, APIRouter)

    def test_has_websocket_route(self):
        config = WSEConfig()
        router = create_wse_router(config)
        routes = [r.path for r in router.routes]
        assert "/wse" in routes

    def test_has_health_endpoint(self):
        config = WSEConfig()
        router = create_wse_router(config)
        routes = [r.path for r in router.routes]
        assert "/wse/health" in routes

    def test_no_debug_endpoints_by_default(self):
        config = WSEConfig(enable_debug=False)
        router = create_wse_router(config)
        routes = [r.path for r in router.routes]
        assert "/wse/debug" not in routes
        assert "/wse/compression-test" not in routes

    def test_debug_endpoints_when_enabled(self):
        config = WSEConfig(enable_debug=True)
        router = create_wse_router(config)
        routes = [r.path for r in router.routes]
        assert "/wse/debug" in routes
        assert "/wse/compression-test" in routes

    def test_different_configs_produce_different_routers(self):
        config1 = WSEConfig(enable_debug=False)
        config2 = WSEConfig(enable_debug=True)
        r1 = create_wse_router(config1)
        r2 = create_wse_router(config2)
        routes1 = [r.path for r in r1.routes]
        routes2 = [r.path for r in r2.routes]
        assert "/wse/debug" not in routes1
        assert "/wse/debug" in routes2

    def test_router_with_custom_topics(self):
        """Router creation should work with custom topics in config."""
        config = WSEConfig(default_topics=["orders", "positions", "balances"])
        router = create_wse_router(config)
        # Just verifying it creates successfully
        assert router is not None

    def test_router_with_auth_handler(self):
        """Router creation should work with a custom auth handler."""

        async def auth(ws):
            return "authenticated_user"

        config = WSEConfig(auth_handler=auth)
        router = create_wse_router(config)
        assert router is not None

    def test_router_with_snapshot_provider(self):
        """Router creation should work with a snapshot provider."""

        class MySnapshot:
            async def get_snapshot(self, user_id: str, topics: list) -> dict:
                return {}

        config = WSEConfig(snapshot_provider=MySnapshot())
        router = create_wse_router(config)
        assert router is not None
