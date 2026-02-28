"""Shared fixtures for WSE integration tests."""

import time

import pytest

from wse_server._wse_accel import RustWSEServer, rust_jwt_encode

# Use high ports to avoid conflicts
BASE_PORT = 15_007

# Shared JWT config
JWT_SECRET = b"test-secret-key-for-integration"
JWT_ISSUER = "wse-test"
JWT_AUDIENCE = "wse-test-api"


def _next_port():
    """Thread-safe port allocator."""
    _next_port.counter += 1
    return BASE_PORT + _next_port.counter


_next_port.counter = 0


def make_token(user_id: str = "test-user", exp_offset: int = 3600) -> str:
    now = int(time.time())
    return rust_jwt_encode(
        {
            "sub": user_id,
            "iss": JWT_ISSUER,
            "aud": JWT_AUDIENCE,
            "exp": now + exp_offset,
            "iat": now,
        },
        JWT_SECRET,
    )


@pytest.fixture(autouse=True)
def _clear_pending_events():
    """Clear the drain_until pending buffer between tests."""
    from tests.test_integration import _pending_events

    _pending_events.clear()
    yield
    _pending_events.clear()


@pytest.fixture()
def server_port():
    return _next_port()


@pytest.fixture()
def server(server_port):
    """Start a RustWSEServer in drain mode, yield it, stop on teardown."""
    srv = RustWSEServer(
        "127.0.0.1",
        server_port,
        max_connections=100,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
    )
    srv.enable_drain_mode()
    srv.start()
    time.sleep(0.05)  # let listener bind
    yield srv
    srv.stop()


@pytest.fixture()
def server_no_auth(server_port):
    """Server without JWT -- connections emit 'connect' with cookies."""
    srv = RustWSEServer("127.0.0.1", server_port, max_connections=100)
    srv.enable_drain_mode()
    srv.start()
    time.sleep(0.05)
    yield srv
    srv.stop()


@pytest.fixture()
def server_with_presence(server_port):
    """Server with presence tracking enabled."""
    srv = RustWSEServer(
        "127.0.0.1",
        server_port,
        max_connections=100,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        presence_enabled=True,
    )
    srv.enable_drain_mode()
    srv.start()
    time.sleep(0.05)
    yield srv
    srv.stop()


@pytest.fixture()
def server_with_recovery(server_port):
    """Server with message recovery enabled."""
    srv = RustWSEServer(
        "127.0.0.1",
        server_port,
        max_connections=100,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        recovery_enabled=True,
        recovery_buffer_size=64,
        recovery_ttl=60,
        recovery_max_messages=100,
    )
    srv.enable_drain_mode()
    srv.start()
    time.sleep(0.05)
    yield srv
    srv.stop()
