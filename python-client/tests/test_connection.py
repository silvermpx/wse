# =============================================================================
# WSE Python Client -- Connection regression tests
# =============================================================================

from __future__ import annotations

import asyncio

import pytest

from wse_client.connection import ConnectionManager


@pytest.mark.asyncio
async def test_force_reconnect_from_heartbeat_does_not_zombie() -> None:
    """_force_reconnect is invoked from inside _heartbeat_loop on idle timeout.

    Regression: it used to cancel-and-await self._heartbeat_task -- i.e. the
    CURRENT task -- so a CancelledError fired inside _force_reconnect before
    _schedule_reconnect() ran, leaving the socket open and no reconnect
    scheduled (a permanent zombie). The current task must be skipped so the
    reconnect is always scheduled.
    """
    conn = ConnectionManager("ws://localhost:5006/wse", token="t")
    scheduled: list[bool] = []
    # Stub out the actual reconnect scheduling; we only assert it is reached.
    conn._schedule_reconnect = lambda: scheduled.append(True)  # type: ignore[method-assign]
    conn._ws = None
    conn._ws_cm = None

    async def fake_heartbeat() -> None:
        # Mirrors the real path: the heartbeat task itself calls _force_reconnect.
        await conn._force_reconnect()

    task = asyncio.ensure_future(fake_heartbeat())
    # The task IS the heartbeat task -- the exact self-cancel scenario.
    conn._heartbeat_task = task

    # Must complete promptly (no self-cancel hang) and schedule the reconnect.
    await asyncio.wait_for(task, timeout=1.0)
    assert scheduled == [True], "reconnect must be scheduled (no zombie)"
    assert conn._heartbeat_task is None
