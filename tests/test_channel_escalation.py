import asyncio
from typing import Callable, List, Optional, cast
from unittest import mock

import pytest

from aio_pika import Channel
from aio_pika.abc import AbstractConnection


class FakeConnection:
    """Minimal double matching the AbstractConnection surface
    accessed by the escalation callback."""

    def __init__(self) -> None:
        self.close_called = False
        self.is_closed = False
        self.transport: Optional[object] = object()
        self.close = mock.AsyncMock()

    def _mark_close_called(self) -> None:
        self.close_called = True


async def _drain_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Yield until every callback scheduled before this point has run.

    Scheduled as a sentinel via ``call_soon`` so the event loop processes
    any task finalization / exception logging that was queued before we
    assert on observable state.  Bounded by ``wait_for`` so a broken
    implementation cannot hang the test.
    """
    drained = asyncio.Event()
    loop.call_soon(drained.set)
    await asyncio.wait_for(drained.wait(), timeout=3.0)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


async def test_escalate_on_close_registers_one_callback() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    before = len(channel.close_callbacks)
    channel.escalate_on_close()

    assert len(channel.close_callbacks) == before + 1


async def test_escalate_on_close_is_idempotent() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    before = len(channel.close_callbacks)
    channel.escalate_on_close()
    channel.escalate_on_close()

    assert len(channel.close_callbacks) == before + 1


# ---------------------------------------------------------------------------
# Close forwarding
# ---------------------------------------------------------------------------


async def test_channel_close_escalates_original_exception_once() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    channel.escalate_on_close(timeout=5.0)

    close_started = asyncio.Event()

    async def track_close(*args: object, **kwargs: object) -> None:
        close_started.set()

    connection.close = mock.AsyncMock(side_effect=track_close)

    original_exc = RuntimeError("independent channel death")
    await channel.close_callbacks(original_exc)
    await asyncio.wait_for(close_started.wait(), timeout=3.0)

    assert connection.close.await_count == 1
    # Positional args should be (original_exc,).
    assert connection.close.await_args is not None
    assert connection.close.await_args.args == (original_exc,)


async def test_channel_close_without_exception_calls_connection_close_without_reason() -> (  # noqa: E501
    None
):
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    channel.escalate_on_close(timeout=5.0)

    close_called = asyncio.Event()

    async def track_close(*args: object, **kwargs: object) -> None:
        close_called.set()

    connection.close = mock.AsyncMock(side_effect=track_close)

    await channel.close_callbacks(None)
    await asyncio.wait_for(close_called.wait(), timeout=3.0)

    assert connection.close.await_count == 1
    # Called with no positional and no keyword arguments.
    assert connection.close.await_args is not None
    assert connection.close.await_args.args == ()


# ---------------------------------------------------------------------------
# Guard conditions
# ---------------------------------------------------------------------------


async def test_explicit_channel_close_does_not_close_connection() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close()
    channel._channel = mock.Mock()
    channel._channel.close = mock.AsyncMock()

    await channel.close()

    assert connection.close.await_count == 0
    assert channel._channel.close.await_count == 1


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
async def test_escalate_on_close_rejects_invalid_timeout(
    timeout: float,
) -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    with pytest.raises(ValueError, match="positive finite"):
        channel.escalate_on_close(timeout)

    assert len(channel.close_callbacks) == 1


async def test_escalate_on_close_preserves_first_timeout() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    channel.escalate_on_close(timeout=1.0)
    channel.escalate_on_close(timeout=2.0)

    assert channel._escalation_timeout == 1.0


async def test_escalation_ignores_explicitly_closing_connection() -> None:
    connection = FakeConnection()
    connection.close_called = True
    channel = Channel(connection=cast(AbstractConnection, connection))

    channel.escalate_on_close(timeout=5.0)

    loop = asyncio.get_running_loop()
    await channel.close_callbacks(RuntimeError("boom"))
    # Let any scheduled escalation task run before asserting it did nothing.
    await _drain_loop(loop)

    assert connection.close.await_count == 0


@pytest.mark.parametrize(
    "scenario,dead_modifier",
    [
        ("is_closed", lambda c: setattr(c, "is_closed", True)),
        ("no_transport", lambda c: setattr(c, "transport", None)),
    ],
)
async def test_escalation_ignores_closed_or_dead_connection(
    scenario: str,
    dead_modifier: Callable[[FakeConnection], None],
) -> None:
    connection = FakeConnection()
    dead_modifier(connection)
    channel = Channel(connection=cast(AbstractConnection, connection))

    channel.escalate_on_close(timeout=5.0)

    loop = asyncio.get_running_loop()
    await channel.close_callbacks(RuntimeError("boom"))
    # Let any scheduled escalation task run before asserting it did nothing.
    await _drain_loop(loop)

    assert connection.close.await_count == 0


# ---------------------------------------------------------------------------
# Concurrency guards
# ---------------------------------------------------------------------------


async def test_channel_close_clears_cancelled_escalation_state() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close()

    channel._escalation_scheduled = True
    channel._escalation_task = asyncio.create_task(asyncio.sleep(10))
    await channel.close()
    await _drain_loop(asyncio.get_running_loop())

    assert channel._escalation_task is None
    assert not channel._escalation_scheduled


async def test_explicit_close_wins_escalation_before_task_starts() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close()
    channel._escalation_task = asyncio.create_task(
        channel._run_escalation(connection, RuntimeError("boom")),
    )

    await channel.close()
    await _drain_loop(asyncio.get_running_loop())

    assert connection.close.await_count == 0
    assert not channel._escalation_scheduled


async def test_explicit_close_wins_started_escalation_race() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close()
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    async def delayed_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        await release_close.wait()

    connection.close = mock.AsyncMock(side_effect=delayed_close)
    await channel.close_callbacks(RuntimeError("boom"))
    await asyncio.wait_for(close_started.wait(), timeout=3.0)
    await channel.close()
    release_close.set()
    await _drain_loop(asyncio.get_running_loop())

    assert connection.close.await_count == 1


async def test_escalation_ignores_duplicate_callback_while_close_pending() -> (  # noqa: E501
    None
):
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    channel.escalate_on_close(timeout=5.0)

    close_started = asyncio.Event()
    close_completed = asyncio.Event()
    release_close = asyncio.Event()

    async def blocking_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        try:
            await release_close.wait()
        finally:
            close_completed.set()

    connection.close = mock.AsyncMock(side_effect=blocking_close)

    exc = RuntimeError("independent death")
    first = asyncio.ensure_future(channel.close_callbacks(exc))
    await asyncio.wait_for(close_started.wait(), timeout=3.0)

    # Duplicate callback while the first close is still pending.  This must
    # not start a second close; bounded so a non-guarding implementation
    # fails with TimeoutError instead of hanging the test.
    await asyncio.wait_for(channel.close_callbacks(exc), timeout=3.0)

    # Release the first close so the test can finish.
    release_close.set()
    await asyncio.wait_for(close_completed.wait(), timeout=3.0)
    await asyncio.wait_for(first, timeout=3.0)

    assert connection.close.await_count == 1


async def test_escalation_timeout_is_bounded() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    timeout = 0.05
    channel.escalate_on_close(timeout=timeout)

    hang = asyncio.Event()
    close_started = asyncio.Event()
    close_cancelled = asyncio.Event()

    async def hanging_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        try:
            await hang.wait()
        except asyncio.CancelledError:
            close_cancelled.set()
            raise

    connection.close = mock.AsyncMock(side_effect=hanging_close)

    loop = asyncio.get_running_loop()
    await channel.close_callbacks(RuntimeError("boom"))
    await asyncio.wait_for(close_started.wait(), timeout=3.0)

    # Measure how long after the close starts before the configured timeout
    # (0.05 s) cancels it.  Assert the cancellation lands near that deadline
    # (with scheduling slack) rather than on the outer 3-second wait_for.
    started_at = loop.time()
    await asyncio.wait_for(close_cancelled.wait(), timeout=3.0)
    elapsed = loop.time() - started_at

    assert elapsed < max(timeout * 3, 0.2), (
        f"escalation cancelled in {elapsed:.3f}s, expected ~{timeout:.3f}s"
    )


# ---------------------------------------------------------------------------
# Exception consumption
# ---------------------------------------------------------------------------


async def test_escalation_close_exception_is_consumed() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    channel.escalate_on_close(timeout=5.0)

    close_started = asyncio.Event()
    close_completed = asyncio.Event()

    async def failing_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        try:
            raise OSError("connection failed")
        finally:
            close_completed.set()

    connection.close = mock.AsyncMock(side_effect=failing_close)

    loop = asyncio.get_running_loop()
    captured: List[dict] = []

    def handler(ev_loop: asyncio.AbstractEventLoop, context: dict) -> None:
        captured.append(context)
        ev_loop.default_exception_handler(context)

    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(handler)
    try:
        invocation = asyncio.ensure_future(
            channel.close_callbacks(RuntimeError("boom")),
        )
        await asyncio.wait_for(close_started.wait(), timeout=3.0)
        await asyncio.wait_for(close_completed.wait(), timeout=3.0)
        await asyncio.wait_for(invocation, timeout=3.0)

        # Let the event loop deliver any "exception was never retrieved"
        # diagnostics that were queued when the escalation task finished.
        await _drain_loop(loop)

        for ctx in captured:
            msg = ctx.get("message", "")
            assert "exception was never retrieved" not in msg, (
                f"Unhandled task exception: {ctx}"
            )

        assert captured == []
    finally:
        loop.set_exception_handler(old_handler)
