import asyncio
import logging
from typing import List
from unittest import mock

import pytest

from aio_pika import Channel


log = logging.getLogger(__name__)


class FakeConnection:
    """Minimal double matching the AbstractConnection surface
    accessed by the escalation callback."""

    def __init__(self) -> None:
        self.close_called = False
        self.is_closed = False
        self.transport: object = object()
        self.close = mock.AsyncMock()


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


async def test_escalate_on_close_registers_one_callback() -> None:
    connection = FakeConnection()
    channel = Channel(connection=connection)  # type: ignore  # noqa

    before = len(channel.close_callbacks)
    channel.escalate_on_close()

    assert len(channel.close_callbacks) == before + 1


async def test_escalate_on_close_is_idempotent() -> None:
    connection = FakeConnection()
    channel = Channel(connection=connection)  # type: ignore  # noqa

    before = len(channel.close_callbacks)
    channel.escalate_on_close()
    channel.escalate_on_close()

    assert len(channel.close_callbacks) == before + 1


# ---------------------------------------------------------------------------
# Close forwarding
# ---------------------------------------------------------------------------


async def test_channel_close_escalates_original_exception_once() -> None:
    connection = FakeConnection()
    channel = Channel(connection=connection)  # type: ignore  # noqa

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
    channel = Channel(connection=connection)  # type: ignore  # noqa

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


async def test_escalation_ignores_explicitly_closing_connection() -> None:
    connection = FakeConnection()
    connection.close_called = True
    channel = Channel(connection=connection)  # type: ignore  # noqa

    channel.escalate_on_close(timeout=5.0)

    await channel.close_callbacks(RuntimeError("boom"))

    # No close task should have been created.
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
    dead_modifier: object,
) -> None:
    connection = FakeConnection()
    dead_modifier(connection)  # type: ignore  # noqa
    channel = Channel(connection=connection)  # type: ignore  # noqa

    channel.escalate_on_close(timeout=5.0)

    await channel.close_callbacks(RuntimeError("boom"))

    assert connection.close.await_count == 0


# ---------------------------------------------------------------------------
# Concurrency guards
# ---------------------------------------------------------------------------


async def test_escalation_ignores_duplicate_callback_while_close_pending() -> (  # noqa: E501
    None
):
    connection = FakeConnection()
    channel = Channel(connection=connection)  # type: ignore  # noqa

    channel.escalate_on_close(timeout=5.0)

    close_started = asyncio.Event()
    release_close = asyncio.Event()

    async def blocking_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        await release_close.wait()

    connection.close = mock.AsyncMock(side_effect=blocking_close)

    exc = RuntimeError("independent death")
    await channel.close_callbacks(exc)
    await asyncio.wait_for(close_started.wait(), timeout=3.0)

    # Duplicate callback while the first close is still pending.
    await channel.close_callbacks(exc)

    # Release the first close so the test can finish.
    release_close.set()
    # Allow the close task to complete after release.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert connection.close.await_count == 1


async def test_escalation_timeout_is_bounded() -> None:
    connection = FakeConnection()
    channel = Channel(connection=connection)  # type: ignore  # noqa

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

    await channel.close_callbacks(RuntimeError("boom"))
    await asyncio.wait_for(close_started.wait(), timeout=3.0)

    # The escalation close is bounded by `timeout` (0.05 s).  Even though
    # connection.close hangs, the task must be cancelled when the bound
    # elapses.  If the timeout is not respected this will raise TimeoutError.
    await asyncio.wait_for(close_cancelled.wait(), timeout=3.0)


# ---------------------------------------------------------------------------
# Exception consumption
# ---------------------------------------------------------------------------


async def test_escalation_close_exception_is_consumed() -> None:
    connection = FakeConnection()
    channel = Channel(connection=connection)  # type: ignore  # noqa

    channel.escalate_on_close(timeout=5.0)

    close_started = asyncio.Event()

    async def failing_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        raise OSError("connection failed")

    connection.close = mock.AsyncMock(side_effect=failing_close)

    loop = asyncio.get_running_loop()
    captured: List[dict] = []

    def handler(ev_loop: asyncio.AbstractEventLoop, context: dict) -> None:
        captured.append(context)
        ev_loop.default_exception_handler(context)

    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(handler)
    try:
        await channel.close_callbacks(RuntimeError("boom"))
        await asyncio.wait_for(close_started.wait(), timeout=3.0)

        # Yield so the event loop processes any final task state.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        for ctx in captured:
            msg = ctx.get("message", "")
            assert "exception was never retrieved" not in msg, (
                f"Unhandled task exception: {ctx}"
            )

        # Also assert no unhandled-task warning for the consumed exception.
        assert captured == []
    finally:
        loop.set_exception_handler(old_handler)
