import asyncio
from typing import Awaitable, List, Literal, Optional, cast
from unittest import mock

import pytest
from yarl import URL

from aio_pika import Channel, Connection
from aio_pika.abc import AbstractChannel, AbstractConnection


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

    def _reset_close_called(self) -> None:
        self.close_called = False


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


async def test_escalation_ignores_closed_connection() -> None:
    connection = FakeConnection()
    connection.is_closed = True
    channel = Channel(connection=cast(AbstractConnection, connection))

    channel.escalate_on_close(timeout=5.0)

    loop = asyncio.get_running_loop()
    await channel.close_callbacks(RuntimeError("boom"))
    # Let any scheduled escalation task run before asserting it did nothing.
    await _drain_loop(loop)

    assert connection.close.await_count == 0


async def test_escalation_ignores_dead_transport() -> None:
    """A channel whose connection has no transport
    must not attempt escalation."""
    connection = FakeConnection()
    connection.transport = None
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close()

    await channel.close_callbacks(RuntimeError("transport already dead"))
    await _drain_loop(asyncio.get_running_loop())

    assert connection.close.await_count == 0
    assert not channel._escalation_scheduled


async def test_escalation_ignores_dead_transport_during_reconnect() -> None:
    """A robust connection in the reconnect window has transport=None
    but close_called=False and is_closed=False. Escalation must not proceed."""
    connection = FakeConnection()
    connection.close_called = False
    connection.is_closed = False
    connection.transport = None
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close(timeout=5.0)

    loop = asyncio.get_running_loop()
    await channel.close_callbacks(RuntimeError("channel died during reconnect"))
    await _drain_loop(loop)

    assert not connection.close_called
    assert connection.close.await_count == 0
    assert not channel._escalation_scheduled


async def test_existing_abstract_connection_subclass_remains_instantiable() -> (  # noqa: E501
    None
):
    """A concrete subclass that was valid before this feature must remain
    instantiable after the feature is added.  The test fails while
    _mark_close_called / _reset_close_called are abstract methods and passes
    once they are removed from the abc."""
    loop = asyncio.get_running_loop()

    class MinimalConnection(AbstractConnection):  # type: ignore[misc]
        """Minimal concrete subclass that does NOT implement
        _mark_close_called or _reset_close_called."""

        def __init__(self, url: URL) -> None:
            self._closed = loop.create_future()

        @property
        def is_closed(self) -> bool:
            raise NotImplementedError

        @property
        def close_called(self) -> bool:
            raise NotImplementedError

        async def close(  # type: ignore[override]
            self,
            exc: object = None,
        ) -> None:
            raise NotImplementedError

        def closed(self) -> Awaitable[Literal[True]]:
            raise NotImplementedError

        async def connect(self, timeout: object = None) -> None:
            raise NotImplementedError

        def channel(  # type: ignore[override]
            self,
            channel_number: object = None,
            publisher_confirms: bool = True,
            on_return_raises: bool = False,
        ) -> AbstractChannel:
            raise NotImplementedError

        async def ready(self) -> None:
            raise NotImplementedError

        async def __aenter__(self) -> AbstractConnection:
            raise NotImplementedError

        async def __aexit__(self, *args: object) -> None:
            raise NotImplementedError

        async def update_secret(  # type: ignore[override]
            self,
            new_secret: str,
            **kwargs: object,
        ) -> object:
            raise NotImplementedError

    conn = MinimalConnection(url=URL("amqp://guest:guest@localhost/"))
    assert conn is not None


async def test_channel_close_does_not_hang_after_escalation_timeout() -> None:
    """After escalation times out, channel.close() must complete within
    a bounded deadline even when the child connection.close() task is
    still running (because _wait_for_connection_close must not await
    a never-ending child task without a timeout)."""
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close(timeout=0.05)

    hang = asyncio.Event()
    close_started = asyncio.Event()

    async def hanging_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        await hang.wait()

    connection.close = mock.AsyncMock(side_effect=hanging_close)

    loop = asyncio.get_running_loop()
    captured: List[dict] = []
    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: captured.append(context))
    try:
        await channel.close_callbacks(RuntimeError("boom"))
        await asyncio.wait_for(close_started.wait(), timeout=3.0)

        # Wait for escalation timeout to fire
        task = channel._escalation_task
        assert task is not None
        await asyncio.wait_for(task, timeout=3.0)

        # After escalation timed out, _connection_close_task is still set
        # to the hanging child task.
        assert channel._connection_close_task is not None

        # This must not hang — currently it hangs because
        # _wait_for_connection_close awaits the never-ending child task
        # without a timeout.
        await asyncio.wait_for(channel.close(), timeout=1.0)

        assert connection.close.await_count == 1
        assert channel._connection_close_task is None
        assert captured == []
    finally:
        loop.set_exception_handler(old_handler)


async def test_explicit_close_does_not_wait_for_escalation_timeout() -> None:
    """Explicit cleanup uses its own bound, not the escalation deadline."""
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close(timeout=30.0)

    close_started = asyncio.Event()
    hanging = asyncio.Event()

    async def hanging_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        await hanging.wait()

    connection.close = mock.AsyncMock(side_effect=hanging_close)

    await channel.close_callbacks(RuntimeError("boom"))
    await asyncio.wait_for(close_started.wait(), timeout=3.0)

    await asyncio.wait_for(channel.close(), timeout=1.0)

    assert connection.close.await_count == 1
    assert channel._connection_close_task is None


async def test_late_connection_close_exception_is_consumed() -> None:
    """A late exception raised by the child connection.close() task after
    the escalation timeout must be consumed (not leaked to the event loop
    exception handler)."""
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close(timeout=0.05)

    release_child = asyncio.Event()
    close_started = asyncio.Event()
    sentinel = RuntimeError("child close failure")
    captured: List[dict] = []

    async def failing_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        await release_child.wait()
        raise sentinel

    connection.close = mock.AsyncMock(side_effect=failing_close)

    loop = asyncio.get_running_loop()
    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: captured.append(context))
    try:
        await channel.close_callbacks(RuntimeError("boom"))
        await asyncio.wait_for(close_started.wait(), timeout=3.0)

        task = channel._escalation_task
        assert task is not None
        await asyncio.wait_for(task, timeout=3.0)

        # Start cleanup.  In the current implementation this blocks on
        # _wait_for_connection_close which awaits the child task.
        close_call = asyncio.create_task(channel.close())

        # Release the child task so it can raise its sentinel.
        release_child.set()

        await asyncio.wait_for(close_call, timeout=1.0)
        await _drain_loop(loop)

        assert channel._connection_close_task is None
        assert captured == []
    finally:
        loop.set_exception_handler(old_handler)


async def test_channel_close_logs_connection_close_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close(timeout=0.05)

    release_child = asyncio.Event()
    close_started = asyncio.Event()
    sentinel = RuntimeError("child close failure")

    async def failing_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        await release_child.wait()
        raise sentinel

    connection.close = mock.AsyncMock(side_effect=failing_close)

    await channel.close_callbacks(RuntimeError("channel failure"))
    await asyncio.wait_for(close_started.wait(), timeout=3.0)

    task = channel._escalation_task
    assert task is not None
    await asyncio.wait_for(task, timeout=3.0)

    release_child.set()
    with caplog.at_level("WARNING", logger="aio_pika.channel"):
        await asyncio.wait_for(channel.close(), timeout=1.0)

    assert any(
        "Channel close cleanup failed" in record.message
        and record.exc_info is not None
        and record.exc_info[1] is sentinel
        for record in caplog.records
    )


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
        channel._run_escalation(
            cast(AbstractConnection, connection),
            RuntimeError("boom"),
        ),
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

    explicit_close = asyncio.create_task(channel.close())
    await _drain_loop(asyncio.get_running_loop())
    assert not explicit_close.done()

    release_close.set()
    await asyncio.wait_for(explicit_close, timeout=3.0)

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


async def test_connection_close_marks_closed_after_cancel() -> None:
    connection = Connection(url=URL("amqp://guest:guest@localhost/"))
    transport = mock.Mock()
    transport.close = mock.AsyncMock(side_effect=asyncio.CancelledError)
    connection.transport = transport

    with pytest.raises(asyncio.CancelledError):
        await connection.close(RuntimeError("close cancelled"))

    assert connection.is_closed
    assert connection.transport is None


async def test_escalation_timeout_is_bounded() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))

    timeout = 0.05
    channel.escalate_on_close(timeout=timeout)

    hang = asyncio.Event()
    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    async def hanging_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        try:
            await hang.wait()
        finally:
            close_finished.set()

    connection.close = mock.AsyncMock(side_effect=hanging_close)

    loop = asyncio.get_running_loop()
    await channel.close_callbacks(RuntimeError("boom"))
    await asyncio.wait_for(close_started.wait(), timeout=3.0)

    # The escalation deadline must finish orchestration without cancelling the
    # underlying close operation, which owns the connection's terminal state.
    started_at = loop.time()
    task = channel._escalation_task
    assert task is not None
    await asyncio.wait_for(task, timeout=3.0)
    elapsed = loop.time() - started_at

    assert elapsed < max(timeout * 10, 1.0), (
        f"escalation finished in {elapsed:.3f}s, expected ~{timeout:.3f}s"
    )
    assert not close_finished.is_set()

    hang.set()
    await asyncio.wait_for(close_finished.wait(), timeout=3.0)
    await channel.close()
    assert connection.close.await_count == 1
    assert channel._connection_close_task is None


async def test_escalation_timeout_late_exception_is_consumed() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close(timeout=0.05)

    close_started = asyncio.Event()
    release_close = asyncio.Event()
    captured: List[dict] = []

    async def failing_close(*args: object, **kwargs: object) -> None:
        close_started.set()
        await release_close.wait()
        raise OSError("late connection failure")

    connection.close = mock.AsyncMock(side_effect=failing_close)
    loop = asyncio.get_running_loop()
    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: captured.append(context))
    try:
        await channel.close_callbacks(RuntimeError("boom"))
        await asyncio.wait_for(close_started.wait(), timeout=3.0)
        task = channel._escalation_task
        assert task is not None
        await asyncio.wait_for(task, timeout=3.0)
        release_close.set()
        await _drain_loop(loop)
        assert channel._connection_close_task is None
        assert captured == []
    finally:
        loop.set_exception_handler(old_handler)


async def test_escalation_child_cancellation_is_consumed() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close()
    connection.close = mock.AsyncMock(side_effect=asyncio.CancelledError)

    await channel.close_callbacks(RuntimeError("boom"))
    await _drain_loop(asyncio.get_running_loop())

    assert channel._escalation_task is None
    assert channel._connection_close_task is None


async def test_completed_escalation_can_run_again() -> None:
    connection = FakeConnection()
    channel = Channel(connection=cast(AbstractConnection, connection))
    channel.escalate_on_close()

    await channel.close_callbacks(RuntimeError("first death"))
    await _drain_loop(asyncio.get_running_loop())
    assert connection.close.await_count == 1

    connection.close_called = False
    connection.is_closed = False
    await channel.close_callbacks(RuntimeError("second death"))
    await _drain_loop(asyncio.get_running_loop())

    assert connection.close.await_count == 2


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


# ---------------------------------------------------------------------------
# Channel factory escalation setting override
# ---------------------------------------------------------------------------
