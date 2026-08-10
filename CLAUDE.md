# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Fork

This checkout is **our fork**, maintained for internal use. Changes land here
on `master` — do **not** open pull requests against `mosquito/aio-pika`
upstream, and do not suggest doing so. Pull upstream changes in as merges when
needed; keep local divergence documented in `CHANGELOG.md`.

## Commands

The project uses `uv` (build backend `uv_build`, dev deps in `[dependency-groups]`).

```shell
uv sync                          # install dev environment
uv run pytest tests              # full test suite (needs Docker, see below)
uv run pytest tests/test_amqp.py::test_simple_publish_and_receive -vv
uv run pytest -k "robust" -vv
uv run ruff check                # lint (line-length 80, rules E/W/F/C90)
uv run ruff format --check       # formatting gate used by CI
uv run mypy                      # files list is pinned in pyproject.toml
uv run make -C docs html         # sphinx docs -> docs/build/html
uv run nox -s docs -- serve      # live-reloading docs
```

CI additionally passes `--doctest-modules --aiomisc-test-timeout=120`, so
docstring examples in `aio_pika/` must actually run.

## Testing notes

* **Docker is mandatory.** `tests/conftest.py::pytest_configure` aborts the run
  if Docker is unreachable. A session-scoped fixture starts
  `mosquito/aiormq-rabbitmq`, waits on an AMQP handshake probe, and tears the
  container down (plus an `atexit` fallback). `tests/docker_client.py` is a
  hand-rolled Docker HTTP client — no `docker` package dependency.
* `Makefile`'s `rabbitmq` target is only for a manually managed broker; tests
  do not use it.
* The `connection_fabric` fixture is parametrized over `connect` and
  `connect_robust`, so most tests in `test_amqp.py` run twice — once against
  each connection class.
* The autouse `memory_tracer` fixture asserts that `tracemalloc` sees **zero**
  net allocations in `aio_pika`/`aiormq`/`pamqp` after each test. Module-level
  caches, un-cleared callback registries, or leaked tasks fail unrelated tests.
* `tests/test_amqp_robust_proxy.py` drives reconnection scenarios through
  `aiomisc_pytest.TCPProxy` (`proxy.disconnect_all()`, `proxy.slowdown(...)`).
  Reconnect/restore changes belong there.
* Async tests use `aiomisc-pytest` (`-p no:asyncio` in `addopts`); there is no
  `pytest-asyncio`.

## Architecture

`aio_pika` is an object-oriented layer over `aiormq` (which sits on `pamqp`).
It owns none of the wire protocol — it manages *lifecycle, state and recovery*.

### Layering

Every public class has an abstract counterpart in [aio_pika/abc.py](aio_pika/abc.py)
(`AbstractConnection`, `AbstractChannel`, `AbstractQueue`, …). That module is
the single source of truth for the API surface and is what user code should
type-annotate against.

Two dataclasses in `abc.py` are the seam to `aiormq`:

* `UnderlayConnection` — wraps `aiormq.connect()` plus a `OneShotCallback`
  registered on `connection.closing`.
* `UnderlayChannel` — wraps `connection.channel()` and registers the same
  close callback on both the channel and its connection, removing them on
  close so nothing leaks (the `memory_tracer` fixture enforces this).

`Connection.transport` / `Channel._channel` hold these; both are `None` when
disconnected, so most methods go through an accessor that raises if closed.

### Plain vs. robust

`Connection`/`Channel`/`Queue`/`Exchange` are one-shot: once the transport dies
they stay dead. The `Robust*` subclasses add automatic recovery and are wired
together purely by class attributes — `Connection.CHANNEL_CLASS`,
`AbstractChannel.QUEUE_CLASS` / `EXCHANGE_CLASS`. Subclass these attributes
rather than adding new factory plumbing.

Recovery flow after a connection drop:

1. `RobustConnection` reconnects on an interval (`_reconnect_lock`,
   `reconnect_interval`, `fail_fast`), then calls `restore()` on every channel
   in its `WeakSet`.
2. `RobustChannel.restore()` → `reopen()` → `_on_open()`, which replays
   `set_qos`, then `restore()`s each remembered exchange, queue and consumer.
3. `RobustExchange`/`RobustQueue.restore()` re-declare and re-bind themselves.

Declarations are remembered only when `restore=True` (the default) on
`declare_queue`/`declare_exchange`; pass `restore=False` for genuinely
ephemeral entities. Each restore path is guarded by its own `asyncio.Lock` plus
an `asyncio.Event` (`__restored`) that `ready()` awaits, so callers block
during a reconnect instead of seeing a half-open channel.

### Callbacks

[aio_pika/tools.py](aio_pika/tools.py) provides the eventing primitives used
everywhere: `CallbackCollection` (weak-ref aware, freezable, callable as a
whole) backs `close_callbacks`, `return_callbacks`, `reconnect_callbacks`,
`reopen_callbacks`; `OneShotCallback` guarantees a close handler fires exactly
once; `ensure_awaitable` lets users pass sync or async callbacks
interchangeably.

### URL parameters

Connection tunables are parsed from the AMQP URL query string via
`ConnectionParameter` entries in the `PARAMETERS` tuple, which subclasses
extend (`RobustConnection.PARAMETERS = Connection.PARAMETERS + (...)`).
Add a new knob there, not as an ad-hoc kwarg, and document it in
[docs/source/url-parameters.md](docs/source/url-parameters.md).

### Higher-level pieces

* [aio_pika/pool.py](aio_pika/pool.py) — generic `Pool` over anything
  implementing `PoolInstance` (both `Connection` and `Channel` do); typically
  a connection pool feeding a channel pool.
* [aio_pika/patterns/](aio_pika/patterns/) — `Master`/`Worker` (task queue with
  `NackMessage`/`RejectMessage` control-flow exceptions) and `RPC`
  (correlation-id request/response with a DLX for expired calls). Both use the
  pickle/json serializers in `patterns/base.py` and expose a `.proxy`
  attribute for attribute-style calls.
* [aio_pika/transaction.py](aio_pika/transaction.py) — AMQP tx wrapper;
  `aio_pika/message.py` holds `Message`/`IncomingMessage` and the
  `message.process()` context manager that ack/nacks based on outcome.

## Conventions

* Line length 80 (ruff); `.editorconfig` says 79 for hand-formatted files.
* mypy runs with `disallow_untyped_defs` and friends over `aio_pika`, `tests`
  and the tutorial examples under `docs/source/rabbitmq-tutorial/examples/` —
  those examples are type-checked code, not prose.
* Public API additions must be exported from `aio_pika/__init__.py`'s
  `__all__` (`no_implicit_reexport` is on) and given an abstract base in
  `abc.py`.
* User-visible changes go in `CHANGELOG.md`; version lives in `pyproject.toml`
  and is overwritten from the release tag at publish time.

## Agent skills

### Issue tracker

Issues and specs live as local markdown under `.scratch/<feature>/` (GitHub
Issues are disabled on the fork). See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, unrenamed — `needs-triage`, `needs-info`,
`ready-for-agent`, `ready-for-human`, `wontfix`. See
`docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at the repo root, both created
lazily. See `docs/agents/domain.md`.
