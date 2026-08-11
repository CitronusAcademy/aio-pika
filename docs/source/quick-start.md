# Quick start

Some useful examples.

## Simple consumer

Connect to RabbitMQ, declare a queue, and consume messages using an async
iterator. Each message is automatically acknowledged when the `process()`
context manager exits successfully.

```{literalinclude} examples/simple_consumer.py
:language: python
```

## Simple publisher

Connect to RabbitMQ and publish a single message to a queue through the
default exchange.

```{literalinclude} examples/simple_publisher.py
:language: python
```

## Escalating independent channel failures

Unexpected independent channel failures close the owning connection by default.
This fork's policy is automatic: a failure on one channel affects every channel
sharing that connection. With a robust connection, the connection then follows
its normal reconnect and channel-restoration flow.

```python
# Unexpected independent channel failures close the owning connection by default.
connection = await aio_pika.connect_robust(
    url,
    channel_escalation=True,
    channel_escalation_timeout=5.0,
)

# Deliberately preserve isolated/legacy behavior for this channel.
legacy_channel = connection.channel(channel_escalation=False)
```

`channel_escalation` defaults to `True`. Pass `False` when creating an
individual channel to opt that channel out while leaving the connection-wide
policy enabled for other channels. Explicit channel or connection shutdown is
not an independent failure and does not trigger escalation.

`channel_escalation_timeout` is the maximum time escalation waits for the
owning connection's close handling. It bounds escalation waiting; it does not
forcibly complete every underlying transport close. Cleanup and robust
recovery may therefore continue after the timeout. This is an intentional
Citronus fork policy and is not the upstream aio-pika default.

## Asynchronous message processing

Consume messages using a callback function instead of an async iterator.
This allows multiple messages to be processed concurrently, controlled by
the `prefetch_count` setting.

```{literalinclude} examples/simple_async_consumer.py
:language: python
```


## Working with RabbitMQ transactions

Publish messages atomically using AMQP transactions. Messages are only
delivered to the queue after the transaction is committed. Shows both
the context manager approach and manual `select`/`commit`/`rollback` usage.

```{literalinclude} examples/simple_publisher_transactions.py
:language: python
```

## Get single message example

Fetch a single message from a queue using `queue.get()` instead of
continuous consumption. This is useful for polling or one-off retrieval,
with manual acknowledgement of the received message.

```{literalinclude} examples/main.py
:language: python
```

## Set logging level

Sometimes you want to see only your debug logs, but when you just call
`logging.basicConfig(logging.DEBUG)` you set the debug log level for all
loggers, includes all aio_pika's modules. If you want to set logging level
independently see following example:

```{literalinclude} examples/log-level-set.py
:language: python
```

## External credentials example

Connect to RabbitMQ using TLS client certificates (x509) for
authentication instead of username/password. Requires CA certificate,
client certificate, and private key files.

```{literalinclude} examples/external-credentials.py
:language: python
```

(connection-pooling)=

## Connection pooling

A single AMQP connection multiplexes multiple channels over one TCP
socket, which is sufficient for most applications. However, a single
connection has a finite throughput limited by its TCP link and the
serialization of frames. Connection pooling may help when:

- You are publishing or consuming a very high volume of messages and
  a single TCP connection becomes saturated.
- You want to isolate groups of channels so that a blocked or slow
  connection does not affect other workloads.
- Your application runs many concurrent tasks that would contend for
  the same connection's write lock.

In most cases you **do not need** connection pooling — start with a
single `connect_robust()` connection and only add pooling after
profiling shows that the connection is a bottleneck.

Use `aio_pika.pool.Pool` to manage a pool of connections and channels.

```{literalinclude} examples/pooling.py
:language: python
```

## FastAPI example

Integrate aio-pika with a FastAPI application. The connection is
established during startup via the lifespan context manager and stored
in `app.state` for use across request handlers.

:::{note}
A single robust connection is sufficient for most use cases. AMQP
multiplexes work over channels within one connection. Consider
[connection pooling](#connection-pooling) only if you have measured
that a single connection is a bottleneck.
:::

```{literalinclude} examples/fastapi-pubsub.py
:language: python
```

## Starlette example

Integrate aio-pika with a Starlette application. The connection is
managed through startup/shutdown event handlers and stored in
`app.state` for use across request handlers.

:::{note}
A single robust connection is sufficient for most use cases. AMQP
multiplexes work over channels within one connection. Consider
[connection pooling](#connection-pooling) only if you have measured
that a single connection is a bottleneck.
:::

```{literalinclude} examples/starlette-pubsub.py
:language: python
```

## aiohttp example

Integrate aio-pika with an aiohttp web application. The connection is
created on application startup and stored in the `app` dict for access
from request handlers.

:::{note}
A single robust connection is sufficient for most use cases. AMQP
multiplexes work over channels within one connection. Consider
[connection pooling](#connection-pooling) only if you have measured
that a single connection is a bottleneck.
:::

```{literalinclude} examples/aiohttp-pubsub.py
:language: python
```

## Tornado example

Integrate aio-pika with a Tornado web application. The publisher handler
sends messages on POST requests, while the subscriber handler waits for
incoming messages and returns them as HTTP responses.

:::{note}
A single robust connection is sufficient for most use cases. AMQP
multiplexes work over channels within one connection. Consider
[connection pooling](#connection-pooling) only if you have measured
that a single connection is a bottleneck.
:::

```{literalinclude} examples/tornado-pubsub.py
:language: python
```
