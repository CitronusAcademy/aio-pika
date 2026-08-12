from typing import Type

from aiormq.connection import parse_bool, parse_int, parse_timeout
from yarl import URL

from aio_pika import connect
from aio_pika.abc import AbstractConnection
from aio_pika.connection import Connection
from aio_pika.robust_connection import RobustConnection, connect_robust


class MockConnection(Connection):
    async def connect(self, timeout=None, **kwargs):
        return self


class MockConnectionRobust(RobustConnection):
    async def connect(self, timeout=None, **kwargs):
        return self


VALUE_GENERATORS = {
    parse_int: {
        "-1": -1,
        "0": 0,
        "43": 43,
        "9999999999999999": 9999999999999999,
        "hello": 0,
    },
    parse_bool: {
        "disabled": False,
        "enable": True,
        "yes": True,
        "no": False,
        "": False,
    },
    parse_timeout: {
        "0": 0,
        "Vasyan": 0,
        "0.1": 0.1,
        "0.54": 0.54,
        "1": 1,
        "100": 100,
        "1000:": 0,
    },
    float: {
        "0": 0.0,
        "0.0": 0.0,
        ".0": 0.0,
        "0.1": 0.1,
        "1": 1.0,
        "hello": None,
    },
}


class TestCase:
    CONNECTION_CLASS: Type[AbstractConnection] = MockConnection

    async def get_instance(self, url, **kwargs) -> AbstractConnection:
        return await connect(  # type: ignore
            url,
            connection_class=self.CONNECTION_CLASS,
            **kwargs,
        )

    async def test_kwargs(self):
        instance = await self.get_instance("amqp://localhost/")

        for parameter in self.CONNECTION_CLASS.PARAMETERS:
            if parameter.is_kwarg:
                continue

            assert hasattr(instance, parameter.name)
            assert getattr(instance, parameter.name) is parameter.parse(
                parameter.default
            )

    async def test_kwargs_values(self):
        for parameter in self.CONNECTION_CLASS.PARAMETERS:
            if parameter.strict:
                continue
            positives = VALUE_GENERATORS[parameter.parser]  # type: ignore
            for example, expected in positives.items():  # type: ignore
                instance = await self.get_instance(
                    f"amqp://localhost/?{parameter.name}={example}",
                )

                assert parameter.parse(example) == expected

                if parameter.is_kwarg:
                    assert instance.kwargs[parameter.name] == expected
                else:
                    assert hasattr(instance, parameter.name)
                    assert getattr(instance, parameter.name) == expected

                    instance = await self.get_instance(
                        "amqp://localhost",
                        **{parameter.name: example},
                    )
                    assert hasattr(instance, parameter.name)
                    assert getattr(instance, parameter.name) == expected


class TestCaseRobust(TestCase):
    CONNECTION_CLASS: Type[MockConnectionRobust] = MockConnectionRobust

    async def get_instance(self, url, **kwargs) -> AbstractConnection:
        return await connect_robust(  # type: ignore
            url,
            connection_class=self.CONNECTION_CLASS,  # type: ignore
            **kwargs,
        )


def test_connection_interleave(amqp_url: URL):
    url = amqp_url.update_query(interleave="1")
    connection = Connection(url=url)
    assert "interleave" in connection.kwargs
    assert connection.kwargs["interleave"] == 1

    connection = Connection(url=amqp_url)
    assert "interleave" not in connection.kwargs


def test_connection_happy_eyeballs_delay(amqp_url: URL):
    url = amqp_url.update_query(happy_eyeballs_delay=".1")
    connection = Connection(url=url)
    assert "happy_eyeballs_delay" in connection.kwargs
    assert connection.kwargs["happy_eyeballs_delay"] == 0.1

    connection = Connection(url=amqp_url)
    assert "happy_eyeballs_delay" not in connection.kwargs


def test_robust_connection_interleave(amqp_url: URL):
    url = amqp_url.update_query(interleave="1")
    connection = RobustConnection(url=url)
    assert "interleave" in connection.kwargs
    assert connection.kwargs["interleave"] == 1

    connection = RobustConnection(url=amqp_url)
    assert "interleave" not in connection.kwargs


def test_robust_connection_happy_eyeballs_delay(amqp_url: URL):
    url = amqp_url.update_query(happy_eyeballs_delay=".1")
    connection = RobustConnection(url=url)
    assert "happy_eyeballs_delay" in connection.kwargs
    assert connection.kwargs["happy_eyeballs_delay"] == 0.1

    connection = RobustConnection(url=amqp_url)
    assert "happy_eyeballs_delay" not in connection.kwargs


# ---------------------------------------------------------------------------
# Channel escalation connection settings
# ---------------------------------------------------------------------------


def test_channel_escalation_defaults():
    """Default settings when no params are provided."""
    connection = Connection(URL("amqp://guest:guest@localhost/"))
    assert connection.channel_escalation is True
    assert connection.channel_escalation_timeout == 5.0


def test_channel_escalation_url_overrides():
    """URL query parameters override defaults."""
    connection = Connection(
        URL(
            "amqp://guest:guest@localhost/?channel_escalation=0"
            "&channel_escalation_timeout=2.5"
        ),
    )
    assert connection.channel_escalation is False
    assert connection.channel_escalation_timeout == 2.5


def test_channel_escalation_kwargs():
    """Keyword arguments override both defaults and URL params."""
    connection = Connection(
        URL("amqp://guest:guest@localhost/"),
        channel_escalation=False,
        channel_escalation_timeout=1.25,
    )
    assert connection.channel_escalation is False
    assert connection.channel_escalation_timeout == 1.25


def test_robust_channel_escalation_defaults():
    """Robust connection defaults match Connection defaults."""
    connection = RobustConnection(URL("amqp://guest:guest@localhost/"))
    assert connection.channel_escalation is True
    assert connection.channel_escalation_timeout == 5.0


def test_robust_channel_escalation_url_overrides():
    """Robust connection URL overrides work."""
    url = URL(
        "amqp://guest:guest@localhost/?channel_escalation=0"
        "&channel_escalation_timeout=2.5",
    )
    connection = RobustConnection(url=url)
    assert connection.channel_escalation is False
    assert connection.channel_escalation_timeout == 2.5


def test_channel_escalation_not_in_kwargs():
    """Channel escalation params are not passed to aiormq."""
    connection = Connection(
        URL("amqp://guest:guest@localhost/"),
        channel_escalation=False,
        channel_escalation_timeout=1.25,
    )
    assert "channel_escalation" not in connection.kwargs
    assert "channel_escalation_timeout" not in connection.kwargs
