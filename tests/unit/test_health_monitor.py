from itertools import cycle
from typing import Iterable

import pytest

from aiodynamo.client import Client
from aiodynamo.credentials import Key, StaticCredentials
from aiodynamo.health import (
    CallbackHealthMonitor,
    CallStrategy,
    CountingHealthMonitor,
    OnSuccess,
)
from aiodynamo.http.types import HttpImplementation, Request, Response
from aiodynamo.models import RetryConfig
from aiodynamo.types import Seconds


def http(responses: Iterable[Response | Exception]) -> HttpImplementation:
    it = iter(responses)

    async def impl(_request: Request) -> Response:
        response = next(it)
        if isinstance(response, Exception):
            raise response
        return response

    return impl


class Once(RetryConfig):
    def delays(self) -> Iterable[Seconds]:
        yield 0


@pytest.mark.parametrize(
    "strategy,first,second",
    [
        (OnSuccess.noop, False, False),
        (OnSuccess.decrement, False, True),
        (OnSuccess.reset, True, True),
    ],
)
async def test_counting_health_monitor(
    strategy: OnSuccess, first: bool, second: bool
) -> None:
    class Error(Exception):
        pass

    monitor = CountingHealthMonitor(max_failures=2, on_success_action=strategy)
    client = Client(
        http(cycle([Error()])),
        credentials=StaticCredentials(Key("a", "b")),
        region="test",
        throttle_config=Once(),
        health_monitor=monitor,
    )
    assert monitor.is_healthy()
    with pytest.raises(Error):
        await client.describe_table("table")
    assert monitor.is_healthy()
    with pytest.raises(Error):
        await client.describe_table("table")
    assert not monitor.is_healthy()
    with pytest.raises(Error):
        await client.describe_table("table")
    monitor.on_success()
    assert monitor.is_healthy() is first
    monitor.on_success()
    assert monitor.is_healthy() is second


@pytest.mark.parametrize(
    "strategy,expected",
    [
        (CallStrategy.edge, 2),
        (CallStrategy.always, 3),
        (CallStrategy.once, 1),
    ],
)
async def test_callback_health_monitor(strategy: CallStrategy, expected: int) -> None:
    calls = 0

    def callback() -> None:
        nonlocal calls
        calls += 1

    monitor = CallbackHealthMonitor(
        inner=CountingHealthMonitor(max_failures=1, on_success_action=OnSuccess.reset),
        callback=callback,
        call_strategy=strategy,
    )
    error = Exception()
    monitor.on_exception(error)
    assert calls == 1
    monitor.on_exception(error)
    monitor.on_success()
    monitor.on_exception(error)
    assert calls == expected
