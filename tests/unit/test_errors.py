import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, cast

import pytest
from aiohttp import ClientConnectionError, ClientSession

from aiodynamo.client import Client
from aiodynamo.credentials import Key, StaticCredentials
from aiodynamo.errors import ValidationException
from aiodynamo.http.aiohttp import AIOHTTP
from aiodynamo.http.types import HttpImplementation, Request, Response
from aiodynamo.models import StaticDelayRetry

creds = StaticCredentials(Key("a", "b"))


async def test_retry_raises_underlying_error_aiohttp() -> None:
    class TestSession:
        @asynccontextmanager
        async def request(self, *args: Any, **kwargs: Any) -> AsyncIterator[None]:
            raise ClientConnectionError()
            yield  # needed for asynccontextmanager

    http = AIOHTTP(cast(ClientSession, TestSession()))
    client = Client(
        http, creds, "test", throttle_config=StaticDelayRetry(time_limit_secs=-1)
    )
    with pytest.raises(ClientConnectionError):
        await client.get_item("test", {"a": "b"})


async def test_dynamo_errors_get_raised_depaginated() -> None:
    class TestResponse:
        status = 400

        async def read(self) -> bytes:
            return json.dumps(
                {
                    "__type": "com.amazonaws.dynamodb.v20120810#ValidationException",
                    "message": "test",
                }
            ).encode()

    class TestSession:
        @asynccontextmanager
        async def request(
            self, *args: Any, **kwargs: Any
        ) -> AsyncIterator[TestResponse]:
            yield TestResponse()

    http = AIOHTTP(cast(ClientSession, TestSession()))
    client = Client(
        http, creds, "test", throttle_config=StaticDelayRetry(time_limit_secs=-1)
    )
    with pytest.raises(ValidationException):
        async for _ in client.scan("test"):
            pass


@pytest.mark.parametrize("status", [500, 503])
async def test_dynamo_retries_50x(status: int) -> None:
    responses = iter(
        [
            Response(status=status, body=b""),
            Response(
                status=200, body=json.dumps({"Item": {"fake": {"S": "key"}}}).encode()
            ),
        ]
    )

    async def http(request: Request) -> Response:
        return next(responses)

    client = Client(
        http,
        creds,
        "test",
        throttle_config=StaticDelayRetry(delay=0.01),
    )
    item = await client.get_item("table", {"fake": "key"})
    assert item == {"fake": "key"}
