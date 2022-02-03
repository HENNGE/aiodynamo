import json
from contextlib import asynccontextmanager
from typing import cast

import pytest
from aiohttp import ClientConnectionError, ClientSession

from aiodynamo.client import Client
from aiodynamo.credentials import Key, StaticCredentials
from aiodynamo.errors import ValidationException
from aiodynamo.http.aiohttp import AIOHTTP
from aiodynamo.models import StaticDelayRetry

creds = StaticCredentials(Key("a", "b"))


async def test_retry_raises_underlying_error_aiohttp():
    class TestSession:
        @asynccontextmanager
        async def request(self, *args, **kwargs):
            raise ClientConnectionError()
            yield  # needed for asynccontextmanager

    http = AIOHTTP(cast(ClientSession, TestSession()))
    client = Client(
        http, creds, "test", throttle_config=StaticDelayRetry(time_limit_secs=-1)
    )
    with pytest.raises(ClientConnectionError):
        await client.get_item("test", {"a": "b"})


async def test_dynamo_errors_get_raised_depaginated():
    class TestResponse:
        status = 400

        async def read(self):
            return json.dumps(
                {
                    "__type": "com.amazonaws.dynamodb.v20120810#ValidationException",
                    "message": "test",
                }
            ).encode()

    class TestSession:
        @asynccontextmanager
        async def request(self, *args, **kwargs):
            yield TestResponse()

    http = AIOHTTP(cast(ClientSession, TestSession()))
    client = Client(
        http, creds, "test", throttle_config=StaticDelayRetry(time_limit_secs=-1)
    )
    with pytest.raises(ValidationException):
        async for _ in client.scan("test"):
            pass
