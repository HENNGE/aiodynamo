import asyncio
from contextlib import asynccontextmanager

import pytest
from aiodynamo.client import Client
from aiodynamo.credentials import Key, StaticCredentials
from aiodynamo.http.aiohttp import AIOHTTP
from aiodynamo.http.base import RequestFailed
from aiodynamo.models import ExponentialBackoffThrottling
from yarl import URL

pytestmark = [pytest.mark.asyncio]


async def test_aiohttp_request_failed():
    exc = asyncio.TimeoutError()

    class FakeSession:
        @asynccontextmanager
        async def request(self, *args, **kwargs):
            raise exc
            yield

    http = AIOHTTP(FakeSession())

    with pytest.raises(RequestFailed) as error:
        await http.get(url=URL("http://foo.invalid.com"), timeout=100)
    assert error.value.__cause__ is exc

    client = Client(
        http=http,
        credentials=StaticCredentials(Key("invalid", "invalid")),
        region="invalid",
        throttle_config=ExponentialBackoffThrottling(0),
    )

    with pytest.raises(RequestFailed) as error:
        await client.get_item("table", {"key": "value"})

    assert error.value.__cause__ is exc
