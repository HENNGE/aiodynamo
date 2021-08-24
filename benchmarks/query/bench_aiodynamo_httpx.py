import asyncio

from httpx import AsyncClient
from pyperf import Runner

from aiodynamo.client import Client, URL
from aiodynamo.credentials import Credentials
from aiodynamo.http.httpx import HTTPX
from aiodynamo.expressions import HashKey
from utils import TABLE_NAME, KEY_FIELD, KEY_VALUE, REGION_NAME, ENDPOINT_URL


async def inner():
    async with AsyncClient() as http_client:
        client = Client(
            HTTPX(http_client),
            Credentials.auto(),
            region=REGION_NAME,
            endpoint=URL(ENDPOINT_URL) if ENDPOINT_URL else None,
        )
        items = [
            item
            async for item in client.query(
                TABLE_NAME,
                key_condition=HashKey(KEY_FIELD, KEY_VALUE),
            )
        ]


def query_aiodynamo_httpx():
    asyncio.run(inner())


Runner().bench_func("query", query_aiodynamo_httpx)
