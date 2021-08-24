import asyncio

from aiohttp import ClientSession
from pyperf import Runner

from aiodynamo.client import Client, URL
from aiodynamo.credentials import Credentials
from aiodynamo.http.aiohttp import AIOHTTP
from aiodynamo.expressions import HashKey
from utils import TABLE_NAME, KEY_FIELD, KEY_VALUE, REGION_NAME, ENDPOINT_URL


async def inner():
    async with ClientSession() as session:
        client = Client(
            AIOHTTP(session),
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


def query_aiodynamo_aiohttp():
    asyncio.run(inner())


Runner().bench_func("query", query_aiodynamo_aiohttp)
