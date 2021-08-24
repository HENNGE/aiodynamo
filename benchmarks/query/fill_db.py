"""Fill one shard in the table with hairy test data"""
import asyncio

from aiohttp import ClientSession
from pyperf import Runner

from aiodynamo.client import Client, URL
from aiodynamo.credentials import Credentials
from aiodynamo.http.aiohttp import AIOHTTP
from utils import TABLE_NAME, KEY_FIELD, KEY_VALUE, REGION_NAME, ENDPOINT_URL, RANGE_KEY_NAME


DUMP = {f"field-%i": f"value-%i" for i in range(100)}

async def inner():
    assert RANGE_KEY_NAME, "Please provide `BENCH_RANGE_KEY_NAME` environment variable"
    async with ClientSession() as session:
        client = Client(
            AIOHTTP(session),
            Credentials.auto(),
            region=REGION_NAME,
            endpoint=URL(ENDPOINT_URL) if ENDPOINT_URL else None,
        )
        for i in range(1000):
            await client.put_item(TABLE_NAME, {KEY_FIELD: KEY_VALUE, "quux": f"sample-{i}", **DUMP})


if __name__ == "__main__":
    import asyncio
    asyncio.run(inner())
