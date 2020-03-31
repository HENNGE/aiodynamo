from aiohttp import ClientSession

from aiodynamo.client import Client
from aiodynamo.credentials import Credentials
from aiodynamo.expressions import F
from aiodynamo.http.aiohttp import AIOHTTP
from aiodynamo.models import Throughput, KeySchema, KeySpec, KeyType


async def example():
    async with ClientSession() as session:
        client = Client(AIOHTTP(session), Credentials.auto(), "us-east-1")

        table = client.table("my-table")

        # Create table if it doesn't exist
        if not await table.exists():
            await table.create(
                Throughput(read=10, write=10),
                KeySchema(hash_key=KeySpec("key", KeyType.string)),
            )

        # Create or override an item
        await table.put_item({"key": "my-item", "value": 1})
        # Get an item
        item = await table.get_item({"key": "my-item"})
        print(item)
        # Update an item, if it exists.
        await table.update_item(
            {"key": "my-item"}, F("value").add(1), condition=F("key").exists()
        )
