from aiobotocore import get_session
from boto3.dynamodb.conditions import Attr

from aiodynamo.client import Client
from aiodynamo.models import Throughput, KeySchema, KeySpec, KeyType, F


async def example():
    aiobotocore_client = get_session().create_client("dynamodb")

    client = Client(aiobotocore_client)

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
    # Update an item, if it exists.
    await table.update_item(
        {"key": "my-item"}, F("value").add(1), condition=Attr("key").exists()
    )
