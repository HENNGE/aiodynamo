import asyncio
import uuid

import pytest
from aiodynamo.client import Client, TimeToLiveStatus
from aiodynamo.errors import EmptyItem, ItemNotFound, TableNotFound, UnknownOperation
from aiodynamo.expressions import F, HashKey, RangeKey
from aiodynamo.models import (
    KeySchema,
    KeySpec,
    KeyType,
    ReturnValues,
    TableStatus,
    Throughput,
    WaitConfig,
)
from aiodynamo.types import TableName

pytestmark = pytest.mark.asyncio


async def test_put_get_item(client: Client, table: TableName):
    item = {
        "h": "hash key value",
        "r": "range key value",
        "string-key": "this is a string",
        "number-key": 123,
        "list-key": ["hello", "world"],
        "nested": {"nested": "key"},
        "binary": b"hello world",
        "string-set": {"string", "set"},
        "number-set": {1, 2},
        "binary-set": {b"hello", b"world"},
        "map": {"foo": "bar"},
    }
    await client.put_item(table, item)
    db_item = await client.get_item(
        table, {"h": "hash key value", "r": "range key value"}
    )
    assert item == db_item


async def test_get_item_with_projection(client: Client, table: TableName):
    item = {
        "h": "hkv",
        "r": "rkv",
        "string-key": "this is a string",
        "number-key": 123,
        "list-key": ["hello", "world"],
        "nested": {"nested": "key"},
    }
    await client.put_item(table, item)
    db_item = await client.get_item(
        table, {"h": "hkv", "r": "rkv"}, projection=F("string-key") & F("list-key", 1)
    )
    assert db_item == {"string-key": "this is a string", "list-key": ["world"]}
    db_item = await client.get_item(
        table, {"h": "hkv", "r": "rkv"}, projection=F("string-key")
    )
    assert db_item == {"string-key": "this is a string"}


async def test_count(client: Client, table: TableName):
    assert await client.count(table, HashKey("h", "h1")) == 0
    assert await client.count(table, HashKey("h", "h2")) == 0
    await client.put_item(table, {"h": "h1", "r": "r1"})
    assert await client.count(table, HashKey("h", "h1")) == 1
    assert await client.count(table, HashKey("h", "h2")) == 0
    await client.put_item(table, {"h": "h2", "r": "r2"})
    assert await client.count(table, HashKey("h", "h1")) == 1
    assert await client.count(table, HashKey("h", "h2")) == 1
    await client.put_item(table, {"h": "h2", "r": "r1"})
    assert await client.count(table, HashKey("h", "h2")) == 2
    assert await client.count(table, HashKey("h", "h1")) == 1
    assert (
        await client.count(table, HashKey("h", "h1") & RangeKey("r").begins_with("x"))
        == 0
    )


async def test_update_item(client: Client, table: TableName):
    item = {
        "h": "hkv",
        "r": "rkv",
        "string-key": "this is a string",
        "number-key": 123,
        "list-key": ["hello", "world"],
        "set-key-one": {"hello", "world"},
        "set-key-two": {"hello", "world"},
        "dead-key": "foo",
    }
    await client.put_item(table, item)
    ue = (
        F("string-key").set("new value")
        & F("number-key").change(-12)
        & F("list-key").append(["!"])
        & F("set-key-one").add({"hoge"})
        & F("dead-key").remove()
        & F("set-key-two").delete({"hello"})
    )
    resp = await client.update_item(
        table, {"h": "hkv", "r": "rkv"}, ue, return_values=ReturnValues.all_new
    )
    assert resp == {
        "h": "hkv",
        "r": "rkv",
        "string-key": "new value",
        "number-key": 111,
        "list-key": ["hello", "world", "!"],
        "set-key-one": {"hello", "world", "hoge"},
        "set-key-two": {"world"},
    }


async def test_delete_item(client: Client, table: TableName):
    item = {"h": "h", "r": "r"}
    await client.put_item(table, item)
    assert await client.get_item(table, item) == item
    assert (
        await client.delete_item(table, item, return_values=ReturnValues.all_old)
        == item
    )
    with pytest.raises(ItemNotFound):
        await client.get_item(table, item)


async def test_delete_table(client: Client, table_factory):
    name = await table_factory()
    # no error
    await client.put_item(name, {"h": "h", "r": "r"})
    await client.delete_table(
        name, wait_for_disabled=WaitConfig(max_attempts=25, retry_delay=5)
    )
    with pytest.raises(Exception):
        await client.put_item(name, {"h": "h", "r": "r"})


async def test_query(client: Client, table: TableName):
    item1 = {"h": "h", "r": "1", "d": "x"}
    item2 = {"h": "h", "r": "2", "d": "y"}
    items = [item1, item2]
    await client.put_item(table, item1)
    await client.put_item(table, item2)
    index = 0
    async for item in client.query(table, HashKey("h", "h")):
        assert item == items[index]
        index += 1
    assert index == 2


async def test_query_descending(client: Client, table: TableName):
    item1 = {"h": "h", "r": "1", "d": "x"}
    item2 = {"h": "h", "r": "2", "d": "y"}
    items = [item1, item2]
    await client.put_item(table, item1)
    await client.put_item(table, item2)
    rv = [
        item
        async for item in client.query(table, HashKey("h", "h"), scan_forward=False)
    ]
    assert rv == list(reversed(items))


async def test_scan(client: Client, table: TableName):
    item1 = {"h": "h", "r": "1", "d": "x"}
    item2 = {"h": "h", "r": "2", "d": "y"}
    items = [item1, item2]
    await client.put_item(table, item1)
    await client.put_item(table, item2)
    index = 0
    async for item in client.scan(table):
        assert item == items[index]
        index += 1
    assert index == 2


async def test_exists(client: Client, table_factory):
    throughput = Throughput(5, 5)
    key_schema = KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string))
    attrs = {"h": KeyType.string, "r": KeyType.string}
    name = await table_factory()
    try:
        assert await client.table_exists(name)
        desc = await client.describe_table(name)
        assert desc.throughput == throughput
        assert desc.status is TableStatus.active
        assert desc.attributes == attrs
        assert desc.key_schema == key_schema
        assert desc.item_count == 0
    finally:
        await client.delete_table(name)
    assert await client.table_exists(name) == False
    with pytest.raises(TableNotFound):
        await client.describe_table(name)


async def test_empty_string(client: Client, table: TableName):
    key = {"h": "h", "r": "r"}
    await client.put_item(table, {**key, "s": ""})
    assert await client.get_item(table, key) == {"h": "h", "r": "r"}
    assert await client.update_item(
        table,
        key,
        F("foo").set("") & F("bar").set("baz"),
        return_values=ReturnValues.all_new,
    ) == {"h": "h", "r": "r", "bar": "baz"}


async def test_empty_item(client: Client, table: TableName):
    with pytest.raises(EmptyItem):
        await client.put_item(table, {"h": "", "r": ""})


async def test_empty_list(client: Client, table: TableName):
    key = {"h": "h", "r": "r"}
    await client.put_item(table, {**key, "l": [1]})
    await client.update_item(table, key, F("l").set([]))
    assert await client.get_item(table, key) == {"h": "h", "r": "r", "l": []}


async def test_ttl(client: Client, table: TableName):
    try:
        desc = await client.describe_time_to_live(table)
    except UnknownOperation:
        raise pytest.skip("TTL not supported by database")
    assert desc.status == TimeToLiveStatus.disabled
    assert desc.attribute == None
    try:
        await client.enable_time_to_live(table, "ttl")
    except UnknownOperation:
        raise pytest.skip("TTL not supported by database")
    enabled_desc = await client.describe_time_to_live(table)
    assert enabled_desc.status == TimeToLiveStatus.enabled
    assert enabled_desc.attribute == "ttl"
    # cannot disable TTL and test that since TTL changes can take up to one
    # hour to complete and other calls to UpdateTimeToLive are not allowed.
    # See: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTimeToLive.html


async def test_query_with_limit(client: Client, high_throughput_table: TableName):
    big = "x" * 20_000

    await asyncio.gather(
        *(
            client.put_item(high_throughput_table, {"h": "h", "r": str(i), "big": big})
            for i in range(100)
        )
    )

    items = [
        item
        async for item in client.query(
            high_throughput_table, HashKey("h", "h"), limit=1
        )
    ]
    assert len(items) == 1
    assert items[0]["r"] == "0"


async def test_scan_with_limit(client: Client, table: TableName):
    item1 = {"h": "h", "r": "1", "d": "x"}
    item2 = {"h": "h", "r": "2", "d": "y"}
    await client.put_item(table, item1)
    await client.put_item(table, item2)
    items = [item async for item in client.scan(table, limit=1)]
    assert len(items) == 1
    assert items[0] == item1


async def test_update_item_with_broken_update_expression(
    client: Client, table: TableName
):
    item = {"h": "h", "r": "r", "f": 1}
    await client.put_item(table, item)
    await client.update_item(table, {"h": "h", "r": "r"}, F("f").set(2) & F("f").set(3))
    item = await client.get_item(table, item)
    assert item == {"h": "h", "r": "r", "f": 3}
