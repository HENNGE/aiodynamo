import os
import uuid

import httpx
import pytest
from aiobotocore import get_session
from aiodynamo.client import Client, TimeToLiveStatus
from aiodynamo.errors import EmptyItem, ItemNotFound, TableNotFound
from aiodynamo.fast.client import FastClient
from aiodynamo.fast.credentials import Credentials
from aiodynamo.fast.errors import UnknownOperation
from aiodynamo.fast.http.aiohttp import AIOHTTP
from aiodynamo.fast.http.httpx import HTTPX
from aiodynamo.models import (
    F,
    KeySchema,
    KeySpec,
    KeyType,
    ReturnValues,
    TableStatus,
    Throughput,
)
from aiodynamo.types import TableName
from aiodynamo.utils import unroll
from aiohttp import ClientSession
from boto3.dynamodb import conditions
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from yarl import URL

pytestmark = pytest.mark.asyncio


def _get_tables(core):
    return unroll(
        core.list_tables,
        "ExclusiveStartTableName",
        "LastEvaluatedTableName",
        "TableNames",
    )


async def _get_tables_list(core):
    return [table async for table in _get_tables(core)]


async def _cleanup(core, tables):
    async for table in _get_tables(core):
        if table not in tables:
            await core.delete_table(TableName=table)


@pytest.fixture
def endpoint():
    if "DYNAMODB_URL" not in os.environ:
        raise pytest.skip("DYNAMODB_URL not defined in environment")
    return os.environ["DYNAMODB_URL"]


@pytest.fixture
def region():
    return os.environ.get("DYNAMODB_REGION", "us-east-1")


@pytest.fixture
async def core(endpoint, region):

    core = get_session().create_client(
        "dynamodb", endpoint_url=endpoint, use_ssl=False, region_name=region,
    )
    tables = await _get_tables_list(core)
    try:
        yield core

    finally:
        await _cleanup(core, tables)
        await core.close()


@pytest.fixture(params=["fast-aiohttp", "fast-httpx", "boto"])
async def client(request, core, endpoint, region):
    if request.param == "boto":
        yield Client(core)
    elif request.param == "fast-aiohttp":
        async with ClientSession() as session:
            http = AIOHTTP(session)
            yield FastClient(http, Credentials.auto(), region, URL(endpoint))
    elif request.param == "fast-httpx":
        async with httpx.AsyncClient() as http_client:
            http = HTTPX(http_client)
            yield FastClient(http, Credentials.auto(), region, URL(endpoint))


@pytest.fixture
async def table(client: Client):
    name = str(uuid.uuid4())
    await client.create_table(
        name,
        Throughput(5, 5),
        KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string)),
    )
    return name


async def test_put_get_item(client: Client, table: TableName):
    item = {
        "h": "hash key value",
        "r": "range key value",
        "string-key": "this is a string",
        "number-key": 123,
        "list-key": ["hello", "world"],
        "nested": {"nested": "key"},
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
    assert await client.count(table, conditions.Key("h").eq("h1")) == 0
    assert await client.count(table, conditions.Key("h").eq("h2")) == 0
    await client.put_item(table, {"h": "h1", "r": "r1"})
    assert await client.count(table, conditions.Key("h").eq("h1")) == 1
    assert await client.count(table, conditions.Key("h").eq("h2")) == 0
    await client.put_item(table, {"h": "h2", "r": "r2"})
    assert await client.count(table, conditions.Key("h").eq("h1")) == 1
    assert await client.count(table, conditions.Key("h").eq("h2")) == 1
    await client.put_item(table, {"h": "h2", "r": "r1"})
    assert await client.count(table, conditions.Key("h").eq("h2")) == 2
    assert await client.count(table, conditions.Key("h").eq("h1")) == 1


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


async def test_delete_table(client: Client, table: TableName):
    await client.delete_table(table)
    with pytest.raises(Exception):
        await client.put_item(table, {"h": "h", "r": "r"})


async def test_query(client: Client, table: TableName):
    item1 = {"h": "h", "r": "1", "d": "x"}
    item2 = {"h": "h", "r": "2", "d": "y"}
    items = [item1, item2]
    await client.put_item(table, item1)
    await client.put_item(table, item2)
    index = 0
    async for item in client.query(table, Key("h").eq("h")):
        assert item == items[index]
        index += 1


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


async def test_exists(client: Client):
    name = str(uuid.uuid4())
    assert await client.table_exists(name) == False
    with pytest.raises(TableNotFound):
        await client.describe_table(name)
    throughput = Throughput(5, 5)
    key_schema = KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string))
    attrs = {"h": KeyType.string, "r": KeyType.string}
    await client.create_table(name, throughput, key_schema)
    assert await client.table_exists(name)
    desc = await client.describe_table(name)
    assert desc.throughput == throughput
    assert desc.status is TableStatus.active
    assert desc.attributes == attrs
    assert desc.key_schema == key_schema
    assert desc.item_count == 0


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
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") == "UnknownOperationException":
            raise pytest.skip("TTL not supported by database")
        raise
    assert desc.status == TimeToLiveStatus.disabled
    assert desc.attribute == None
    await client.enable_time_to_live(table, "ttl")
    enabled_desc = await client.describe_time_to_live(table)
    assert enabled_desc.status == TimeToLiveStatus.enabled
    assert enabled_desc.attribute == "ttl"
    await client.disable_time_to_live(table, "ttl")
    disabled_desc = await client.describe_time_to_live(table)
    assert disabled_desc.status == TimeToLiveStatus.disabled
    assert desc.attribute == None
