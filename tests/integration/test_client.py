import asyncio
import secrets
from operator import itemgetter
from typing import List, Type

import pytest
from yarl import URL

from aiodynamo import errors
from aiodynamo.client import Client
from aiodynamo.credentials import ChainCredentials
from aiodynamo.errors import (
    ItemNotFound,
    NoCredentialsFound,
    TableNotFound,
    TooManyTransactions,
    TransactionEmpty,
    UnknownOperation,
    ValidationException,
)
from aiodynamo.expressions import Condition, F, HashKey, RangeKey
from aiodynamo.http.types import HttpImplementation
from aiodynamo.models import (
    BatchGetRequest,
    BatchWriteRequest,
    ExponentialBackoffRetry,
    GlobalSecondaryIndex,
    KeySchema,
    KeySpec,
    KeyType,
    LocalSecondaryIndex,
    Projection,
    ProjectionType,
    RetryConfig,
    ReturnValues,
    TableStatus,
    Throughput,
    TimeToLiveStatus,
)
from aiodynamo.operations import ConditionCheck, Delete, Get, Put, Update
from aiodynamo.types import TableName
from tests.integration.conftest import TableFactory


async def test_create_table_with_indices(
    client: Client, table_name_prefix: str, wait_config: RetryConfig
) -> None:
    name = table_name_prefix + secrets.token_hex(4)
    await client.create_table(
        name,
        Throughput(5, 5),
        KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string)),
        gsis=[
            GlobalSecondaryIndex(
                name="global",
                schema=KeySchema(
                    KeySpec("h", KeyType.string), KeySpec("g", KeyType.string)
                ),
                projection=Projection(ProjectionType.all),
                throughput=Throughput(5, 5),
            )
        ],
        lsis=[
            LocalSecondaryIndex(
                name="local",
                schema=KeySchema(
                    KeySpec("h", KeyType.string), KeySpec("l", KeyType.string)
                ),
                projection=Projection(ProjectionType.all),
            )
        ],
        wait_for_active=wait_config,
    )


async def test_put_get_item(
    client: Client, table: TableName, consistent_read: bool
) -> None:
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
        table,
        {"h": "hash key value", "r": "range key value"},
        consistent_read=consistent_read,
    )

    assert item == db_item


async def test_get_item_with_projection(client: Client, table: TableName) -> None:
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


async def test_count(client: Client, table: TableName) -> None:
    assert await client.count(table, HashKey("h", "h1")) == 0
    assert await client.count(table, HashKey("h", "h2")) == 0
    assert await client.count(table, HashKey("h", "h1"), limit=1) == 0
    assert await client.count(table, HashKey("h", "h2"), limit=1) == 0
    await client.put_item(table, {"h": "h1", "r": "r1"})
    assert await client.count(table, HashKey("h", "h1")) == 1
    assert await client.count(table, HashKey("h", "h2")) == 0
    assert await client.count(table, HashKey("h", "h1"), limit=1) == 1
    assert await client.count(table, HashKey("h", "h2"), limit=1) == 0
    await client.put_item(table, {"h": "h2", "r": "r2"})
    assert await client.count(table, HashKey("h", "h1")) == 1
    assert await client.count(table, HashKey("h", "h2")) == 1
    assert await client.count(table, HashKey("h", "h1"), limit=1) == 1
    assert await client.count(table, HashKey("h", "h2"), limit=1) == 1
    await client.put_item(table, {"h": "h2", "r": "r1"})
    assert await client.count(table, HashKey("h", "h2")) == 2
    assert await client.count(table, HashKey("h", "h1")) == 1
    assert await client.count(table, HashKey("h", "h1"), limit=1) == 1
    assert await client.count(table, HashKey("h", "h2"), limit=1) == 1
    assert (
        await client.count(table, HashKey("h", "h1") & RangeKey("r").begins_with("x"))
        == 0
    )


async def test_count_with_limit(
    client: Client, prefilled_table: TableName, consistent_read: bool
) -> None:
    assert (
        await client.count(
            prefilled_table,
            HashKey("h", "h"),
            limit=90,
            consistent_read=consistent_read,
        )
        == 90
    )


async def test_scan_count(client: Client, table: TableName) -> None:
    assert await client.scan_count(table) == 0
    assert await client.scan_count(table, limit=1) == 0
    await client.put_item(table, {"h": "h1", "r": "r1"})
    assert await client.scan_count(table) == 1
    assert await client.scan_count(table, limit=1) == 1
    await client.put_item(table, {"h": "h2", "r": "r2"})
    assert await client.scan_count(table) == 2
    assert await client.scan_count(table, limit=1) == 1

    assert (
        await client.scan_count(table, filter_expression=F("r").begins_with("x")) == 0
    )


async def test_scan_count_with_limit(
    client: Client, prefilled_table: TableName, consistent_read: bool
) -> None:
    assert (
        await client.scan_count(
            prefilled_table, limit=90, consistent_read=consistent_read
        )
        == 90
    )


async def test_update_item(client: Client, table: TableName) -> None:
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


async def test_delete_item(client: Client, table: TableName) -> None:
    item = {"h": "h", "r": "r"}
    await client.put_item(table, item)
    assert await client.get_item(table, item) == item
    assert (
        await client.delete_item(table, item, return_values=ReturnValues.all_old)
        == item
    )
    with pytest.raises(ItemNotFound):
        await client.get_item(table, item)


async def test_delete_table(
    client: Client, table_factory: TableFactory, wait_config: RetryConfig
) -> None:
    name = await table_factory(Throughput(5, 5))
    # no error
    await client.put_item(name, {"h": "h", "r": "r"})
    await client.delete_table(name, wait_for_disabled=wait_config)
    with pytest.raises(Exception):
        await client.put_item(name, {"h": "h", "r": "r"})


async def test_query(client: Client, table: TableName, consistent_read: bool) -> None:
    item1 = {"h": "h", "r": "1", "d": "x"}
    item2 = {"h": "h", "r": "2", "d": "y"}
    items = [item1, item2]
    await client.put_item(table, item1)
    await client.put_item(table, item2)
    index = 0
    async for item in client.query(
        table, HashKey("h", "h"), consistent_read=consistent_read
    ):
        assert item == items[index]
        index += 1
    assert index == 2


async def test_query_descending(client: Client, table: TableName) -> None:
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


async def test_scan(client: Client, table: TableName, consistent_read: bool) -> None:
    item1 = {"h": "h", "r": "1", "d": "x"}
    item2 = {"h": "h", "r": "2", "d": "y"}
    items = [item1, item2]
    await client.put_item(table, item1)
    await client.put_item(table, item2)
    index = 0
    async for item in client.scan(table, consistent_read=consistent_read):
        assert item == items[index]
        index += 1
    assert index == 2


async def test_exists(
    client: Client, table_factory: TableFactory, wait_config: RetryConfig
) -> None:
    throughput = Throughput(5, 5)
    key_schema = KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string))
    attrs = {"h": KeyType.string, "r": KeyType.string}
    name = await table_factory(throughput)
    try:
        assert await client.table_exists(name)
        desc = await client.describe_table(name)
        assert desc.throughput == throughput
        assert desc.status is TableStatus.active
        assert desc.attributes == attrs
        assert desc.key_schema == key_schema
        assert desc.item_count == 0
    finally:
        await client.delete_table(name, wait_for_disabled=wait_config)
    assert await client.table_exists(name) is False
    with pytest.raises(TableNotFound):
        await client.describe_table(name)


async def test_empty_string(
    client: Client, table: TableName, real_dynamo: bool
) -> None:
    if not real_dynamo:
        pytest.xfail("empty strings not supported by dynalite yet")
    key = {"h": "h", "r": "r"}
    await client.put_item(table, {**key, "s": ""})
    assert await client.get_item(table, key) == {"h": "h", "r": "r", "s": ""}
    assert await client.update_item(
        table,
        key,
        F("foo").set("") & F("bar").set("baz"),
        return_values=ReturnValues.all_new,
    ) == {"h": "h", "r": "r", "bar": "baz", "s": ""}


async def test_empty_item(client: Client, table: TableName) -> None:
    with pytest.raises(ValidationException):
        await client.put_item(table, {"h": "", "r": ""})


async def test_empty_list(client: Client, table: TableName) -> None:
    key = {"h": "h", "r": "r"}
    await client.put_item(table, {**key, "l": [1]})
    await client.update_item(table, key, F("l").set([]))
    assert await client.get_item(table, key) == {"h": "h", "r": "r", "l": []}


async def test_ttl(client: Client, table: TableName) -> None:
    try:
        desc = await client.describe_time_to_live(table)
    except UnknownOperation:
        raise pytest.skip("TTL not supported by database")
    assert desc.status == TimeToLiveStatus.disabled
    assert desc.attribute is None
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


async def test_query_with_limit(client: Client, prefilled_table: TableName) -> None:
    items = [
        item async for item in client.query(prefilled_table, HashKey("h", "h"), limit=1)
    ]
    assert len(items) == 1
    assert items[0]["r"] == "0"


@pytest.mark.parametrize(
    "range_key,expected",
    [
        (RangeKey("r").begins_with("10"), ["10"]),
        (RangeKey("r").between("97", "99"), ["97", "98", "99"]),
        (RangeKey("r").gt("98"), ["99"]),
        (RangeKey("r").gte("99"), ["99"]),
        (RangeKey("r").lt("1"), ["0"]),
        (RangeKey("r").lte("0"), ["0"]),
        (RangeKey("r").equals("1"), ["1"]),
    ],
    ids=repr,
)
async def test_query_range_key_filters(
    client: Client,
    prefilled_table: TableName,
    range_key: Condition,
    expected: List[str],
) -> None:
    items = [
        item["r"]
        async for item in client.query(
            prefilled_table, HashKey("h", "h") & range_key, projection=F("r")
        )
    ]
    assert items == expected


async def test_query_single_page(
    client: Client, prefilled_table: TableName, consistent_read: bool
) -> None:
    first_page = await client.query_single_page(
        prefilled_table, HashKey("h", "h"), consistent_read=consistent_read
    )
    assert first_page.items
    assert first_page.last_evaluated_key
    assert not first_page.is_last_page
    second_page = await client.query_single_page(
        prefilled_table,
        HashKey("h", "h"),
        start_key=first_page.last_evaluated_key,
    )
    assert not set(map(itemgetter("r"), first_page.items)) & set(
        map(itemgetter("r"), second_page.items)
    )


async def test_scan_single_page(
    client: Client, prefilled_table: TableName, consistent_read: bool
) -> None:
    first_page = await client.scan_single_page(
        prefilled_table, consistent_read=consistent_read
    )
    assert first_page.items
    assert first_page.last_evaluated_key
    assert not first_page.is_last_page
    second_page = await client.scan_single_page(
        prefilled_table,
        start_key=first_page.last_evaluated_key,
    )
    assert not set(map(itemgetter("r"), first_page.items)) & set(
        map(itemgetter("r"), second_page.items)
    )


async def test_scan_with_limit(client: Client, table: TableName) -> None:
    item1 = {"h": "h", "r": "1", "d": "x"}
    item2 = {"h": "h", "r": "2", "d": "y"}
    await client.put_item(table, item1)
    await client.put_item(table, item2)
    items = [item async for item in client.scan(table, limit=1)]
    assert len(items) == 1
    assert items[0] == item1


async def test_update_item_with_broken_update_expression(
    client: Client, table: TableName
) -> None:
    item = {"h": "h", "r": "r", "f": 1}
    await client.put_item(table, item)
    with pytest.raises(errors.ValidationException):
        await client.update_item(
            table, {"h": "h", "r": "r"}, F("f").set(2) & F("f").set(3)
        )


async def test_scan_with_projection_only(client: Client, table: TableName) -> None:
    item1 = {"h": "h", "r": "1", "d": "x"}
    item2 = {"h": "h", "r": "2", "d": "y"}
    await client.put_item(table, item1)
    await client.put_item(table, item2)
    items = [item async for item in client.scan(table, projection=F("d"))]
    assert items == [{"d": "x"}, {"d": "y"}]


async def test_put_item_with_condition_with_no_values(
    client: Client, table: TableName
) -> None:
    await client.put_item(
        table, {"h": "h", "r": "1"}, condition=F("h").does_not_exist()
    )
    with pytest.raises(errors.ConditionalCheckFailed):
        await client.put_item(
            table, {"h": "h", "r": "1"}, condition=F("h").does_not_exist()
        )


async def test_delete_item_with_conditions(client: Client, table: TableName) -> None:
    await client.put_item(table, {"h": "h", "r": "1", "d": "x"})
    with pytest.raises(errors.ConditionalCheckFailed):
        await client.delete_item(
            table, {"h": "h", "r": "1"}, condition=F("d").does_not_exist()
        )
    assert await client.get_item(table, {"h": "h", "r": "1"})


async def test_size_condition_expression(client: Client, table: TableName) -> None:
    key = {"h": "h", "r": "r"}

    await client.put_item(table, {**key, "s": "hello", "v": "initial", "n": 5})
    with pytest.raises(errors.ConditionalCheckFailed):
        await client.update_item(
            table,
            key,
            update_expression=F("v").set("unchanged"),
            condition=F("s").size().not_equals(F("n")),
        )
    item = await client.get_item(table, key)
    assert item["v"] == "initial"
    await client.update_item(
        table,
        key,
        update_expression=F("v").set("changed"),
        condition=F("s").size().equals(F("n")),
    )
    item = await client.get_item(table, key)
    assert item["v"] == "changed"
    await client.update_item(
        table,
        key,
        update_expression=F("v").set("final"),
        condition=F("s").size().lt(6),
    )
    item = await client.get_item(table, key)
    assert item["v"] == "final"


async def test_comparison_condition_expression(
    client: Client, table: TableName
) -> None:
    key = {"h": "h", "r": "r"}

    await client.put_item(table, {**key, "v": "initial", "n": 5, "c": 6})
    with pytest.raises(errors.ConditionalCheckFailed):
        await client.update_item(
            table,
            key,
            update_expression=F("v").set("unchanged"),
            condition=F("n").equals(F("c")),
        )
    item = await client.get_item(table, key)
    assert item["v"] == "initial"
    await client.update_item(
        table,
        key,
        update_expression=F("v").set("changed"),
        condition=F("n").lt(F("c")),
    )
    item = await client.get_item(table, key)
    assert item["v"] == "changed"
    await client.update_item(
        table,
        key,
        update_expression=F("v").set("final"),
        condition=F("n").gte(5),
    )
    item = await client.get_item(table, key)
    assert item["v"] == "final"


async def test_no_credentials(
    http: HttpImplementation, endpoint: URL, region: str
) -> None:
    client = Client(
        http,
        ChainCredentials([]),
        region,
        endpoint,
        throttle_config=ExponentialBackoffRetry(time_limit_secs=1),
    )
    with pytest.raises(NoCredentialsFound):
        await client.get_item("no-table", {"key": "no-key"})


async def test_batch(client: Client, table: TableName) -> None:
    response = await client.batch_write(
        {
            table: BatchWriteRequest(
                items_to_put=[{"h": "h", "r": "1"}, {"h": "h", "r": "2"}]
            )
        }
    )
    assert not response
    assert len([item async for item in client.query(table, HashKey("h", "h"))]) == 2

    result = await client.batch_get(
        {table: BatchGetRequest(keys=[{"h": "h", "r": "1"}, {"h": "h", "r": "2"}])}
    )
    assert sorted(result.items[table], key=itemgetter("r")) == sorted(
        [{"h": "h", "r": "1"}, {"h": "h", "r": "2"}], key=itemgetter("r")
    )
    assert not result.unprocessed_keys

    response = await client.batch_write(
        {
            table: BatchWriteRequest(
                items_to_put=[{"h": "h", "r": "3"}],
                keys_to_delete=[{"h": "h", "r": "1"}],
            )
        }
    )
    assert not response
    assert len([item async for item in client.query(table, HashKey("h", "h"))]) == 2
    response = await client.batch_write(
        {
            table: BatchWriteRequest(
                keys_to_delete=[{"h": "h", "r": "2"}, {"h": "h", "r": "3"}],
            )
        }
    )
    assert not response
    assert len([item async for item in client.query(table, HashKey("h", "h"))]) == 0


@pytest.mark.usefixtures("supports_transactions")
@pytest.mark.parametrize(
    "items,aiodynamo_error",
    [
        ([], TransactionEmpty),
        (
            [Put(table="any-table", item={"h": "h", "r": str(i)}) for i in range(101)],
            TooManyTransactions,
        ),
    ],
)
async def test_transact_write_items_input_validation(
    client: Client,
    items: List[Put],
    aiodynamo_error: Type[Exception],
) -> None:
    with pytest.raises(aiodynamo_error):
        await client.transact_write_items(items=items)


@pytest.mark.usefixtures("supports_transactions")
async def test_transact_write_items_put(client: Client, table: TableName) -> None:
    puts = [
        Put(table=table, item={"h": "h", "r": str(i), "s": "initial"}) for i in range(2)
    ]
    await client.transact_write_items(items=puts)
    assert len([item async for item in client.query(table, HashKey("h", "h"))]) == 2

    with pytest.raises(errors.ConditionalCheckFailed):
        put = Put(
            table=table,
            item={"h": "h", "r": "0", "s": "initial"},
            condition=F("h").does_not_exist(),
        )
        await client.transact_write_items(items=[put])


@pytest.mark.usefixtures("supports_transactions")
async def test_transact_write_items_update(client: Client, table: TableName) -> None:
    await client.put_item(table=table, item={"h": "h", "r": "1", "s": "initial"})
    updates = [
        Update(
            table=table,
            key={"h": "h", "r": "1"},
            expression=F("s").set(f"changed"),
        )
    ]
    await client.transact_write_items(items=updates)
    query = await client.query_single_page(table, HashKey("h", "h"))
    assert query.items[0]["s"] == "changed"

    with pytest.raises(errors.ConditionalCheckFailed):
        update = Update(
            table=table,
            key={"h": "h", "r": "1"},
            expression=F("s").set("changed2"),
            condition=F("s").not_equals("changed"),
        )
        await client.transact_write_items(items=[update])


@pytest.mark.usefixtures("supports_transactions")
async def test_transact_write_items_delete(client: Client, table: TableName) -> None:
    await client.put_item(table=table, item={"h": "h", "r": "1", "s": "initial"})
    deletes = [
        Delete(
            table=table,
            key={"h": "h", "r": "1"},
        )
    ]
    await client.transact_write_items(items=deletes)
    assert len([item async for item in client.query(table, HashKey("h", "h"))]) == 0

    await client.put_item(table=table, item={"h": "h", "r": "1", "s": "initial"})
    with pytest.raises(errors.ConditionalCheckFailed):
        delete = Delete(
            table=table,
            key={"h": "h", "r": "1"},
            condition=F("s").not_equals("initial"),
        )
        await client.transact_write_items(items=[delete])


@pytest.mark.usefixtures("supports_transactions")
async def test_transact_write_items_condition_check(
    client: Client, table: TableName
) -> None:
    await client.put_item(table=table, item={"h": "h", "r": "1", "s": "initial"})
    condition = ConditionCheck(
        table=table, key={"h": "h", "r": "1"}, condition=F("s").not_equals("initial")
    )
    with pytest.raises(errors.ConditionalCheckFailed):
        await client.transact_write_items(items=[condition])

    condition = ConditionCheck(
        table=table, key={"h": "h", "r": "1"}, condition=F("s").equals("initial")
    )
    await client.transact_write_items(items=[condition])


@pytest.mark.usefixtures("supports_transactions")
async def test_transact_write_items_multiple_operations(
    client: Client, table: TableName
) -> None:
    await client.put_item(table=table, item={"h": "h", "r": "1", "s": "initial"})
    await client.put_item(table=table, item={"h": "h", "r": "2", "s": "initial"})

    put = Put(table=table, item={"h": "h", "r": "3", "s": "initial"})
    update = Update(
        table=table,
        key={"h": "h", "r": "1"},
        expression=F("s").set("changed"),
    )
    delete = Delete(
        table=table,
        key={"h": "h", "r": "2"},
    )

    await client.transact_write_items(items=[put, update, delete])

    items = [item async for item in client.query(table, HashKey("h", "h"))]
    assert len(items) == 2
    assert items[0]["s"] == "changed"


@pytest.mark.usefixtures("supports_transactions")
@pytest.mark.parametrize(
    "items,aiodynamo_error",
    [
        ([], TransactionEmpty),
        (
            [Get(table="any-table", key={"h": "h", "r": str(i)}) for i in range(101)],
            TooManyTransactions,
        ),
    ],
)
async def test_transact_get_items_input_validation(
    client: Client,
    items: List[Get],
    aiodynamo_error: Type[Exception],
) -> None:
    with pytest.raises(aiodynamo_error):
        await client.transact_get_items(items=items)


@pytest.mark.usefixtures("supports_transactions")
async def test_transact_get_items(client: Client, table: TableName) -> None:
    await client.put_item(table=table, item={"h": "h", "r": "1", "s": "initial"})
    items = [Get(table=table, key={"h": "h", "r": "1"})]

    response = await client.transact_get_items(items=items)

    assert len(response) == 1


@pytest.mark.usefixtures("supports_transactions")
async def test_transact_get_items_with_projection(
    client: Client, table: TableName
) -> None:
    await client.put_item(table=table, item={"h": "h", "r": "1", "s": "initial"})
    items = [Get(table=table, key={"h": "h", "r": "1"}, projection=F("h") & F("s"))]

    response = await client.transact_get_items(items=items)

    assert len(response) == 1
    assert response[0] == {"h": "h", "s": "initial"}


async def test_pay_per_request_table(
    client: Client, pay_per_request_table: TableName
) -> None:
    # query and scan are tested in the same method since creating all the items takes a long time
    big = "x" * 20_000

    await asyncio.gather(
        *(
            client.put_item(pay_per_request_table, {"h": "h", "r": str(i), "big": big})
            for i in range(100)
        )
    )

    first_page = await client.query_single_page(
        pay_per_request_table, HashKey("h", "h")
    )
    assert first_page.items
    assert first_page.last_evaluated_key
    assert not first_page.is_last_page
    second_page = await client.query_single_page(
        pay_per_request_table,
        HashKey("h", "h"),
        start_key=first_page.last_evaluated_key,
    )
    assert not set(map(itemgetter("r"), first_page.items)) & set(
        map(itemgetter("r"), second_page.items)
    )

    first_page = await client.scan_single_page(pay_per_request_table)
    assert first_page.items
    assert first_page.last_evaluated_key
    assert not first_page.is_last_page
    second_page = await client.scan_single_page(
        pay_per_request_table,
        start_key=first_page.last_evaluated_key,
    )
    assert not set(map(itemgetter("r"), first_page.items)) & set(
        map(itemgetter("r"), second_page.items)
    )
