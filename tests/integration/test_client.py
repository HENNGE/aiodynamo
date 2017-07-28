import os
import uuid

import pytest
from aiobotocore import get_session

from aiodynamo.client import Client, Throughput, KeySchema, Key, KeyType
from aiodynamo.utils import unroll


pytestmark = pytest.mark.asyncio


def _get_tables(core):
    return unroll(core.list_tables, 'ExclusiveStartTableName', 'LastEvaluatedTableName', 'TableNames')


async def _get_tables_list(core):
    return [table async for table in _get_tables(core)]

async def _cleanup(core, tables):
    async for table in _get_tables(core):
        if table not in tables:
            await core.delete_table(TableName=table)


@pytest.fixture
async def core():
    if 'DYNAMODB_URL' not in os.environ:
        raise pytest.skip('DYNAMODB_URL not defined in environment')
    core = get_session().create_client(
        'dynamodb',
        endpoint_url=os.environ['DYNAMODB_URL'],
        use_ssl=False,
        region_name=os.environ.get('DYNAMODB_REGION', 'us-east-1')
    )
    tables = await _get_tables_list(core)
    try:
        yield core
    finally:
        await _cleanup(core, tables)
        core.close()


@pytest.fixture
async def client(core):
    return Client(core)


@pytest.fixture
async def table(client: Client):
    name = str(uuid.uuid4())
    await client.create_table(name, Throughput(5, 5), KeySchema(Key('h', KeyType.string), Key('r', KeyType.string)))
    return name


async def test_put_get_item(client: Client, table):
    item = {
        'h': 'hash key value',
        'r': 'range key value',
        'string-key': 'this is a string',
        'number-key': 123,
        'list-key': ['hello', 'world'],
        'nested': {
            'nested': 'key'
        }
    }
    await client.put_item(
        table,
        item
    )
    db_item = await client.get_item(table, {'h': 'hash key value', 'r': 'range key value'})
    assert item == db_item
