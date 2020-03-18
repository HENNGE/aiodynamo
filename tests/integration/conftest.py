import contextlib
import os
import uuid

import pytest
from aiodynamo.client import Client
from aiodynamo.credentials import Credentials
from aiodynamo.models import KeySchema, KeySpec, KeyType, Throughput, WaitConfig
from yarl import URL


@pytest.fixture
def table_name_prefix() -> str:
    return os.environ.get("DYNAMODB_TABLE_PREFIX", "")


@pytest.fixture
def endpoint():
    if os.environ.get("TEST_ON_AWS", "false") == "true":
        return None
    if "DYNAMODB_URL" not in os.environ:
        raise pytest.skip("DYNAMODB_URL not defined in environment")
    return os.environ["DYNAMODB_URL"]


@pytest.fixture
def region():
    return os.environ.get("DYNAMODB_REGION", "us-east-1")


@pytest.fixture
async def client(http, endpoint, region):
    yield Client(
        http,
        Credentials.auto(),
        region,
        URL(endpoint) if endpoint is not None else endpoint,
    )


@contextlib.asynccontextmanager
async def table_factory(client: Client, table_name_prefix: str, throughput: int = 5):
    name = table_name_prefix + str(uuid.uuid4())
    await client.create_table(
        name,
        Throughput(throughput, throughput),
        KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string)),
        wait_for_active=WaitConfig(max_attempts=25, retry_delay=5),
    )
    try:
        yield name
    finally:
        await client.delete_table(name)


@pytest.fixture
async def table(client: Client, table_name_prefix: str):
    async with table_factory(client, table_name_prefix) as name:
        yield name


@pytest.fixture
async def fast_table(client: Client, table_name_prefix: str):
    async with table_factory(client, table_name_prefix, throughput=1000) as name:
        yield name
