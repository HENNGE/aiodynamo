import asyncio
import os
import uuid
from typing import Awaitable, Callable, List, Optional

import pytest
from aiodynamo.client import Client
from aiodynamo.credentials import Credentials
from aiodynamo.http.base import HTTP
from aiodynamo.models import KeySchema, KeySpec, KeyType, Throughput, WaitConfig
from yarl import URL


@pytest.fixture
def table_name_prefix() -> str:
    return os.environ.get("DYNAMODB_TABLE_PREFIX", "")


@pytest.fixture
def real_dynamo() -> bool:
    return os.environ.get("TEST_ON_AWS", "false") == "true"


@pytest.fixture
def endpoint(real_dynamo) -> Optional[URL]:
    if real_dynamo:
        return None
    if "DYNAMODB_URL" not in os.environ:
        raise pytest.skip("DYNAMODB_URL not defined in environment")
    return URL(os.environ["DYNAMODB_URL"])


@pytest.fixture
def region() -> str:
    return os.environ.get("DYNAMODB_REGION", "us-east-1")


@pytest.fixture
def client(http: HTTP, endpoint: URL, region: str) -> Client:
    yield Client(
        http, Credentials.auto(), region, endpoint,
    )


@pytest.fixture
async def table_factory(
    client: Client, table_name_prefix: str
) -> Callable[[Optional[Throughput]], Awaitable[str]]:
    async def factory(throughput: Throughput = Throughput(5, 5)) -> str:
        name = table_name_prefix + str(uuid.uuid4())
        await client.create_table(
            name,
            throughput,
            KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string)),
            wait_for_active=WaitConfig(max_attempts=25, retry_delay=5),
        )
        return name

    return factory


@pytest.fixture
async def table(
    client: Client, table_factory: Callable[[Optional[Throughput]], Awaitable[str]]
) -> str:
    name = await table_factory()
    try:
        yield name
    finally:
        await client.delete_table(name)


@pytest.fixture
async def high_throughput_table(
    client: Client, table_factory: Callable[[Optional[Throughput]], Awaitable[str]]
):
    name = await table_factory(Throughput(1000, 2500))
    try:
        yield name
    finally:
        await client.delete_table(name)
