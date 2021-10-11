import asyncio
import os
import sys
import uuid
from typing import AsyncGenerator, Awaitable, Generator, Optional, Union

from httpx import AsyncClient

from aiodynamo.http.httpx import HTTPX

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

import pytest
from yarl import URL

from aiodynamo.client import Client
from aiodynamo.credentials import Credentials
from aiodynamo.http.base import HTTP
from aiodynamo.models import (
    KeySchema,
    KeySpec,
    KeyType,
    PayPerRequest,
    Throughput,
    WaitConfig,
)


class TableFactory(Protocol):
    @staticmethod
    def __call__(throughput: Union[Throughput, PayPerRequest] = ...) -> Awaitable[str]:
        ...


@pytest.fixture(scope="session")
def table_name_prefix() -> str:
    return os.environ.get("DYNAMODB_TABLE_PREFIX", "")


@pytest.fixture(scope="session")
def real_dynamo() -> bool:
    return os.environ.get("TEST_ON_AWS", "false") == "true"


@pytest.fixture(scope="session")
def endpoint(real_dynamo: bool) -> Optional[URL]:
    if real_dynamo:
        return None
    if "DYNAMODB_URL" not in os.environ:
        raise pytest.skip("DYNAMODB_URL not defined in environment")
    return URL(os.environ["DYNAMODB_URL"])


@pytest.fixture(scope="session")
def region() -> str:
    return os.environ.get("DYNAMODB_REGION", "us-east-1")


@pytest.fixture()
def client(http: HTTP, endpoint: URL, region: str) -> Generator[Client, None, None]:
    yield Client(
        http,
        Credentials.auto(),
        region,
        endpoint,
    )


async def _make_table(
    client: Client, table_name_prefix: str, throughput: Union[Throughput, PayPerRequest]
) -> str:
    name = table_name_prefix + str(uuid.uuid4())
    await client.create_table(
        name,
        throughput,
        KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string)),
        wait_for_active=WaitConfig(max_attempts=25, retry_delay=5),
    )
    return name


@pytest.fixture()
async def table_factory(client: Client, table_name_prefix: str) -> TableFactory:
    async def factory(
        throughput: Union[Throughput, PayPerRequest] = Throughput(5, 5)
    ) -> str:
        return await _make_table(client, table_name_prefix, throughput)

    return factory


@pytest.fixture
async def table(
    client: Client, table_factory: TableFactory
) -> AsyncGenerator[str, None]:
    name = await table_factory()
    try:
        yield name
    finally:
        await client.delete_table(name)


@pytest.fixture(scope="session")
def prefilled_table(endpoint: URL, region: str, table_name_prefix: str):
    """
    Event loop is function scoped, so we can't use pytest-asyncio here.
    """

    async def startup() -> str:
        async with AsyncClient() as session:
            client = Client(HTTPX(session), Credentials.auto(), region, endpoint)
            name = await _make_table(client, table_name_prefix, Throughput(1000, 2500))
            big = "x" * 20_000

            await asyncio.gather(
                *(
                    client.put_item(name, {"h": "h", "r": str(i), "big": big})
                    for i in range(100)
                )
            )

        return name

    async def shutdown(name: str):
        async with AsyncClient() as session:
            await Client(
                HTTPX(session), Credentials.auto(), region, endpoint
            ).delete_table(name)

    name = asyncio.run(startup())
    try:
        yield name
    finally:
        asyncio.run(shutdown(name))


@pytest.fixture
async def pay_per_request_table(
    client: Client, table_factory: TableFactory
) -> AsyncGenerator[str, None]:
    name = await table_factory(PayPerRequest())
    try:
        yield name
    finally:
        await client.delete_table(name)
