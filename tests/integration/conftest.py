import asyncio
import os
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
    Optional,
    Union,
    cast,
)

import pytest
from _pytest.fixtures import SubRequest
from httpx import AsyncClient
from yarl import URL

from aiodynamo.client import Client
from aiodynamo.credentials import Credentials
from aiodynamo.errors import UnknownOperation
from aiodynamo.expressions import F
from aiodynamo.http.httpx import HTTPX
from aiodynamo.http.types import HttpImplementation
from aiodynamo.models import (
    KeySchema,
    KeySpec,
    KeyType,
    PayPerRequest,
    RetryConfig,
    StaticDelayRetry,
    Throughput,
)
from aiodynamo.operations import ConditionCheck
from aiodynamo.types import TableName

TableFactory = Callable[[Union[Throughput, PayPerRequest]], Awaitable[str]]


class Flavor(Enum):
    real = "real"
    scylla = "scylla"
    dynalite = "dynalite"
    other = "other"


@dataclass
class Implementation:
    name: str
    flavor: Flavor
    endpoint: Optional[URL]


def find_dynamo_implementations() -> Iterable[Implementation]:
    if os.environ.get("TEST_ON_AWS", "false") == "true":
        yield Implementation(name="Real DynamoDB", flavor=Flavor.real, endpoint=None)
    for impl in filter(bool, os.environ.get("DYNAMODB_URLS", "").split(" ")):
        name, url = impl.split("=")
        if "," in url:
            url, flavor_name = url.split(",")
            flavor = Flavor(flavor_name)
        else:
            flavor = Flavor.other
        yield Implementation(name=name, flavor=flavor, endpoint=URL(url))
    try:
        yield Implementation(
            name=os.environ.get("DYNAMODB_NAME", "single"),
            flavor=Flavor(os.environ.get("DYNAMODB_FLAVOR", "other")),
            endpoint=URL(os.environ["DYNAMODB_URL"]),
        )
    except KeyError:
        pass


def pytest_make_parametrize_id(config: Any, val: Any) -> str:
    if isinstance(val, Implementation):
        return val.name
    if isinstance(val, str):
        return val
    return repr(val)


@pytest.fixture(scope="session", params=find_dynamo_implementations())
def dynamodb_implementation(request: SubRequest) -> Implementation:
    return cast(Implementation, request.param)


@pytest.fixture(scope="session")
def table_name_prefix() -> str:
    return os.environ.get("DYNAMODB_TABLE_PREFIX", "")


@pytest.fixture(scope="session")
def flavor(dynamodb_implementation: Implementation) -> Flavor:
    return dynamodb_implementation.flavor


@pytest.fixture(scope="session")
def real_dynamo(flavor: Flavor) -> bool:
    return flavor is Flavor.real


@pytest.fixture(scope="session")
def scylla(flavor: Flavor) -> bool:
    return flavor is Flavor.scylla


@pytest.fixture(scope="session")
def dynalite(flavor: Flavor) -> bool:
    return flavor is Flavor.dynalite


@pytest.fixture()
async def supports_transactions(client: Client, table: TableName) -> None:
    try:
        await client.transact_write_items(
            [ConditionCheck(table, {"h": "h", "r": "r"}, F("h").does_not_exist())]
        )
    except UnknownOperation:
        raise pytest.skip("Transactions not supported")


@pytest.fixture(scope="session")
def endpoint(dynamodb_implementation: Implementation) -> Optional[URL]:
    return dynamodb_implementation.endpoint


@pytest.fixture(scope="session")
def region() -> str:
    return os.environ.get("DYNAMODB_REGION", "us-east-1")


@pytest.fixture()
def client(
    http: HttpImplementation, endpoint: URL, region: str
) -> Generator[Client, None, None]:
    yield Client(
        http,
        Credentials.auto(),
        region,
        endpoint,
    )


@pytest.fixture(scope="session")
def wait_config(dynamodb_implementation: Implementation) -> RetryConfig:
    return (
        RetryConfig.default_wait_config()
        if dynamodb_implementation.flavor is Flavor.real
        else StaticDelayRetry(time_limit_secs=5, delay=0.5)
    )


@pytest.fixture(params=[True, False], scope="session")
def consistent_read(request: SubRequest) -> bool:
    return cast(bool, request.param)


async def _make_table(
    client: Client,
    table_name_prefix: str,
    throughput: Union[Throughput, PayPerRequest],
    wait_config: RetryConfig,
) -> str:
    name = table_name_prefix + str(uuid.uuid4())
    await client.create_table(
        name,
        throughput,
        KeySchema(KeySpec("h", KeyType.string), KeySpec("r", KeyType.string)),
        wait_for_active=wait_config,
    )
    return name


@pytest.fixture
async def table_factory(
    client: Client, table_name_prefix: str, wait_config: RetryConfig
) -> Callable[[Throughput], Awaitable[str]]:
    async def factory(throughput: Throughput = Throughput(5, 5)) -> str:
        return await _make_table(client, table_name_prefix, throughput, wait_config)

    return factory


@pytest.fixture
async def table(
    client: Client, table_factory: TableFactory
) -> AsyncGenerator[str, None]:
    name = await table_factory(Throughput(5, 5))
    try:
        yield name
    finally:
        await client.delete_table(name)


@pytest.fixture(scope="session")
def prefilled_table(
    endpoint: URL,
    region: str,
    table_name_prefix: str,
    wait_config: RetryConfig,
    session_event_loop: asyncio.BaseEventLoop,
) -> Generator[str, None, None]:
    """
    Event loop is function scoped, so we can't use pytest-asyncio here.
    """

    async def startup() -> str:
        async with AsyncClient() as session:
            client = Client(HTTPX(session), Credentials.auto(), region, endpoint)
            name = await _make_table(
                client, table_name_prefix, Throughput(1000, 2500), wait_config
            )
            big = "x" * 20_000

            await asyncio.gather(
                *(
                    client.put_item(name, {"h": "h", "r": str(i), "big": big})
                    for i in range(100)
                )
            )

        return name

    async def shutdown(name: str) -> None:
        async with AsyncClient() as session:
            await Client(
                HTTPX(session), Credentials.auto(), region, endpoint
            ).delete_table(name)

    name = session_event_loop.run_until_complete(startup())
    try:
        yield name
    finally:
        session_event_loop.run_until_complete(shutdown(name))


@pytest.fixture
async def pay_per_request_table(
    client: Client, table_factory: TableFactory
) -> AsyncGenerator[str, None]:
    name = await table_factory(PayPerRequest())
    try:
        yield name
    finally:
        await client.delete_table(name)
