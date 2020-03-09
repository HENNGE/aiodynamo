import os
import uuid

import httpx
import pytest
from aiobotocore import get_session
from aiodynamo.client import Client
from aiodynamo.fast.client import FastClient
from aiodynamo.fast.credentials import Credentials
from aiodynamo.fast.http.aiohttp import AIOHTTP
from aiodynamo.fast.http.httpx import HTTPX
from aiodynamo.models import KeySchema, KeySpec, KeyType, Throughput
from aiohttp import ClientSession
from yarl import URL


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
async def core(endpoint, region):
    if endpoint is None:
        core = get_session().create_client("dynamodb", region_name=region)
    else:
        core = get_session().create_client(
            "dynamodb", endpoint_url=endpoint, use_ssl=False, region_name=region,
        )
    try:
        yield core

    finally:
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
        wait_for_active=True,
    )
    try:
        yield name
    finally:
        await client.delete_table(name)
