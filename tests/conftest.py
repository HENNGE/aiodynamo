import asyncio
from asyncio import AbstractEventLoop
from typing import AsyncGenerator, Generator

import pytest
from _pytest.fixtures import SubRequest

from aiodynamo.http.types import HttpImplementation


@pytest.fixture(params=["httpx", "aiohttp"])
async def http(request: SubRequest) -> AsyncGenerator[HttpImplementation, None]:
    if request.param == "httpx":
        try:
            import httpx

            from aiodynamo.http.httpx import HTTPX
        except ImportError:
            raise pytest.skip("httpx not installed")
        async with httpx.AsyncClient() as client:
            yield HTTPX(client)
    elif request.param == "aiohttp":
        try:
            import aiohttp

            from aiodynamo.http.aiohttp import AIOHTTP
        except ImportError:
            raise pytest.skip("aiohttp not installed")
        async with aiohttp.ClientSession() as session:
            yield AIOHTTP(session)


@pytest.fixture(scope="session")
def session_event_loop() -> Generator[AbstractEventLoop, None, None]:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
