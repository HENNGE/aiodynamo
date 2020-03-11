import pytest


@pytest.fixture(params=["httpx", "aiohttp"])
async def http(request):
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
