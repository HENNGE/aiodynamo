import datetime

import aiohttp
import httpx
import pytest
from aiodynamo.fast.credentials import (
    EnvironmentCredentials,
    InstanceMetadataCredentials,
    Key,
    Metadata,
)
from aiodynamo.fast.http.aiohttp import AIOHTTP
from aiodynamo.fast.http.httpx import HTTPX
from aiohttp import web
from yarl import URL

pytestmark = [pytest.mark.asyncio]


@pytest.fixture(params=["aiohttp", "httpx"])
async def http(request):
    if request.param == "aiohttp":
        async with aiohttp.ClientSession() as session:
            yield AIOHTTP(session)
    else:
        async with httpx.AsyncClient() as client:
            yield HTTPX(client)


class InstanceMetadataServer:
    def __init__(self):
        self.port = 0
        self.role = None
        self.metadata = None

    async def role_handler(self, request):
        if self.role is None:
            raise web.HTTPNotFound()
        return web.Response(body=self.role.encode("utf-8"))

    async def credentials_handler(self, request):
        if self.role is None:
            raise web.HTTPNotFound()
        if request.match_info["role"] != self.role:
            raise web.HTTPNotFound()
        if self.metadata is None:
            raise web.HTTPNotFound()
        creds = {
            "Code": "Success",
            "LastUpdated": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "Type": "AWS-HMAC",
            "AccessKeyId": self.metadata.key.id,
            "SecretAccessKey": self.metadata.key.secret,
            "Expiration": self.metadata.expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if self.metadata.key.token:
            creds["Token"] = self.metadata.key.token
        return web.json_response(creds)


@pytest.fixture
async def instance_metadata_server():
    ims = InstanceMetadataServer()
    app = web.Application()
    app.add_routes(
        [web.get("/latest/meta-data/iam/security-credentials/", ims.role_handler)]
    )
    app.add_routes(
        [
            web.get(
                "/latest/meta-data/iam/security-credentials/{role}",
                ims.credentials_handler,
            )
        ]
    )
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    ims.port = site._server.sockets[0].getsockname()[1]
    yield ims
    await runner.cleanup()


async def test_env_credentials(monkeypatch, http):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    assert await EnvironmentCredentials().get_key(http) is None
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "accesskey")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secretkey")
    key = await EnvironmentCredentials().get_key(http)
    assert key is not None
    assert key.id == "accesskey"
    assert key.secret == "secretkey"
    assert key.token is None
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    key = await EnvironmentCredentials().get_key(http)
    assert key is not None
    assert key.id == "accesskey"
    assert key.secret == "secretkey"
    assert key.token == "token"


async def test_ec2_instance_metdata_credentials(http, instance_metadata_server):
    imc = InstanceMetadataCredentials(
        timeout=0.1,
        base_url=URL("http://localhost").with_port(instance_metadata_server.port),
    )
    with pytest.raises(Exception):
        assert await imc.get_key(http)
    instance_metadata_server.role = "hoge"
    metadata = Metadata(
        Key("id", "secret", "token"),
        datetime.datetime.now() + datetime.timedelta(days=2),
    )
    instance_metadata_server.metadata = metadata
    assert await imc.get_key(http) == metadata.key
    instance_metadata_server.role = None
    instance_metadata_server.metadata = None
    assert await imc.get_key(http) == metadata.key
