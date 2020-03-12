import asyncio
import datetime

import pytest
from aiodynamo.credentials import (
    EXPIRED_THRESHOLD,
    EXPIRES_SOON_THRESHOLD,
    EnvironmentCredentials,
    InstanceMetadataCredentials,
    Key,
    Metadata,
)
from aiohttp import web
from freezegun import freeze_time
from yarl import URL

pytestmark = [pytest.mark.asyncio]


class InstanceMetadataServer:
    def __init__(self):
        self.port = 0
        self.role = None
        self.metadata = None
        self.calls = 0

    async def role_handler(self, request):
        if self.role is None:
            raise web.HTTPNotFound()
        return web.Response(body=self.role.encode("utf-8"))

    async def credentials_handler(self, request):
        self.calls += 1
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


async def test_simultaneous_credentials_refresh(http, instance_metadata_server):
    instance_metadata_server.role = "hoge"
    now = datetime.datetime(2020, 3, 12, 15, 37, 51, tzinfo=datetime.timezone.utc)
    expires = now + EXPIRES_SOON_THRESHOLD - datetime.timedelta(seconds=10)
    expired = now + EXPIRED_THRESHOLD - datetime.timedelta(seconds=10)
    not_expired = now + datetime.timedelta(days=2)
    imc = InstanceMetadataCredentials(
        timeout=0.1,
        base_url=URL("http://localhost").with_port(instance_metadata_server.port),
    )
    key1 = Key("id1", "secret1")
    key2 = Key("id2", "secret2")
    imc._metadata = Metadata(key1, expires,)
    instance_metadata_server.metadata = Metadata(key2, not_expired)
    assert instance_metadata_server.calls == 0
    with freeze_time(now):
        key = await imc.get_key(http)
        assert key == key1
        assert instance_metadata_server.calls == 0
        assert imc._refresher is not None
        imc._metadata = Metadata(key1, expired)
        key = await imc.get_key(http)
        assert key == key2
        assert instance_metadata_server.calls == 1
