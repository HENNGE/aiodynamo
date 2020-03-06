from __future__ import annotations

import abc
import asyncio
import datetime
import json
import logging
import os
from dataclasses import dataclass, field
from enum import Enum, auto
from functools import wraps
from typing import *

from aiohttp import ClientSession
from yarl import URL

from ..types import Numeric
from .http.base import HTTP, Headers, TooManyRetries


@dataclass(frozen=True)
class Key:
    id: str
    secret: str = field(repr=False)
    token: Optional[str] = field(repr=False, default=None)


class Credentials(metaclass=abc.ABCMeta):
    @classmethod
    def auto(cls) -> ChainCredentials:
        return ChainCredentials(
            candidates=[
                EnvironmentCredentials(),
                ContainerMetadataCredentials(),
                InstanceMetadataCredentials(),
            ]
        )

    @abc.abstractmethod
    async def get_key(self, http: HTTP) -> Optional[Key]:
        pass


@dataclass(frozen=True)
class ChainCredentials(Credentials):
    candidates: Sequence[Credentials]

    async def get_key(self, http: HTTP) -> Optional[Key]:
        for candidate in self.candidates:
            try:
                key = await candidate.get_key(http)
            except:
                logging.exception("Candidate %r failed", candidate)
                continue
            if key is None:
                logging.info("Candidate %r didn't find a key", candidate)
            else:
                return key
        return None


class EnvironmentCredentials(Credentials):
    def __init__(self):
        try:
            self.key = Key(
                os.environ["AWS_ACCESS_KEY_ID"],
                os.environ["AWS_SECRET_ACCESS_KEY"],
                os.environ.get("AWS_SESSION_TOKEN", None),
            )
        except KeyError:
            self.key = None

    async def get_key(self, http: ClientSession) -> Optional[Key]:
        return self.key


def highlander(coro_func):
    """
    Ensures there can only ever be one "active" instance of the wrapped coroutine function.

    Multiple concurrent calls will only result in a single call to the wrapped coroutine and
    all callers will get the same result (or Exception).

    Non-concurrent calls will behave as if the coroutine function is not wrapped in this decorator.
    """
    future = None

    def reset_future(_):
        nonlocal future
        future = None

    async def shielded(*args, **kwargs):
        nonlocal future
        future = asyncio.create_task(coro_func(*args, **kwargs))
        future.add_done_callback(reset_future)
        return await future

    @wraps(coro_func)
    async def wrapper(*args, **kwargs):
        nonlocal future
        if future is not None:
            return await future
        # This shield is needed in case the caller to this gets cancelled. Otherwise
        # other "queued" callers will get a cancelled exception instead of the result.
        return await asyncio.shield(shielded(*args, **kwargs))

    return wrapper


class Expiration(Enum):
    not_expired = auto()
    expires_soon = auto()
    expired = auto()


EXPIRES_SOON_THRESHOLD = datetime.timedelta(minutes=15)
EXPIRED_THRESHOLD = datetime.timedelta(minutes=5)


@dataclass(frozen=True)
class Metadata:
    key: Key
    expires: datetime.datetime

    def lives_longer_than(self, other: Metadata) -> bool:
        return self.expires > other.expires

    def check_expiration(self) -> Expiration:
        now = datetime.datetime.now(datetime.timezone.utc)
        if now >= self.expires:
            return Expiration.expired
        diff = self.expires - now
        if diff < EXPIRED_THRESHOLD:
            return Expiration.expired
        if diff < EXPIRES_SOON_THRESHOLD:
            return Expiration.expires_soon
        return Expiration.not_expired


@dataclass
class MetadataCredentials(Credentials, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def _fetch_metadata(self, http: HTTP) -> Metadata:
        pass

    async def fetch_with_retry(
        self,
        *,
        http: HTTP,
        max_attempts: int,
        url: URL,
        timeout: Numeric,
        headers: Optional[Headers] = None
    ) -> bytes:
        for attempt in range(max_attempts):
            try:
                response = await http.get(url=url, headers=headers, timeout=timeout)
            except:
                logging.exception("GET failed")
                continue
            if response:
                return response
        raise TooManyRetries()

    def __post_init__(self):
        self._metadata: Optional[Metadata] = None

    async def get_key(self, http: HTTP) -> Optional[Key]:
        try:
            await self._check_metadata(http)
        except Disabled:
            return None
        return self._metadata and self._metadata.key

    @highlander
    async def _check_metadata(self, http: HTTP):
        if self._metadata is None:
            await self._refresh(http)
            return
        expiration = self._metadata.check_expiration()
        if expiration is Expiration.expired:
            await self._refresh(http)
            return
        elif expiration is Expiration.expires_soon:
            asyncio.create_task(self._maybe_refresh(http))
            return
        return self._metadata.key

    @highlander
    async def _refresh(self, http: HTTP):
        self._maybe_set_metadata(await self._fetch_metadata(http))

    @highlander
    async def _maybe_refresh(self, http: HTTP):
        try:
            self._maybe_set_metadata(await self._fetch_metadata(http))
        except:
            pass

    def _maybe_set_metadata(self, metadata: Metadata):
        """
        Guards against `_maybe_refresh` somehow being slower than `_refresh`
        """
        if self._metadata is None or metadata.lives_longer_than(self._metadata):
            self._metadata = metadata


class Disabled(Exception):
    pass


T = TypeVar("T")
U = TypeVar("U")


def and_then(thing: Optional[T], mapper: Callable[[T], U]) -> Optional[U]:
    if thing is not None:
        return mapper(thing)
    return thing


# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint.html
@dataclass
class ContainerMetadataCredentials(MetadataCredentials):
    timeout: Numeric = 2
    max_attempts: int = 3
    base_url: URL = URL("http://169.254.170.2")
    relative_uri: str = field(
        default_factory=lambda: os.environ.get(
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", None
        )
    )
    full_uri: str = field(
        default_factory=lambda: and_then(
            os.environ.get("AWS_CONTAINER_CREDENTIALS_FULL_URI", None), URL
        )
    )
    auth_token: Optional[str] = field(
        default_factory=lambda: os.environ.get(
            "AWS_CONTAINER_AUTHORIZATION_TOKEN", None
        )
    )

    async def _fetch_metadata(self, http: HTTP) -> Metadata:
        if self.relative_uri is not None:
            url = self.base_url.with_path(self.relative_uri)
        elif self.full_uri is not None:
            url = self.full_uri
        else:
            raise Disabled()
        headers = and_then(self.auth_token, lambda token: {"Authorization": token})
        response = await self.fetch_with_retry(
            http=http,
            max_attempts=self.max_attempts,
            url=url,
            headers=headers,
            timeout=self.timeout,
        )
        data = json.loads(response)
        return Metadata(
            key=Key(
                id=data["AccessKeyId"],
                secret=data["SecretAccessKey"],
                token=data["Token"],
            ),
            expires=parse_amazon_timestamp(data["Expiration"]),
        )


# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
@dataclass
class InstanceMetadataCredentials(MetadataCredentials):
    timeout: Numeric = 1
    max_attempts: int = 1
    base_url: URL = URL("http://169.254.169.254")
    disabled: bool = field(
        default_factory=lambda: os.environ.get(
            "AWS_EC2_METADATA_DISABLED", "false"
        ).lower()
        == "true"
    )

    async def _fetch_metadata(self, http: HTTP) -> Metadata:
        if self.disabled:
            raise Disabled()
        role_url = self.base_url.with_path(
            "/latest/meta-data/iam/security-credentials/"
        )
        role = (
            await self.fetch_with_retry(
                http=http,
                max_attempts=self.max_attempts,
                url=role_url,
                timeout=self.timeout,
            )
        ).decode("utf-8")
        credentials_url = role_url.join(URL(role))
        raw_credentials = await self.fetch_with_retry(
            http=http,
            max_attempts=self.max_attempts,
            url=credentials_url,
            timeout=self.timeout,
        )
        credentials = json.loads(raw_credentials)
        return Metadata(
            key=Key(
                credentials["AccessKeyId"],
                credentials["SecretAccessKey"],
                credentials["Token"],
            ),
            expires=parse_amazon_timestamp(credentials["Expiration"]),
        )


def parse_amazon_timestamp(timestamp: str) -> datetime.datetime:
    return datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc
    )
