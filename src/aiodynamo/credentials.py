from __future__ import annotations

import abc
import asyncio
import datetime
import json
import logging
import os
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import *

from yarl import URL

from .http.base import HTTP, Headers
from .types import Timeout
from .utils import parse_amazon_timestamp


@dataclass(frozen=True)
class Key:
    id: str
    secret: str = field(repr=False)
    token: Optional[str] = field(repr=False, default=None)


class Credentials(metaclass=abc.ABCMeta):
    @classmethod
    def auto(cls) -> ChainCredentials:
        """
        Return the default credentials loader chain.
        """
        return ChainCredentials(
            candidates=[
                EnvironmentCredentials(),
                ContainerMetadataCredentials(),
                InstanceMetadataCredentials(),
            ]
        )

    @abc.abstractmethod
    async def get_key(self, http: HTTP) -> Optional[Key]:
        """
        Return a Key if one could be found.
        """

    @abc.abstractmethod
    def invalidate(self) -> bool:
        """
        Invalidate the credentials if possible and return whether credentials
        got invalidated or not.
        """


@dataclass(frozen=True)
class ChainCredentials(Credentials):
    """
    Chains multiple credentials providers together, trying them
    in order. Returns the first key found. Exceptions are suppressed.
    """

    candidates: Sequence[Credentials]

    async def get_key(self, http: HTTP) -> Optional[Key]:
        for candidate in self.candidates:
            try:
                key = await candidate.get_key(http)
            except:
                logging.exception("Candidate %r failed", candidate)
                continue
            if key is None:
                logging.debug("Candidate %r didn't find a key", candidate)
            else:
                logging.debug("Candidate %r found a key %r", candidate, key)
                return key
        return None

    def invalidate(self) -> bool:
        return any(candidate.invalidate() for candidate in self.candidates)


class EnvironmentCredentials(Credentials):
    """
    Loads the credentials from the environment.
    """

    def __init__(self):
        try:
            self.key = Key(
                os.environ["AWS_ACCESS_KEY_ID"],
                os.environ["AWS_SECRET_ACCESS_KEY"],
                os.environ.get("AWS_SESSION_TOKEN", None),
            )
        except KeyError:
            self.key = None

    async def get_key(self, http: HTTP) -> Optional[Key]:
        return self.key

    def invalidate(self) -> bool:
        return False


class Refresh(Enum):
    required = auto()
    soon = auto()
    not_required = auto()


EXPIRES_SOON_THRESHOLD = datetime.timedelta(minutes=15)
EXPIRED_THRESHOLD = datetime.timedelta(minutes=10)


@dataclass(frozen=True)
class Metadata:
    key: Key
    expires: datetime.datetime

    def check_refresh(self) -> Refresh:
        now = datetime.datetime.now(datetime.timezone.utc)
        if now >= self.expires:
            return Refresh.required
        diff = self.expires - now
        if diff < EXPIRED_THRESHOLD:
            return Refresh.required
        if diff < EXPIRES_SOON_THRESHOLD:
            return Refresh.soon
        return Refresh.not_required


@dataclass
class MetadataCredentials(Credentials, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def fetch_metadata(self, http: HTTP) -> Metadata:
        pass

    @abc.abstractmethod
    def is_disabled(self) -> bool:
        pass

    async def fetch_with_retry(
        self,
        *,
        http: HTTP,
        max_attempts: int,
        url: URL,
        timeout: Timeout,
        headers: Optional[Headers] = None
    ) -> bytes:
        for attempt in range(max_attempts):
            try:
                response = await http.get(url=url, headers=headers, timeout=timeout)
            except Exception:
                logging.exception("GET failed")
                continue
            if response:
                logging.debug("fetchhed metadata %r", response)
                return response
        raise TooManyRetries()

    def __post_init__(self):
        self._metadata: Optional[Metadata] = None
        self._refresher: Optional[asyncio.Task] = None

    async def get_key(self, http: HTTP) -> Optional[Key]:
        if self.is_disabled():
            logging.debug("%r is disabled", self)
            return None
        refresh = self._check_refresh()
        logging.debug("refresh status %r", refresh)
        if refresh is Refresh.required:
            if self._refresher is None:
                logging.debug("starting mandatory refresh")
                self._refresher = asyncio.create_task(self._refresh(http))
            else:
                logging.debug("re-using refresh")
            try:
                await self._refresher
            finally:
                self._refresher = None
        elif refresh is Refresh.soon:
            if self._refresher is None:
                logging.debug("starting early refresh")
                self._refresher = asyncio.create_task(self._refresh(http))
            else:
                logging.debug("already refreshing")
        return self._metadata and self._metadata.key

    def invalidate(self) -> bool:
        logging.debug("%r invalidated", self)
        self._metadata = None
        return True

    def _check_refresh(self) -> Refresh:
        if self._metadata is None:
            return Refresh.required
        return self._metadata.check_refresh()

    async def _refresh(self, http: HTTP):
        self._metadata = await self.fetch_metadata(http)
        logging.debug("fetched metadata %r", self._metadata)


T = TypeVar("T")
U = TypeVar("U")


def and_then(thing: Optional[T], mapper: Callable[[T], U]) -> Optional[U]:
    if thing is not None:
        return mapper(thing)
    return thing


# https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint.html
@dataclass
class ContainerMetadataCredentials(MetadataCredentials):
    timeout: Timeout = 2
    max_attempts: int = 3
    base_url: URL = URL("http://169.254.170.2")
    relative_uri: str = field(
        default_factory=lambda: os.environ.get(
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", None
        )
    )
    full_uri: URL = field(
        default_factory=lambda: and_then(
            os.environ.get("AWS_CONTAINER_CREDENTIALS_FULL_URI", None), URL
        )
    )
    auth_token: Optional[str] = field(
        default_factory=lambda: os.environ.get(
            "AWS_CONTAINER_AUTHORIZATION_TOKEN", None
        )
    )

    @property
    def url(self) -> Optional[URL]:
        if self.relative_uri is not None:
            return self.base_url.with_path(self.relative_uri)
        elif self.full_uri is not None:
            return self.full_uri
        else:
            return None

    def is_disabled(self) -> bool:
        return self.url is None

    async def fetch_metadata(self, http: HTTP) -> Metadata:
        headers = and_then(self.auth_token, lambda token: {"Authorization": token})
        response = await self.fetch_with_retry(
            http=http,
            max_attempts=self.max_attempts,
            url=self.url,
            headers=headers,
            timeout=self.timeout,
        )
        data = json.loads(response)
        return Metadata(
            key=Key(
                id=data["AccessKeyId"],
                secret=data["SecretAccessKey"],
                token=data.get("Token", None),
            ),
            expires=parse_amazon_timestamp(data["Expiration"]),
        )


# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
@dataclass
class InstanceMetadataCredentials(MetadataCredentials):
    timeout: Timeout = 1
    max_attempts: int = 1
    base_url: URL = URL("http://169.254.169.254")
    disabled: bool = field(
        default_factory=lambda: os.environ.get(
            "AWS_EC2_METADATA_DISABLED", "false"
        ).lower()
        == "true"
    )

    def is_disabled(self) -> bool:
        return self.disabled

    async def fetch_metadata(self, http: HTTP) -> Metadata:
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
                credentials.get("Token", None),
            ),
            expires=parse_amazon_timestamp(credentials["Expiration"]),
        )


class TooManyRetries(Exception):
    pass
