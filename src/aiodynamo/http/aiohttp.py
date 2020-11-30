import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, TypeVar, cast

from aiohttp import ClientError, ClientSession
from yarl import URL

from aiodynamo.types import Timeout

from ..errors import exception_from_response
from .base import HTTP, Headers, RequestFailed

T = TypeVar("T")


@contextmanager
def wrap_errors() -> Iterator[None]:
    try:
        yield
    except (ClientError, asyncio.TimeoutError):
        raise RequestFailed()


@dataclass(frozen=True)
class AIOHTTP(HTTP):
    session: ClientSession

    async def get(
        self, *, url: URL, headers: Optional[Headers] = None, timeout: Timeout
    ) -> bytes:
        with wrap_errors():
            async with self.session.request(
                method="GET", url=url, headers=headers, timeout=timeout
            ) as response:
                if response.status >= 400:
                    raise RequestFailed()
                return await response.read()

    async def post(
        self, *, url: URL, body: bytes, headers: Optional[Headers] = None
    ) -> Dict[str, Any]:
        with wrap_errors():
            async with self.session.request(
                method="POST",
                url=url,
                headers=headers,
                data=body,
            ) as response:
                if response.status >= 400:
                    raise exception_from_response(
                        response.status, await response.read()
                    )
                return cast(
                    Dict[str, Any],
                    await response.json(
                        content_type="application/x-amz-json-1.0", encoding="utf-8"
                    ),
                )
