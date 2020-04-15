import asyncio
from dataclasses import dataclass
from functools import wraps
from typing import Any, Dict, Optional

from aiodynamo.types import Timeout
from aiohttp import ClientError, ClientSession
from yarl import URL

from ..errors import exception_from_response
from .base import HTTP, Headers, RequestFailed


def wrap_errors(coro):
    @wraps(coro)
    async def wrapper(*args, **kwargs):
        try:
            return await coro(*args, **kwargs)
        except (ClientError, asyncio.TimeoutError):
            raise RequestFailed()

    return wrapper


@dataclass(frozen=True)
class AIOHTTP(HTTP):
    session: ClientSession

    @wrap_errors
    async def get(
        self, *, url: URL, headers: Optional[Headers] = None, timeout: Timeout
    ) -> bytes:
        async with self.session.request(
            method="GET", url=url, headers=headers, timeout=timeout
        ) as response:
            if response.status >= 400:
                raise RequestFailed()
            return await response.read()

    @wrap_errors
    async def post(
        self, *, url: URL, body: bytes, headers: Optional[Headers] = None
    ) -> Dict[str, Any]:
        async with self.session.request(
            method="POST", url=url, headers=headers, data=body,
        ) as response:
            if response.status >= 400:
                raise exception_from_response(response.status, await response.read())
            return await response.json(
                content_type="application/x-amz-json-1.0", encoding="utf-8"
            )
