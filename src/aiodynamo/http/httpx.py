import json
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, cast

from httpx import AsyncClient, HTTPError
from yarl import URL

from aiodynamo.types import Timeout

from ..errors import exception_from_response
from .base import HTTP, Headers, RequestFailed


@contextmanager
def wrap_errors() -> Iterator[None]:
    try:
        yield
    except HTTPError:
        raise RequestFailed()


@dataclass(frozen=True)
class HTTPX(HTTP):
    client: AsyncClient

    async def get(
        self, *, url: URL, headers: Optional[Headers] = None, timeout: Timeout
    ) -> bytes:
        with wrap_errors():
            # FIXME: the `or {}` is not needed but httpx type hints are wrong
            response = await self.client.get(
                str(url), headers=headers or {}, timeout=timeout
            )
            if response.status_code >= 400:
                raise RequestFailed()
            return await response.aread()

    async def post(
        self, *, url: URL, body: bytes, headers: Optional[Headers] = None
    ) -> Dict[str, Any]:
        with wrap_errors():
            # FIXME: the `or {}` is not needed but httpx type hints are wrong
            response = await self.client.post(
                str(url), data=body, headers=headers or {}
            )
            if response.status_code >= 400:
                raise exception_from_response(
                    response.status_code, await response.aread()
                )
            return cast(Dict[str, Any], json.loads(await response.aread()))
