import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from aiodynamo.types import Timeout
from httpx import AsyncClient
from yarl import URL

from ..errors import exception_from_response
from .base import HTTP, Headers, RequestFailed


@dataclass(frozen=True)
class HTTPX(HTTP):
    client: AsyncClient

    async def get(
        self, *, url: URL, headers: Optional[Headers] = None, timeout: Timeout
    ) -> bytes:
        response = await self.client.get(str(url), headers=headers, timeout=timeout)
        if response.status_code >= 400:
            raise RequestFailed(
                url, response.status_code, await response.aread(), headers
            )
        return await response.aread()

    async def post(
        self, *, url: URL, body: bytes, headers: Optional[Headers] = None
    ) -> Dict[str, Any]:
        response = await self.client.post(str(url), data=body, headers=headers)
        if response.status_code >= 400:
            raise exception_from_response(response.status_code, await response.aread())
        return json.loads(await response.aread())
