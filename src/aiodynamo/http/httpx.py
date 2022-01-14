from dataclasses import dataclass
from typing import Dict, cast

import httpx

from .types import Request, RequestFailed, Response


@dataclass(frozen=True)
class HTTPX:
    client: httpx.AsyncClient

    async def __call__(self, request: Request) -> Response:
        try:
            response = await self.client.request(
                method=request.method,
                url=request.url,
                # httpx is coded with no_implicit_optionals=false, we use strict=true
                headers=cast(Dict[str, str], request.headers),
                content=cast(bytes, request.body),
            )
            return Response(response.status_code, await response.aread())
        except httpx.HTTPError as exc:
            raise RequestFailed(exc)
