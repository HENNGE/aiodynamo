from dataclasses import dataclass

import aiohttp

from .types import Request, RequestFailed, Response


@dataclass(frozen=True)
class AIOHTTP:
    session: aiohttp.ClientSession

    async def __call__(self, request: Request) -> Response:
        try:
            async with self.session.request(
                request.method, request.url, headers=request.headers, data=request.body
            ) as response:
                return Response(response.status, await response.read())
        except aiohttp.ClientError as exc:
            raise RequestFailed(exc)
