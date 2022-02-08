from dataclasses import dataclass
from typing import Dict, Optional, Union

from aiodynamo._compat import Literal, Protocol


@dataclass(frozen=True)
class Request:
    method: Union[Literal["GET"], Literal["POST"]]
    url: str
    headers: Optional[Dict[str, str]]
    body: Optional[bytes]


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes


@dataclass
class RequestFailed(Exception):
    inner: Exception


class HttpImplementation(Protocol):
    async def __call__(self, request: Request) -> Response:
        ...
