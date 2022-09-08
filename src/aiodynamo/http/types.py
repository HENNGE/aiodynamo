from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional, Union

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


HttpImplementation = Callable[[Request], Awaitable[Response]]
