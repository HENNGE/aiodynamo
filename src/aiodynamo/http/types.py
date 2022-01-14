from dataclasses import dataclass
from typing import Awaitable, Dict, Optional, Union

from aiodynamo._compat import Literal
from aiodynamo._mypy_hacks import FixedCallable


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


HttpImplementation = FixedCallable[Request, Awaitable[Response]]
