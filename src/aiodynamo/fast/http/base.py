import abc
from dataclasses import dataclass
from typing import *

from yarl import URL

Headers = Mapping[str, str]


@dataclass
class RequestFailed(Exception):
    url: URL
    status: int
    response: Optional[bytes] = None
    headers: Optional[Headers] = None
    body: Optional[bytes] = None


class TooManyRetries(Exception):
    pass


class HTTP(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get(self, *, url: URL, headers: Optional[Headers] = None) -> bytes:
        pass

    @abc.abstractmethod
    async def post(
        self, *, url: URL, body: bytes, headers: Optional[Headers] = None
    ) -> Dict[str, Any]:
        pass
