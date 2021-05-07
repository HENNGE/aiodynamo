import abc
from typing import *

from yarl import URL

from aiodynamo.types import Timeout

Headers = Dict[str, str]


class RequestFailed(Exception):
    pass


class HTTP(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get(
        self, *, url: URL, headers: Optional[Headers] = None, timeout: Timeout
    ) -> bytes:
        """
        Make a GET request and return the response as bytes.

        Raise a RequestFailed exception if the request was not successful.
        """
        pass

    @abc.abstractmethod
    async def post(
        self, *, url: URL, body: bytes, headers: Optional[Headers] = None
    ) -> Dict[str, Any]:
        """
        Make a POST request and return the parsed JSON object.

        Raise a RequestFailed exception if the request was not successful.
        """
        pass
