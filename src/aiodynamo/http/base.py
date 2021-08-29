import abc
from typing import Dict, Optional

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
    ) -> bytes:
        """
        Make a POST request and return the body bytes.

        Raise a RequestFailed exception if the request was not successful.
        """
        pass
