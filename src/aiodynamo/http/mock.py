from dataclasses import dataclass
from typing import Optional, Any, List

from aiodynamo.types import Timeout

from .base import HTTP, Headers


@dataclass
class MockHTTP(HTTP):
    responses: List[bytes]
    counter: int = 0

    async def get(
        self, *, url: Any, headers: Optional[Headers] = None, timeout: Timeout
    ) -> bytes:
        try:
            return self.responses[self.counter]
        finally:
            self.counter = (self.counter + 1) % len(self.responses)

    async def post(
        self, *, url: Any, body: bytes, headers: Optional[Headers] = None
    ) -> bytes:
        try:
            return self.responses[self.counter]
        finally:
            self.counter = (self.counter + 1) % len(self.responses)
