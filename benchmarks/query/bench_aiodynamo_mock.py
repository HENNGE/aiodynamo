import asyncio
import json
from typing import Any, Dict, List

from pyperf import Runner

from aiodynamo.client import Client
from aiodynamo.credentials import Credentials
from aiodynamo.expressions import HashKey
from aiodynamo.http.mock import MockHTTP

RESP: List[bytes] = []


async def inner() -> None:
    client = Client(MockHTTP(RESP), Credentials.auto(), region="foo", endpoint=None)
    items = [item async for item in client.query("t", key_condition=HashKey("f", "v"))]


def query_aiodynamo_mock() -> None:
    asyncio.run(inner())


def setup_responses() -> None:
    def item(i: int) -> Dict[str, Any]:
        return {
            "foobar": {"S": "foobar"},
            "quux": {"S": f"sample-{ i }"},
            **{f"field-{ j }": {"S": f"value-{ j }"} for j in range(100)},
        }

    RESP[:] = [
        json.dumps(
            {
                "ScannedCount": 1000,
                "Count": 1000,
                "Items": [item(i) for i in range(1000)],
            }
        ).encode("utf-8")
    ]


if __name__ == "__main__":
    setup_responses()
    Runner().bench_func("query", query_aiodynamo_mock)
