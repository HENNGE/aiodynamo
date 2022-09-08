import json
from typing import Any, Type, cast

import pytest

from aiodynamo.client import Client
from aiodynamo.credentials import Key, StaticCredentials
from aiodynamo.errors import (
    ExpiredToken,
    InternalDynamoError,
    ProvisionedThroughputExceeded,
    ServiceUnavailable,
    Throttled,
)
from aiodynamo.expressions import HashKey
from aiodynamo.http.types import HttpImplementation, Request, Response
from aiodynamo.models import StaticDelayRetry


def bjson(data: Any) -> bytes:
    return json.dumps(data).encode()


@pytest.mark.parametrize(
    "status,dynamo_error,aiodynamo_error",
    [
        (400, "ThrottlingException", Throttled),
        (400, "ProvisionedThroughputExceededException", ProvisionedThroughputExceeded),
        (400, "ExpiredTokenException", ExpiredToken),
        (503, "", ServiceUnavailable),
        (500, "", InternalDynamoError),
    ],
)
async def test_client_send_request_retryable_errors(
    status: int, dynamo_error: str, aiodynamo_error: Type[Exception]
) -> None:
    async def http(request: Request) -> Response:
        return Response(
            status=status,
            body=bjson({"__type": f"com.amazonaws.dynamodb.v20120810#{dynamo_error}"}),
        )

    client = Client(
        http,
        StaticCredentials(Key("a", "b")),
        "test",
        throttle_config=StaticDelayRetry(delay=0.01, time_limit_secs=0),
    )

    with pytest.raises(aiodynamo_error):
        await client.count("test", HashKey("key", "value"))
