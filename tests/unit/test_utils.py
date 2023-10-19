import base64
from decimal import Decimal
from functools import partial
from typing import Any, Callable, Dict

import pytest
from boto3.dynamodb.types import (  # type: ignore[import-untyped]
    DYNAMODB_CONTEXT,
    TypeDeserializer,
)

from aiodynamo.types import NumericTypeConverter
from aiodynamo.utils import deserialize, dy2py


def test_binary_decode() -> None:
    assert dy2py({"test": {"B": base64.b64encode(b"hello")}}, float) == {
        "test": b"hello"
    }


@pytest.mark.parametrize(
    "value,numeric_type,result",
    [
        (
            {
                "N": "1.2",
            },
            float,
            1.2,
        ),
        ({"NS": ["1.2"]}, float, {1.2}),
        ({"N": "1.2"}, DYNAMODB_CONTEXT.create_decimal, Decimal("1.2")),
        ({"NS": ["1.2"]}, DYNAMODB_CONTEXT.create_decimal, {Decimal("1.2")}),
    ],
)
def test_numeric_decode(
    value: Dict[str, Any], numeric_type: NumericTypeConverter, result: Any
) -> None:
    assert deserialize(value, numeric_type) == result


def test_serde_compatibility() -> None:
    def generate_item(nest: bool) -> Dict[str, Any]:
        item = {
            "hash": {
                "S": "string",
            },
            "range": {
                "B": base64.b64encode(b"bytes"),
            },
            "null": {"NULL": True},
            "true": {"BOOL": True},
            "false": {"BOOL": False},
            "int": {"N": "42"},
            "float": {"N": "4.2"},
            "numeric_set": {"NS": ["42", "4.2"]},
            "string_set": {"SS": ["hello", "world"]},
            "binary_set": {
                "BS": [base64.b64encode(b"hello"), base64.b64encode(b"world")]
            },
        }
        if nest:
            item["list"] = {"L": [{"M": generate_item(False)}]}
        return item

    item = generate_item(True)

    class BinaryDeserializer(TypeDeserializer):  # type: ignore[misc]
        def _deserialize_b(self, value: Any) -> bytes:
            return base64.b64decode(value)

    def deserialize_item(
        item: Dict[str, Any], deserializer: Callable[[Any], Any]
    ) -> Dict[str, Any]:
        return {k: deserializer(v) for k, v in item.items()}

    fast = deserialize_item(
        item, partial(deserialize, numeric_type=DYNAMODB_CONTEXT.create_decimal)
    )
    boto = deserialize_item(item, BinaryDeserializer().deserialize)
    assert fast == boto
