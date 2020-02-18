from decimal import Decimal

import pytest
from aiodynamo import utils
from aiodynamo.utils import clean, deserialize, dy2py, remove_empty_strings
from boto3.dynamodb.types import DYNAMODB_CONTEXT


@pytest.mark.asyncio
async def test_unroll():
    async def func(**kwargs):
        if "InKey" in kwargs:
            return {"Items": [1, 2, 3]}

        else:
            return {"OutKey": "Foo", "Items": ["a", "b", "c"]}

    result = [item async for item in utils.unroll(func, "InKey", "OutKey", "Items")]
    assert result == ["a", "b", "c", 1, 2, 3]


@pytest.mark.asyncio
async def test_unroll_with_limit():
    async def func(**kwargs):
        if "InKey" in kwargs:
            return {"OutKey": "Bar", "Items": [1, 2, 3]}

        else:
            return {"OutKey": "Foo", "Items": ["a", "b", "c"]}

    result = [
        item
        async for item in utils.unroll(
            func, "InKey", "OutKey", "Items", limit=4, limitkey="Limit"
        )
    ]
    assert result == ["a", "b", "c", 1]


def test_clean():
    assert clean(
        foo="bar", none=None, list=[], tuple=(), dict={}, int=0, bool=False
    ) == {"foo": "bar", "bool": False}


def test_binary_decode():
    assert dy2py({"test": {"B": b"hello"}}, float) == {"test": b"hello"}


@pytest.mark.parametrize(
    "value,numeric_type,result",
    [
        ({"N": "1.2",}, float, 1.2),
        ({"NS": ["1.2"]}, float, {1.2}),
        ({"N": "1.2"}, DYNAMODB_CONTEXT.create_decimal, Decimal("1.2")),
        ({"NS": ["1.2"]}, DYNAMODB_CONTEXT.create_decimal, {Decimal("1.2")}),
    ],
)
def test_numeric_decode(value, numeric_type, result):
    assert deserialize(value, numeric_type) == result


@pytest.mark.parametrize(
    "item,result",
    [({"foo": ""}, {}), ({"foo": {"bar": "", "baz": 1}}, {"foo": {"baz": 1}})],
)
def test_remove_empty_strings(item, result):
    assert dict(remove_empty_strings(item)) == result
