import pytest

from aiodynamo import utils

pytestmark =pytest.mark.asyncio


async def test_unroll():
    async def func(**kwargs):
        if 'InKey' in kwargs:
            return {
                'Items': [1, 2, 3],
            }
        else:
            return {
                'OutKey': 'Foo',
                'Items': ['a', 'b', 'c']
            }

    result = [
        item async for item in utils.unroll(func, 'InKey', 'OutKey', 'Items')
    ]
    assert result == ['a', 'b', 'c', 1, 2, 3]


async def test_unroll_with_limit():
    async def func(**kwargs):
        if 'InKey' in kwargs:
            return {
                'Items': [1, 2, 3],
            }
        else:
            return {
                'OutKey': 'Foo',
                'Items': ['a', 'b', 'c']
            }

    result = [
        item async for item in utils.unroll(func, 'InKey', 'OutKey', 'Items', limit=4, limitkey='Limit')
    ]
    assert result == ['a', 'b', 'c', 1]
