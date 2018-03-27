import pytest

from aiodynamo import utils
from aiodynamo.utils import clean, dy2py, remove_empty_strings


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_unroll_with_limit():
    async def func(**kwargs):
        if 'InKey' in kwargs:
            return {
                'OutKey': 'Bar',
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


def test_clean():
    assert clean(
        foo='bar',
        none=None,
        list=[],
        tuple=(),
        dict={},
        int=0,
        bool=False,
    ) == {'foo': 'bar', 'bool': False}


def test_binary_decode():
    assert dy2py({
        'test': {
            'B': b'hello'
        }
    }) == {
        'test': b'hello'
    }


@pytest.mark.parametrize('item,result', [
    ({'foo': ''}, {}),
    ({'foo': {'bar': '', 'baz': 1}}, {'foo': {'baz': 1}})
])
def test_remove_empty_strings(item, result):
    assert dict(remove_empty_strings(item)) == result
