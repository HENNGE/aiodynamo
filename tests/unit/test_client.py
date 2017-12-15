import pytest

from aiodynamo import client
from aiodynamo.client import F, remove_empty_strings


@pytest.mark.parametrize('pe,expression,names', [
    (F('foo') & F('bar'), '#N0,#N1', {'#N0': 'foo', '#N1': 'bar'}),
    (F('foo', 0, 'bar') & F('bar'), '#N0[0].#N1,#N1', {'#N0': 'foo', '#N1': 'bar'}),
    (F('foo', '12', 'bar') & F('bar'), '#N0.#N1.#N2,#N2', {'#N0': 'foo', '#N1': '12', '#N2': 'bar'}),
])
def test_project(pe, expression, names):
    assert pe.encode() == (expression, names)


def test_clean():
    assert client.clean(
        foo='bar',
        none=None,
        list=[],
        tuple=(),
        dict={},
        int=0,
    ) == {'foo': 'bar'}


def test_binary_decode():
    assert client.dy2py({
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
