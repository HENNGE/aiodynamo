import pytest

from aiodynamo import client


@pytest.mark.parametrize('args,expression,names', [
    (('foo', 'bar'), '#N0,#N1', {'#N0': 'foo', '#N1': 'bar'}),
    ((client.attribute('foo')[0].bar, 'bar'), '#N0[0].#N1,#N1', {'#N0': 'foo', '#N1': 'bar'}),
    ((client.attribute('foo')['12'].bar, 'bar'), '#N0.#N1.#N2,#N2', {'#N0': 'foo', '#N1': '12', '#N2': 'bar'}),
])
def test_project(args, expression, names):
    assert client.project(*args).encode() == (expression, names)


def test_clean():
    assert client.clean(
        foo='bar',
        none=None,
        list=[],
        tuple=(),
        dict={},
        int=0,
    ) == {'foo': 'bar'}
