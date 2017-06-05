import pytest

from aiodynamo.exceptions import InvalidModel
from aiodynamo.models import get_key_type, Model, register, ConstKey, Key, field


@pytest.mark.parametrize('test_input,expected', [
    (str, 'S'),
    (int, 'N'),
    (bytes, 'B'),
])
def test_get_key_type(test_input, expected):
    assert get_key_type(test_input) == expected


def test_invalid_key_type():
    with pytest.raises(InvalidModel):
        get_key_type(list)


def test_register_no_keys():
    with pytest.raises(TypeError):
        register(hash_key=Model())


def test_register_const_range_key():
    with pytest.raises(TypeError):
        register(hash_key=Key('h', str), range_key=ConstKey('r', str, 'const'))


def test_register_range_key_type():
    with pytest.raises(TypeError):
        register(hash_key=Key('h', str), range_key=Model())


def test_register_non_model():
    with pytest.raises(TypeError):
        @register(hash_key=Key('h', str))
        class NonModel:
            pass


def test_register_missing_hash_key():
    with pytest.raises(ValueError):
        @register(hash_key=Key('h', str))
        class Broken(Model):
            r = field('')


def test_register_missing_range_key():
    with pytest.raises(ValueError):
        @register(hash_key=Key('h', str), range_key=Key('r', str))
        class Broken(Model):
            h = field('')


def test_register_const_field_diff_value():
    with pytest.raises(ValueError):
        @register(hash_key=ConstKey('h', str, 'BAR'), range_key=Key('r', str))
        class Broken(Model):
            h = field('FOO')
            r = field('')
