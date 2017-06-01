from typing import Dict, List, Iterator, Tuple, Optional, Type, Union, Callable

import attr

from aiodynamo.converters import Converter, PassthroughConverter
from aiodynamo.types import DynamoKeyType, DynamoValue
from . import exceptions, constants


def get_key_type(key_type: DynamoKeyType) -> str:
    """
    Convert key type from Python builtin to dynamo code
    """
    if key_type is str:
        return 'S'
    elif key_type is int:
        return 'N'
    elif key_type is bytes:
        return 'B'
    else:
        raise exceptions.InvalidModel(f'Invalid key type {key_type}')


class Model:
    aiodynamodb_original_instance = constants.FRESH

    def modify(self, **attrs) -> 'Model':
        new = attr.evolve(self, **attrs)
        new.aiodynamodb_original_instance = self.aiodynamodb_original_instance
        return new


def iterencode(instance: Model, fields: Dict[str, 'Field'], keys: List[str]) -> Iterator[Tuple[str, DynamoValue]]:
    original = getattr(instance, constants.ORIGINAL_INSTANCE_ATTR_NAME)
    for name, field in fields.items():
        value = getattr(instance, name)
        if field.convert:
            value = field.convert.py_to_db(value)
        # ignore fields that are the default value
        is_key = name in keys
        is_default = value == field.default
        is_same = original is not constants.FRESH and value == getattr(original, name)
        if is_key or not is_default or not is_same:
            yield name, value


@attr.s
class Config:
    hash_key: 'Key' = attr.ib()
    range_key: Optional['Key'] = attr.ib()
    fields: Dict[str, 'Field'] = attr.ib()
    model: Type[Model] = attr.ib()
    key_schema: List[Dict[str, str]] = attr.ib(init=False)
    key_attributes: List[Dict[str, str]] = attr.ib(init=False)
    key_names: List[str] = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.key_schema: List[Dict[str, str]] = [{
            'AttributeName': self.hash_key.name,
            'KeyType': 'HASH'
        }]
        self.key_attributes: List[Dict[str, str]] = [{
            'AttributeName': self.hash_key.name,
            'AttributeType': get_key_type(self.hash_key.type)
        }]
        self.key_names: List[str] = [self.hash_key.name]
        if self.range_key:
            self.key_schema.append({
                'AttributeName': self.range_key.name,
                'KeyType': 'RANGE'
            })
            self.key_attributes.append({
                'AttributeName': self.range_key.name,
                'AttributeType': get_key_type(self.range_key.type)
            })
            self.key_names.append(self.range_key.name)

    def from_database(self, data: Dict[str, DynamoValue]) -> Model:
        cleaned = {
            key: value
            for
            key, value
            in
            data.items()
            if
            key in self.fields
        }
        instance = self.model(**cleaned)
        setattr(instance, constants.ORIGINAL_INSTANCE_ATTR_NAME, instance)
        return instance

    def encode(self, instance: Model) -> Dict[str, DynamoValue]:
        return dict(
            iterencode(
                instance,
                self.fields,
                self.key_names
            )
        )

    def encode_key(self, value1=constants.NOTHING, value2=constants.NOTHING) -> Dict[str, DynamoValue]:
        """
        This method encodes the key of an item to a dictionary. Because tables
        could have only a hash key, both a hash key and an optional range key or
        a constant hash key with an optional range key, this method is a bit 
        complicated.

        For Hash-Key-Only-Tables:
            If the Hash Key is constant, no value may be provided.
            If the Hash Key is normal, a single value must be given.
        For Hash-Range-Tables:
            If the Hash Key is constant, zero (scan) or one (lookup via Range Key)
            values may be provided.
            If the Hash Key is normal, one (scan) or two (lookup via Range Key)
            values may be provided.
        """
        const_hash_key = isinstance(self.hash_key, ConstKey)
        if value1 is constants.NOTHING and value2 is constants.NOTHING:
            if const_hash_key:
                return {
                    self.hash_key.name: self.hash_key.value
                }
            else:
                raise ValueError('No key value provided')
        elif value2 is constants.NOTHING:
            if const_hash_key:
                if self.range_key:
                    return {
                        self.hash_key.name: self.hash_key.value,
                        self.range_key.name: value1
                    }
                else:
                    raise ValueError(
                        'Cannot provide range key value without range key')
            else:
                return {
                    self.hash_key.name: value1
                }
        else:
            if self.range_key:
                return {
                    self.hash_key.name: value1,
                    self.range_key.name: value2,
                }
            else:
                raise ValueError(
                    'Cannot provide range key value without range key')


@attr.s
class Field:
    default: DynamoValue = attr.ib()
    convert: Optional[Converter] = attr.ib(default=None)


def field(default: Union[DynamoValue, Callable[[], DynamoValue]],
          convert: Optional[Converter] = None, repr: bool = True):
    if convert is None:
        convert = PassthroughConverter
    if callable(default):
        default = attr.Factory(default)
    return attr.ib(
        default=default,
        convert=convert.db_to_py,
        repr=repr,
        metadata={
            constants.AIODYNAMO_META: Field(default, convert)
        }
    )


@attr.s
class Key:
    name = attr.ib()
    type = attr.ib()


@attr.s
class ConstKey(Key):
    value = attr.ib()


_registry: Dict[Type[Model], Config] = {}


def get_config(cls: Type[Model]) -> Config:
    return _registry[cls]


def register(*, hash_key: Key, range_key: Optional[Key] = None) -> Callable[
    [Type[Model]], Type[Model]]:
    if not isinstance(hash_key, Key):
        raise TypeError('Hash key must be a Key')
    if range_key:
        if isinstance(range_key, ConstKey):
            raise TypeError('Range key cannot be ConstKey')
        if not isinstance(range_key, Key):
            raise TypeError('Range key must be a Key')

    def decorator(cls: Type[Model]) -> Type[Model]:
        if not issubclass(cls, Model):
            raise TypeError('Can only register Model subclasses')
        cls = attr.s(cls)

        fields: Dict[str, Field] = {
            attribute.name: attribute.metadata[constants.AIODYNAMO_META]
            for
            attribute
            in
            attr.fields(cls)
            if
            constants.AIODYNAMO_META in attribute.metadata
        }
        if hash_key.name not in fields:
            raise ValueError(f'Cannot find hash key {hash_key.name} in fields')
        if range_key and range_key.name not in fields:
            raise ValueError(
                f'Cannot find range key {range_key.name} in fields')
        if constants.ORIGINAL_INSTANCE_ATTR_NAME in fields:
            raise ValueError(
                f'Cannot have field named {constants.ORIGINAL_INSTANCE_ATTR_NAME}')
        if isinstance(hash_key, ConstKey):
            if hash_key.value != fields[hash_key.name].default:
                raise ValueError(
                    'Constant key fields must have a default value equal to the constant value')
        _registry[cls] = Config(hash_key, range_key, fields, cls)
        return cls

    return decorator


