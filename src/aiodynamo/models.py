from enum import auto, Enum
from functools import wraps

from typing import Type, Union, List

import attr

from .types import TModel
from .exceptions import InvalidModel, InvalidKey

NULL = object()
TKeyType = Union[Type[str], Type[int], Type[bytes]]


class Keys(Enum):
    HashRange = auto()
    Hash = auto()


class Meta(Enum):
    constant = auto()
    field_type = auto()
    key_type = auto()


class FieldTypes(Enum):
    normal = auto()
    hash = auto()
    range = auto()


def get_type(key_type):
    if key_type is str:
        return 'S'
    elif key_type is int:
        return 'N'
    elif key_type is bytes:
        return 'B'
    else:
        raise InvalidModel(f'Invalid key type {key_type}')


@attr.s(frozen=True)
class HashRangeEncoder:
    hash_field = attr.ib()
    range_field = attr.ib()

    def build(self, **kwargs):
        hash_key = kwargs.pop(self.hash_field.name, NULL)
        try:
            range_key = kwargs.pop(self.range_field.name)
        except KeyError:
            raise InvalidKey('Range key not provided')
        if kwargs:
            raise InvalidKey('Too many values provided')
        if hash_key is NULL:
            constant = self.hash_field.metadata.get(Meta.constant, NULL)
            if constant is NULL:
                raise InvalidKey('Hash key not specified')
            else:
                hash_key = constant
        return {
            self.hash_field.name: hash_key,
            self.range_field.name: range_key,
        }

    def build_hash(self, **kwargs):
        try:
            value = kwargs.pop(self.hash_field.name)
        except KeyError:
            raise InvalidKey('Hash key not provided')
        if kwargs:
            raise InvalidKey('Too many values provided')
        return self.hash_field.name, value
    
    def pop(self, data):
        return {
            self.hash_field.name: data.pop(self.hash_field.name),
            self.range_field.name: data.pop(self.range_field.name),
        }

    def schema(self):
        return [
            {
                'AttributeName': self.hash_field.name,
                'KeyType': 'HASH',
            },
            {
                'AttributeName': self.range_field.name,
                'KeyType': 'RANGE',
            },
        ]

    def attributes(self):
        return [
            {
                'AttributeName': self.hash_field.name,
                'AttributeType': get_type(self.hash_field.metadata[Meta.key_type]),
            },
            {
                'AttributeName': self.range_field.name,
                'AttributeType': get_type(self.range_field.metadata[Meta.key_type]),
            }
        ]


@attr.s(frozen=True)
class HashEncoder:
    hash_field = attr.ib()

    def build(self, **kwargs):
        try:
            value = kwargs.pop(self.hash_field.name)
        except KeyError:
            raise InvalidKey('Hash key not provided')
        if kwargs:
            raise InvalidKey('Too many values provided')
        return {
            self.hash_field.name: value,
        }

    def build_hash(self, **kwargs):
        try:
            value = kwargs.pop(self.hash_field.name)
        except KeyError:
            raise InvalidKey('Hash key not provided')
        if kwargs:
            raise InvalidKey('Too many values provided')
        return self.hash_field.name, value

    def pop(self, data):
        return {
            self.hash_field.name: data.pop(self.hash_field.name),
        }

    def schema(self):
        return [
            {
                'AttributeName': self.hash_field.name,
                'KeyType': 'HASH',
            },
        ]

    def attributes(self):
        return [
            {
                'AttributeName': self.hash_field.name,
                'AttributeType': get_type(self.hash_field.metadata[Meta.key_type]),
            },
        ]
    

def extract_hash_range(fields: List[attr.Attribute]):
    hash_field = None
    range_field = None
    for field in fields:
        field_type = field.metadata.get(Meta.field_type, None)
        if field_type is FieldTypes.hash:
            if hash_field is not None:
                raise InvalidModel('Two hash fields specified')
            else:
                hash_field = field
        elif field_type is FieldTypes.range:
            if range_field is not None:
                raise InvalidModel('Two range fields specified')
            else:
                range_field = field
    return hash_field, range_field


@attr.s(frozen=True)
class ModelConfig:
    hash_field = attr.ib()
    range_field = attr.ib()
    key_encoder = attr.ib()

    @classmethod
    def from_model(cls, keys: Keys, model: Type[TModel]) -> 'ModelConfig':
        fields = attr.fields(model)
        hash_field, range_field = extract_hash_range(fields)
        if hash_field is None:
            raise InvalidModel('Hash field not specified')
        if keys is Keys.HashRange:
            if range_field is None:
                raise InvalidModel('Range field not specified')
            key_encoder = HashRangeEncoder(hash_field, range_field)
        elif keys is not Keys.Hash:
            raise InvalidModel(f'Invalid keys type {keys}')
        else:
            key_encoder = HashEncoder(hash_field)
        return cls(
            hash_field=hash_field,
            range_field=range_field,
            key_encoder=key_encoder
        )

    def build_hash_key(self, **kwargs):
        return self.key_encoder.build_hash(**kwargs)

    def build_key(self, **kwargs):
        return self.key_encoder.build(**kwargs)

    def pop_key(self, data):
        return self.key_encoder.pop(data)

    def key_schema(self):
        return self.key_encoder.schema()

    def key_attributes(self):
        return self.key_encoder.attributes()

    def model_init_factory(self, real_init):
        @wraps(real_init)
        def wrapper(this, **kwargs):
            constant = self.hash_field.metadata.get(Meta.constant, NULL)
            if constant is not NULL:
                kwargs[self.hash_field.name] = constant
            return real_init(this,**kwargs)
        return wrapper


def modify(self, **updates):
    try:
        old = self.__aiodynamodb_old__
    except AttributeError:
        old = self

    new = attr.assoc(self, **updates)
    object.__setattr__(new, '__aiodynamodb_old__', old)
    return new


def field(**kwargs):
    return attr.ib(metadata={
        Meta.field_type: FieldTypes.normal
    }, **kwargs)


def hash_key(key_type: TKeyType, *, constant=NULL,  **kwargs):
    return attr.ib(metadata={
        Meta.constant: constant,
        Meta.key_type: key_type,
        Meta.field_type: FieldTypes.hash
    }, **kwargs)


def range_key(key_type: TKeyType, **kwargs):
    return attr.ib(metadata={
        Meta.key_type: key_type,
        Meta.field_type: FieldTypes.range
    }, **kwargs)


def model(*, keys: Keys):
    def deco(cls: Type[TModel]) -> Type[TModel]:
        cls = attr.s(frozen=True)(cls)
        cls.__aiodynamodb__ = config = ModelConfig.from_model(keys, cls)
        cls.__init__ = config.model_init_factory(cls.__init__)
        cls.modify = modify
        return cls
    return deco
