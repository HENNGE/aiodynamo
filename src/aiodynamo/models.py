from enum import auto, Enum
from functools import wraps
from typing import Type, Union, List, Any

import attr

from .types import TModel, DynamoValue, DynamoObject, EncodedObject
from .exceptions import InvalidModel, InvalidKey
from .helpers import remove_empty_strings, deserialize

NULL = object()
TKeyType = Union[Type[str], Type[int], Type[bytes]]


__all__ = [
    'Keys',
    'model',
    'field',
    'hash_key',
    'range_key'
]


class Keys(Enum):
    """
    Specify which key schema your table uses
    """
    HashRange = auto()
    Hash = auto()


class Meta(Enum):
    """
    Internal enum for attrs metadata keys
    """
    constant = auto()
    field_type = auto()
    key_type = auto()
    alias = auto()
    auto = auto()


class FieldTypes(Enum):
    """
    Internal enum to tag fields
    """
    normal = auto()
    hash = auto()
    range = auto()


def get_type(key_type: Union[Type[str], Type[int], Type[bytes]]) -> str:
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
        raise InvalidModel(f'Invalid key type {key_type}')


def name(field: attr.Attribute) -> str:
    """
    Return field name in the database
    """
    alias = field.metadata.get(Meta.alias, NULL)
    if alias is NULL:
        return field.name
    else:
        return alias


def convert(field: attr.Attribute, value: Any) -> DynamoValue:
    """
    Possibly convert the value of a field
    """
    if field.convert:
        return field.convert(value)
    else:
        return value


def gather_filter(attr, value):
    return (
        attr.metadata.get(Meta.field_type, NULL) != NULL
        and
        value != ''
        and
        value != set()
    )


@attr.s(frozen=True)
class HashEncoder:
    hash_field = attr.ib()

    def build(self, **kwargs):
        """
        Build a key from keyword arguments. This is used in `Key` arguments in
        dynamo APIs
        """
        try:
            value = kwargs.pop(self.hash_field.name)
        except KeyError:
            raise InvalidKey('Hash key not provided')
        if kwargs:
            raise InvalidKey('Too many values provided')
        return {
            name(self.hash_field): convert(self.hash_field, value),
        }

    def build_hash(self, **kwargs):
        """
        Build only the hash key from keyword arguments. This is used for 
        filters.
        """
        try:
            value = kwargs.pop(self.hash_field.name)
        except KeyError:
            raise InvalidKey('Hash key not provided')
        if kwargs:
            raise InvalidKey('Too many values provided')
        return name(self.hash_field), convert(self.hash_field, value)

    def build_range_key(self, value):
        raise InvalidKey('Model has no range key')

    def pop(self, data):
        """
        Pop the key from a dictionary of data.
        """
        try:
            return {
                name(self.hash_field): data.pop(self.hash_field.name),
            }
        except KeyError:
            raise InvalidKey('Hash key not provided')

    def pop_hash(self, kwargs):
        try:
            return {
                name(self.hash_field): kwargs.pop(self.hash_field.name),
            }
        except KeyError:
            raise InvalidKey('Hash key not provided')

    def schema(self):
        """
        Return the key schema, useful for creating the table.
        """
        return [{
            'AttributeName': name(self.hash_field),
            'KeyType': 'HASH',
        }]

    def attributes(self):
        """
        Return the key attributes, useful for creating the table.
        """
        return [{
            'AttributeName': name(self.hash_field),
            'AttributeType': get_type(self.hash_field.metadata[Meta.key_type]),
        }]


@attr.s(frozen=True)
class HashRangeEncoder(HashEncoder):
    """
    Encoder object for Hash/Range key type models
    """
    range_field = attr.ib()

    def build(self, **kwargs) -> DynamoObject:
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
            name(self.hash_field): convert(self.hash_field, hash_key),
            name(self.range_field): convert(self.range_field, range_key),
        }

    def build_range_key(self, value):
        return {
            name(self.range_field): convert(self.range_field, value)
        }

    def pop(self, data: DynamoObject) -> DynamoObject:
        try:
            return {
                **super().pop(data),
                name(self.range_field): data.pop(self.range_field.name),
            }
        except KeyError:
            raise InvalidKey('Range key not provided')

    def schema(self):
        return super().schema() + [{
            'AttributeName': name(self.range_field),
            'KeyType': 'RANGE',
        }]

    def attributes(self):
        return super().attributes() + [{
            'AttributeName': name(self.range_field),
            'AttributeType': get_type(self.range_field.metadata[Meta.key_type]),
        }]


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


def get_anti_aliases(fields):
    anti_aliases = {}
    for field in fields:
        alias = field.metadata.get(Meta.alias, NULL)
        if alias is NULL:
            anti_aliases[field.name] = field.name
        else:
            anti_aliases[alias] = field.name
    return anti_aliases


@attr.s(frozen=True)
class ModelConfig:
    model = attr.ib()
    fields = attr.ib()
    anti_aliases = attr.ib()
    hash_field = attr.ib()
    range_field = attr.ib()
    key_encoder = attr.ib()

    @classmethod
    def get(cls, instance_or_model: Union[TModel, Type[TModel]]) -> 'ModelConfig':
        return instance_or_model.__aiodynamodb__

    @classmethod
    def from_model(cls, keys: Keys, model: Type[TModel]) -> 'ModelConfig':
        """
        Build model config from a model class.
        """

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
            model=model,
            hash_field=hash_field,
            range_field=range_field,
            key_encoder=key_encoder,
            anti_aliases=get_anti_aliases(fields),
            fields={field.name: field for field in fields},
        )

    def build_hash_key(self, **kwargs):
        return self.key_encoder.build_hash(**kwargs)

    def pop_hash_key(self, kwargs):
        return self.key_encoder.pop_hash(kwargs)

    def build_range_key(self, value):
        return self.key_encoder.build_range_key(value)

    def build_key(self, **kwargs):
        return self.key_encoder.build(**kwargs)

    def pop_key(self, data: DynamoObject) -> DynamoObject:
        return self.key_encoder.pop(data)

    def key_schema(self):
        return self.key_encoder.schema()

    def key_attributes(self):
        return self.key_encoder.attributes()

    def alias(self, data: DynamoObject) -> DynamoObject:
        """
        Alias python keys to database field names
        """
        return {
            name(self.fields[key]): value for key, value in data.items()
        }

    def anti_alias(self, data: DynamoObject) -> DynamoObject:
        """
        Unalias database field names to python keys
        """
        return {
            self.anti_aliases[key]: value for key, value in data.items()
        }

    def from_database(self, raw_item: EncodedObject) -> TModel:
        """
        Convert data from the database to a model instance
        """
        decoded = deserialize(raw_item)
        data = self.anti_alias(decoded)
        for key, field in self.fields.items():
            try:
                data[key] = field.convert.from_db(data[key])
            except (AttributeError, KeyError):
                pass
        return self.model(**data)

    def gather(self, instance: TModel) -> DynamoObject:
        """
        Encode model instance to dynamodb object.
        """
        data = attr.asdict(
            instance,
            filter=gather_filter,
            retain_collection_types=True,
        )
        for key, field in self.fields.items():
            auto_gen = field.metadata.get(Meta.auto, NULL)
            if auto_gen is not NULL:
                data[key] = auto_gen()
        return data

    def model_init_factory(self, real_init):
        """
        Builds an __init__ function for the model which sets constant hash keys.
        """
        @wraps(real_init)
        def wrapper(this, **kwargs):
            constant = self.hash_field.metadata.get(Meta.constant, NULL)
            if constant is not NULL:
                kwargs[self.hash_field.name] = constant
            return real_init(this, **kwargs)
        return wrapper


def modify(self, **updates):
    """
    Like attr.assoc, but remembers original instance for diffing.
    """
    try:
        old = self.__aiodynamodb_old__
    except AttributeError:
        old = self

    new = attr.assoc(self, **updates)
    object.__setattr__(new, '__aiodynamodb_old__', old)
    return new


def field(*, alias=NULL, auto=NULL, default=attr.NOTHING, convert=None, **kwargs):
    """
    Define a model field.
    
    Use `alias` if you want the python keys and dynamo field names to differ.
    `auto` can be used to set an automatic value when saving the instance (good
    for modification timestamps).
    """
    if callable(default) and not isinstance(default, attr.Factory):
        default = attr.Factory(default)
    if convert is None:
        convert = remove_empty_strings
    else:
        convert = lambda x: remove_empty_strings(convert(x))
    return attr.ib(metadata={
        Meta.field_type: FieldTypes.normal,
        Meta.alias: alias,
        Meta.auto: auto
    }, default=default, convert=convert, **kwargs)


def hash_key(key_type: TKeyType, *, constant=NULL, alias=NULL, **kwargs):
    """
    Define the hash key field for your model. 
    
    `key_type` must be `str`, `int` or `bytes` depending on the key type you
    want to use. When used together with a range key, you can set `constant` to 
    a constant value.
    """
    return attr.ib(metadata={
        Meta.constant: constant,
        Meta.key_type: key_type,
        Meta.field_type: FieldTypes.hash,
        Meta.alias: alias
    }, **kwargs)


def range_key(key_type: TKeyType, *, alias=NULL, **kwargs):
    """
    Define the range key field for your model (if any).
    
    `key_type` must be `str`, `int` or `bytes` depending on the key type you
    want to use.
    """
    return attr.ib(metadata={
        Meta.key_type: key_type,
        Meta.field_type: FieldTypes.range,
        Meta.alias: alias
    }, **kwargs)


def model(*, keys: Keys):
    """
    Register a class as a model.
    """
    def deco(cls: Type[TModel]) -> Type[TModel]:
        cls = attr.s(frozen=True)(cls)
        cls.__aiodynamodb__ = config = ModelConfig.from_model(keys, cls)
        cls.__init__ = config.model_init_factory(cls.__init__)
        cls.modify = modify
        return cls
    return deco
