from typing import TypeVar, Type, Tuple, Dict, Union, Iterator

import attr
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from .types import DynamoObject, EncodedObject, TModel, DynamoValue

TDiff = TypeVar('Diff')

EMPTY_STRING = TypeVar('EMPTY_STRING')

Serializer = TypeSerializer()
Deserializer = TypeDeserializer()


def serialize(data: DynamoObject) -> EncodedObject:
    return {
        key: Serializer.serialize(value) for key, value in remove_empty_strings(data).items()
    }


def deserialize(data: EncodedObject) -> DynamoObject:
    return {
        key: Deserializer.deserialize(value) for key, value in data.items()
    }


def get_diff(cls: Type[TModel], new: DynamoObject, old: DynamoObject) -> TDiff:
    diff: TDiff = {}
    for field in attr.fields(cls):
        new_value = new[field.name]
        old_value = old[field.name]
        if new_value != old_value:
            diff[field.name] = new_value
    return diff


class DynamoEncoder:
    prefix = None

    def __init__(self):
        self.values = {}
        self.index = 0

    def encode(self, value):
        if value not in self.values:
            self.values[value] = f'{self.prefix}{self.index}'
            self.index += 1
        return self.values[value]

    def gather(self):
        return {value: key for key, value in self.values.items()}


class NameEncoder(DynamoEncoder):
    prefix = '#n'


class ValueEncoder(DynamoEncoder):
    prefix = ':v'

    def gather(self):
        return serialize(super().gather())


def encode_update_expression(data: TDiff) -> Tuple[str, Dict[str, str], EncodedObject]:
    """
    Encode a diff into an UpdateExpression, ExpressionAttributeNames and 
    ExpressionAttributeValues.
    
    Currently only does SET but could be expanded with a smarter diff to do
    cool things.
    """
    ue_bits = []
    ean = NameEncoder()
    eav = ValueEncoder()
    for key, value in data.items():
        key = ean.encode(key)
        value = eav.encode(value)
        ue_bits.append(f'{key} = {value}')
    joined = ', '.join(ue_bits)
    return f'SET {joined}', ean.gather(), eav.gather()


def boto_err(exc: ClientError, code: str) -> bool:
    """
    Check if a boto error is a certain error.
    """
    try:
        return exc.response['Error']['Code'] == code
    except KeyError:
        return False


def _remove_empty_strings(value: DynamoValue) -> Union[DynamoValue, EMPTY_STRING]:
    if value == '':
        return EMPTY_STRING
    elif isinstance(value, dict):
        clean = {}
        for key, attr_value in value.items():
            clean_value = _remove_empty_strings(attr_value)
            if clean_value is not EMPTY_STRING:
                clean[key] = clean_value
        return clean
    elif isinstance(value, list):
        clean = []
        for item in value:
            clean_item = _remove_empty_strings(item)
            if clean_item is not EMPTY_STRING:
                clean.append(clean_item)
        return clean
    elif isinstance(value, set):
        clean = set()
        for item in value:
            clean_item = _remove_empty_strings(item)
            if clean_item is not EMPTY_STRING:
                clean.add(clean_item)
        return clean
    else:
        return value


def remove_empty_strings(value: DynamoValue) -> DynamoValue:
    clean = _remove_empty_strings(value)
    if clean is EMPTY_STRING:
        return ''
    else:
        return clean


@attr.s
class Tracker:
    prefix = attr.ib()
    data = attr.ib(default=attr.Factory(dict))
    index = attr.ib(default=0)

    def track(self, thing):
        if thing not in self.data:
            self.data[thing] = f'{self.prefix}{self.index}'
            self.index += 1
        return self.data[thing]

    def collect(self):
        return {value: key for key, value in self.data.items()}


class Substitutes:
    def __init__(self):
        self.values = Tracker(':v')
        self.names = Tracker('#n')

    def name(self, name):
        return self.names.track(name)

    def value(self, value):
        return self.values.track(value)

    def get_names(self):
        return self.names.collect()

    def get_values(self):
        return self.values.collect()
