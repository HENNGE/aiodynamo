from botocore.exceptions import ClientError
from typing import TypeVar, Type, Tuple, Dict, Union

import attr
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from .models import ModelConfig
from .types import DynamoObject, EncodedObject, TModel


TDiff = TypeVar('Diff')

Serializer = TypeSerializer()
Deserializer = TypeDeserializer()


def serialize(data: DynamoObject) -> EncodedObject:
    return {
        key: Serializer.serialize(value) for key, value in data.items()
    }


def deserialize(data: EncodedObject) -> DynamoObject:
    return {
        key: Deserializer.deserialize(value) for key, value in data.items()
    }


def get_config(instance: Union[TModel, Type[TModel]]) -> ModelConfig:
    return instance.__aiodynamodb__


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
    try:
        return exc.response['Error']['Code'] == code
    except KeyError:
        return False
