from typing import TypeVar, Union

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
