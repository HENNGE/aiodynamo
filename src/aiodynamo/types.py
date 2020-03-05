from typing import Any, Callable, Dict, List, TypeVar, Union

from boto3.dynamodb.types import TypeSerializer

Numeric = Union[float, int]

Item = TypeVar("Item", bound=Dict[str, Any])
DynamoItem = TypeVar("DynamoItem", bound=Dict[str, Dict[str, Any]])
TableName = TypeVar("TableName", bound=str)
KeyPath = List[Union[str, int]]
PathEncoder = Callable[[KeyPath], str]
EncoderFunc = Callable[[Any], str]
NOTHING = object()
EMPTY = object()


Serializer = TypeSerializer()


SIMPLE_TYPES = frozenset({"BOOL", "S", "B"})
SIMPLE_SET_TYPES = frozenset({"SS", "BS"})
NULL_TYPE = "NULL"
