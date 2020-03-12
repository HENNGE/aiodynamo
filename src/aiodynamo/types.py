import decimal
from enum import Enum
from typing import Any, Callable, Dict, Sequence, TypeVar, Union

Timeout = Union[float, int]
Numeric = Union[float, int, decimal.Decimal]

Item = TypeVar("Item", bound=Dict[str, Any])
DynamoItem = TypeVar("DynamoItem", bound=Dict[str, Dict[str, Any]])
TableName = TypeVar("TableName", bound=str)
KeyPath = Sequence[Union[str, int]]
PathEncoder = Callable[[KeyPath], str]
EncoderFunc = Callable[[Any], str]
NOTHING = object()


class AttributeType(Enum):
    string = "S"
    string_set = "SS"
    number = "N"
    number_set = "NS"
    binary = "B"
    binary_set = "BS"
    boolean = "BOOL"
    null = "NULL"
    list = "L"
    map = "M"


SIMPLE_TYPES = frozenset({AttributeType.boolean, AttributeType.string})
