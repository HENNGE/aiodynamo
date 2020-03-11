import decimal
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


SIMPLE_TYPES = frozenset({"BOOL", "S", "B"})
SIMPLE_SET_TYPES = frozenset({"SS", "BS"})
NULL_TYPE = "NULL"
