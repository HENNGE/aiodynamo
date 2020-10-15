import decimal
import sys
from enum import Enum
from typing import Any, Dict, Union

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

Timeout = Union[float, int]
Numeric = Union[float, int, decimal.Decimal]

Item = Dict[str, Any]
DynamoItem = Dict[str, Dict[str, Any]]
TableName = str
ParametersDict = TypedDict(
    "ParametersDict",
    {
        "ExpressionAttributeNames": Dict[str, str],
        "ExpressionAttributeValues": Dict[str, Dict[str, Any]],
    },
    total=False,
)
Timespan = Union[float, int]
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
