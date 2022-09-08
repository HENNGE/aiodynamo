import decimal
from enum import Enum
from typing import Any, Callable, Dict, List, Union

from ._compat import TypedDict

Timeout = Union[float, int]
Numeric = Union[float, int, decimal.Decimal]

Item = Dict[str, Any]
DynamoItem = Dict[str, Dict[str, Any]]
TableName = str


class ParametersDict(TypedDict, total=False):
    ExpressionAttributeNames: Dict[str, str]
    ExpressionAttributeValues: Dict[str, Dict[str, Any]]


NOTHING = object()

Seconds = Union[float, int]


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


class EncodedThroughputData(TypedDict):
    ReadCapacityUnits: int
    WriteCapacityUnits: int


class EncodedThroughput(TypedDict):
    ProvisionedThroughput: EncodedThroughputData


class EncodedPayPerRequest(TypedDict):
    BillingMode: str


class EncodedKeySchema(TypedDict):
    AttributeName: str
    KeyType: str


class EncodedProjectionRequired(TypedDict):
    ProjectionType: str


class EncodedProjection(EncodedProjectionRequired, total=False):
    NonKeyAttributes: List[str]


class EncodedLocalSecondaryIndex(TypedDict):
    IndexName: str
    KeySchema: List[EncodedKeySchema]
    Projection: EncodedProjection


class EncodedGlobalSecondaryIndex(EncodedLocalSecondaryIndex):
    ProvisionedThroughput: EncodedThroughput


class EncodedStreamSpecificationRequired(TypedDict):
    StreamEnabled: bool


class EncodedStreamSpecification(EncodedStreamSpecificationRequired, total=False):
    StreamViewType: str


SIMPLE_TYPES = frozenset({AttributeType.boolean, AttributeType.string})


NumericTypeConverter = Callable[[str], Any]
