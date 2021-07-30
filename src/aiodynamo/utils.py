import base64
import datetime
import decimal
import logging
from collections import abc as collections_abc
from functools import reduce
from typing import Any, Callable, Dict, List, Mapping, Set, Tuple, TypeVar, Union

from .types import DynamoItem, Item, NumericTypeConverter

T = TypeVar("T")


logger = logging.getLogger("aiodynamo")


def py2dy(data: Union[Item, None]) -> Union[DynamoItem, None]:
    if data is None:
        return data

    return serialize_dict(data)


def dy2py(data: DynamoItem, numeric_type: NumericTypeConverter) -> Item:
    return {key: deserialize(value, numeric_type) for key, value in data.items()}


def deserialize_simple_types(val: T, _: NumericTypeConverter) -> T:
    return val


def deserialize_binary(val: str, _: NumericTypeConverter) -> bytes:
    return base64.b64decode(val)


def deserialize_string_set(val: List[str], _: NumericTypeConverter) -> Set[str]:
    return set(val)


def deserialize_binary_set(val: List[str], _: NumericTypeConverter) -> Set[bytes]:
    return {base64.b64decode(v) for v in val}


def deserialize_null(_: None, __: NumericTypeConverter) -> None:
    return None


def deserialize_number(val: str, numeric_type: NumericTypeConverter) -> Any:
    return numeric_type(val)


def deserialize_number_set(
    val: List[str], numeric_type: NumericTypeConverter
) -> Set[T]:
    return {numeric_type(v) for v in val}


def deserialize_list(val: List[Any], numeric_type: NumericTypeConverter) -> List[Any]:
    return [deserialize(v, numeric_type) for v in val]


def deserialize_map(
    val: Dict[str, Any], numeric_type: NumericTypeConverter
) -> Dict[str, Any]:
    return {k: deserialize(v, numeric_type) for k, v in val.items()}


TAG_DESERIALIZE_MAPPING: Dict[str, Callable[[Any, NumericTypeConverter], Any]] = {
    "S": deserialize_simple_types,
    "SS": deserialize_string_set,
    "N": deserialize_number,
    "NS": deserialize_number_set,
    "B": deserialize_binary,
    "BS": deserialize_binary_set,
    "BOOL": deserialize_simple_types,
    "NULL": deserialize_null,
    "L": deserialize_list,
    "M": deserialize_map,
}


def deserialize(value: Dict[str, Any], numeric_type: NumericTypeConverter) -> Any:
    if not value:
        raise TypeError(
            "Value must be a nonempty dictionary whose key " "is a valid dynamodb type."
        )
    tag, val = next(iter(value.items()))
    try:
        return TAG_DESERIALIZE_MAPPING[tag](val, numeric_type)
    except KeyError:
        raise TypeError(f"Dynamodb type {tag} is not supported")


NUMERIC_TYPES = int, float, decimal.Decimal


def serialize(value: Any) -> Dict[str, Any]:
    """
    Serialize a Python value to a Dynamo Value, removing empty strings.
    """
    tag, value = low_level_serialize(value)
    return {tag: value}


def low_level_serialize(value: Any) -> Tuple[str, Any]:
    if value is None:
        return "NULL", True
    elif isinstance(value, bool):
        return "BOOL", value
    elif isinstance(value, NUMERIC_TYPES):
        return "N", str(value)
    elif isinstance(value, str):
        return "S", value
    elif isinstance(value, bytes):
        return "B", base64.b64encode(value).decode("ascii")
    elif isinstance(value, collections_abc.Set):
        numeric_items, str_items, byte_items, total = reduce(
            lambda acc, item: (
                acc[0] + isinstance(item, NUMERIC_TYPES),
                acc[1] + isinstance(item, str),
                acc[2] + isinstance(item, bytes),
                acc[3] + 1,
            ),
            value,
            (0, 0, 0, 0),
        )
        if numeric_items == total:
            return "NS", [str(item) for item in value]
        elif str_items == total:
            return "SS", [item for item in value]
        elif byte_items == total:
            return (
                "BS",
                [base64.b64encode(item).decode("ascii") for item in value],
            )
        else:
            raise TypeError(
                f"Sets which are not entirely numeric, strings or bytes are not supported. value: {value!r}"
            )
    elif isinstance(value, collections_abc.Mapping):
        return "M", serialize_dict(value)
    elif isinstance(value, collections_abc.Sequence):
        return "L", [item for item in map(serialize, value)]
    else:
        raise TypeError(f"Unsupported type {type(value)}: {value!r}")


def serialize_dict(value: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {key: serialize(value) for key, value in value.items()}


def parse_amazon_timestamp(timestamp: str) -> datetime.datetime:
    return datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc
    )
