import base64
import datetime
import decimal
from collections import abc as collections_abc
from functools import reduce
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Union

from .types import SIMPLE_TYPES, AttributeType, DynamoItem, Item


def py2dy(data: Union[Item, None]) -> Union[DynamoItem, None]:
    if data is None:
        return data

    return serialize_dict(data)


def dy2py(data: DynamoItem, numeric_type: Callable[[str], Any]) -> Item:
    return {key: deserialize(value, numeric_type) for key, value in data.items()}


def maybe_immutable(thing: Any):
    if isinstance(thing, list):
        return tuple(thing)

    elif isinstance(thing, set):
        return frozenset(thing)

    elif isinstance(thing, dict):
        return frozenset((key, value) for key, value in thing.items())

    else:
        return thing


def deserialize(value: Dict[str, Any], numeric_type: Callable[[str], Any]) -> Any:
    if not value:
        raise TypeError(
            "Value must be a nonempty dictionary whose key " "is a valid dynamodb type."
        )
    tag, val = next(iter(value.items()))
    try:
        attr_type = AttributeType(tag)
    except ValueError:
        raise TypeError(f"Dynamodb type {tag} is not supported")
    if attr_type in SIMPLE_TYPES:
        return val
    if attr_type is AttributeType.null:
        return None
    if attr_type is AttributeType.binary:
        return base64.b64decode(val)
    if attr_type is AttributeType.number:
        return numeric_type(val)
    if attr_type is AttributeType.string_set:
        return set(val)
    if attr_type is AttributeType.binary_set:
        return {base64.b64decode(v) for v in val}
    if attr_type is AttributeType.number_set:
        return {numeric_type(v) for v in val}
    if attr_type is AttributeType.list:
        return [deserialize(v, numeric_type) for v in val]
    if attr_type is AttributeType.map:
        return {k: deserialize(v, numeric_type) for k, v in val.items()}
    raise TypeError(f"Dynamodb type {attr_type} is not supported")


NUMERIC_TYPES = int, float, decimal.Decimal


def serialize(value: Any) -> Optional[Dict[str, Any]]:
    """
    Serialize a Python value to a Dynamo Value, removing empty strings.
    """
    tag_and_value = low_level_serialize(value)
    if tag_and_value is not None:
        return {tag_and_value[0]: tag_and_value[1]}
    return None


def low_level_serialize(value: Any) -> Optional[Tuple[str, Any]]:
    if value is None:
        return "NULL", True
    elif isinstance(value, bool):
        return "BOOL", value
    elif isinstance(value, NUMERIC_TYPES):
        return "N", str(value)
    elif isinstance(value, str):
        if not value:
            return None
        return "S", value
    elif isinstance(value, bytes):
        if not value:
            return None
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
            return "SS", [item for item in value if item]
        elif byte_items == total:
            return (
                "BS",
                [base64.b64encode(item).decode("ascii") for item in value if item],
            )
        else:
            raise TypeError(
                f"Sets which are not entirely numeric, strings or bytes are not supported. value: {value!r}"
            )
    elif isinstance(value, collections_abc.Mapping):
        return "M", serialize_dict(value)
    elif isinstance(value, collections_abc.Sequence):
        return "L", [item for item in map(serialize, value) if item is not None]
    else:
        raise TypeError(f"Unsupported type {type(value)}: {value!r}")


def serialize_dict(value: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        result_key: result_value
        for result_key, result_value in (
            (inner_key, serialize(inner_value))
            for inner_key, inner_value in value.items()
        )
        if result_value is not None
    }


def parse_amazon_timestamp(timestamp: str) -> datetime.datetime:
    return datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc
    )
