import base64
import datetime
import decimal
import logging
from collections import abc as collections_abc
from functools import reduce
from typing import Any, Callable, Dict, List, Mapping, Set, Tuple, TypeVar, Union

import ddbcereal

from .types import DynamoItem, Item, NumericTypeConverter

T = TypeVar("T")


logger = logging.getLogger("aiodynamo")

_serializer = ddbcereal.Serializer(
    allow_inexact=True, validate_numbers=False, raw_transport=True
)
serialize_dict = _serializer.serialize_item
low_level_serialize = _serializer.serialize


def py2dy(data: Union[Item, None]) -> Union[DynamoItem, None]:
    if data is None:
        return data

    return serialize_dict(data)


def parse_amazon_timestamp(timestamp: str) -> datetime.datetime:
    return datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc
    )
