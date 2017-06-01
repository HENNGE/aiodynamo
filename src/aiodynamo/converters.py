from typing import Callable

import attr

from aiodynamo.types import DynamoValue

TConverter = Callable[[DynamoValue], DynamoValue]

@attr.s
class Converter:
    db_to_py: TConverter = attr.ib()
    py_to_db: TConverter = attr.ib()


StringBoolean = Converter(
    lambda value: value == '1' or value is True,
    lambda value: value == '1' or value is True,
)

Integer = Converter(int, int)

PassthroughConverter = Converter(lambda x: x, lambda x: x)
