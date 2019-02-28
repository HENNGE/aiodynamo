import abc
import datetime
from collections import defaultdict
from enum import Enum
from typing import Dict, List, Any, Set, Tuple, Union

import attr

from .types import Path, PathEncoder, EncoderFunc, NOTHING, EMPTY
from .utils import clean, ensure_not_empty, check_empty_value, maybe_immutable


ProjectionExpr = Union["ProjectionExpression", "F"]


@attr.s
class Throughput:
    read: int = attr.ib()
    write: int = attr.ib()

    def encode(self):
        return {"ReadCapacityUnits": self.read, "WriteCapacityUnits": self.write}


class KeyType(Enum):
    string = "S"
    number = "N"
    binary = "B"


@attr.s
class KeySpec:
    name: str = attr.ib()
    type: KeyType = attr.ib()


@attr.s
class KeySchema:
    hash_key: KeySpec = attr.ib()
    range_key: KeySpec = attr.ib(default=None)

    def __iter__(self):
        yield self.hash_key

        if self.range_key:
            yield self.range_key

    def to_attributes(self) -> Dict[str, str]:
        return {key.name: key.type.value for key in self}

    def encode(self) -> List[Dict[str, str]]:
        return [
            {"AttributeName": key.name, "KeyType": key_type}
            for key, key_type in zip(self, ["HASH", "RANGE"])
        ]


class ProjectionType(Enum):
    all = "ALL"
    keys_only = "KEYS_ONLY"
    include = "INCLUDE"


@attr.s
class Projection:
    type: ProjectionType = attr.ib()
    attrs: List[str] = attr.ib(default=None)

    def encode(self):
        encoded = {"ProjectionType": self.type.value}
        if self.attrs:
            encoded["NonKeyAttributes"] = self.attrs
        return encoded


@attr.s
class LocalSecondaryIndex:
    name: str = attr.ib()
    schema: KeySchema = attr.ib()
    projection: Projection = attr.ib()

    def encode(self):
        return {
            "IndexName": self.name,
            "KeySchema": self.schema.encode(),
            "Projection": self.projection.encode(),
        }


@attr.s
class GlobalSecondaryIndex(LocalSecondaryIndex):
    throughput: Throughput = attr.ib()

    def encode(self):
        return {**super().encode(), "ProvisionedThroughput": self.throughput.encode()}


class StreamViewType(Enum):
    keys_only = "KEYS_ONLY"
    new_image = "NEW_IMAGE"
    old_image = "OLD_IMAGE"
    new_and_old_images = "NEW_AND_OLD_IMAGES"


@attr.s
class StreamSpecification:
    enabled: bool = attr.ib(default=False)
    view_type: StreamViewType = attr.ib(default=StreamViewType.new_and_old_images)

    def encode(self):
        return clean(
            StreamEnabled=self.enabled,
            StreamViewType=self.view_type.value if self.enabled else None,
        )


class ReturnValues(Enum):
    none = "NONE"
    all_old = "ALL_OLD"
    updated_old = "UPDATED_OLD"
    all_new = "ALL_NEW"
    updated_new = "UPDATED_NEW"


class ActionTypes(Enum):
    set = "SET"
    remove = "REMOVE"
    add = "ADD"
    delete = "DELETE"


class BaseAction(metaclass=abc.ABCMeta):
    type = abc.abstractproperty()

    def encode(self, name_encoder: "Encoder", value_encoder: "Encoder") -> str:
        return self._encode(name_encoder.encode_path, value_encoder.encode)

    @abc.abstractmethod
    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        ...


@attr.s
class SetAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib(converter=ensure_not_empty)
    ine: "F" = attr.ib(default=NOTHING)

    type = ActionTypes.set

    @check_empty_value
    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        if self.ine is not NOTHING:
            return f"{N(self.path)} = if_not_exists({N(self.ine.path)}, {V(self.value)}"

        else:
            return f"{N(self.path)} = {V(self.value)}"

    def if_not_exists(self, key: "F") -> "SetAction":
        return attr.evolve(self, ine=key)


@attr.s
class ChangeAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib(converter=ensure_not_empty)

    type = ActionTypes.set

    @check_empty_value
    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        if self.value > 0:
            op = "+"
            value = self.value
        else:
            value = self.value * -1
            op = "-"
        return f"{N(self.path)} = {N(self.path)} {op} {V(value)}"


@attr.s
class AppendAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib(converter=ensure_not_empty)

    type = ActionTypes.set

    @check_empty_value
    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        return f"{N(self.path)} = list_append({N(self.path)}, {V(self.value)})"


@attr.s
class RemoveAction(BaseAction):
    path: Path = attr.ib()

    type = ActionTypes.remove

    def _encode(self, N, V) -> str:
        return N(self.path)


@attr.s
class DeleteAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib(converter=ensure_not_empty)

    type = ActionTypes.delete

    @check_empty_value
    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        return f"{N(self.path)} {V(self.value)}"


@attr.s
class AddAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib(converter=ensure_not_empty)

    type = ActionTypes.add

    @check_empty_value
    def _encode(self, N: PathEncoder, V: EncoderFunc):
        return f"{N(self.path)} {V(self.value)}"


class F:
    def __init__(self, *path):
        self.path: Path = path

    def __and__(self, other: "F") -> "ProjectionExpression":
        pe = ProjectionExpression()
        return pe & self & other

    def encode(self, encoder: "Encoder") -> str:
        return encoder.encode_path(self.path)

    def set(self, value: Any) -> "UpdateExpression":
        return UpdateExpression(SetAction(self.path, value))

    def change(self, diff: int) -> "UpdateExpression":
        return UpdateExpression(ChangeAction(self.path, diff))

    def append(self, value: List[Any]) -> "UpdateExpression":
        return UpdateExpression(AppendAction(self.path, list(value)))

    def remove(self) -> "UpdateExpression":
        return UpdateExpression(RemoveAction(self.path))

    def add(self, value: Set[Any]) -> "UpdateExpression":
        return UpdateExpression(AddAction(self.path, value))

    def delete(self, value: Set[Any]) -> "UpdateExpression":
        return UpdateExpression(DeleteAction(self.path, value))


class UpdateExpression:
    def __init__(self, *updates: BaseAction):
        self.updates = updates

    def __and__(self, other: "UpdateExpression") -> "UpdateExpression":
        return UpdateExpression(*self.updates, *other.updates)

    def __bool__(self):
        return bool(self.updates)

    def encode(self) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
        name_encoder = Encoder("#N")
        value_encoder = Encoder(":V")
        parts = defaultdict(list)
        for action in self.updates:
            value = action.encode(name_encoder, value_encoder)
            if value is not EMPTY:
                parts[action.type].append(value)
        part_list = [
            f'{action.value} {", ".join(values)}' for action, values in parts.items()
        ]
        return " ".join(part_list), name_encoder.finalize(), value_encoder.finalize()


@attr.s
class ProjectionExpression:
    fields: List[F] = attr.ib(default=attr.Factory(list))

    def __and__(self, field: F) -> "ProjectionExpression":
        return ProjectionExpression(self.fields + [field])

    def encode(self) -> Tuple[str, Dict[str, Any]]:
        name_encoder = Encoder("#N")
        return (
            ",".join(field.encode(name_encoder) for field in self.fields),
            name_encoder.finalize(),
        )


class TableStatus(Enum):
    creating = "CREATING"
    updating = "UPDATING"
    deleting = "DELETING"
    active = "ACTIVE"


@attr.s
class TableDescription:
    attributes: Dict[str, KeyType] = attr.ib()
    created: datetime.datetime = attr.ib()
    item_count: int = attr.ib()
    key_schema: KeySchema = attr.ib()
    throughput: Throughput = attr.ib()
    status: TableStatus = attr.ib()


@attr.s
class Encoder:
    prefix: str = attr.ib()
    data: List[Any] = attr.ib(default=attr.Factory(list))
    cache: Dict[Any, Any] = attr.ib(default=attr.Factory(dict))

    def finalize(self) -> Dict[str, str]:
        return {f"{self.prefix}{index}": value for index, value in enumerate(self.data)}

    def encode(self, name: Any) -> str:
        key = maybe_immutable(name)
        try:
            return self.cache[key]

        except KeyError:
            can_cache = True
        except TypeError:
            can_cache = False
        encoded = f"{self.prefix}{len(self.data)}"
        self.data.append(name)
        if can_cache:
            self.cache[key] = encoded
        return encoded

    def encode_path(self, path: Path) -> str:
        bits = [self.encode(path[0])]
        for part in path[1:]:
            if isinstance(part, int):
                bits.append(f"[{part}]")
            else:
                bits.append(f".{self.encode(part)}")
        return "".join(bits)


class Select(Enum):
    all_attributes = "ALL_ATTRIBUTES"
    all_projected_attributes = "ALL_PROJECTED_ATTRIBUTES"
    count = "COUNT"
    specific_attributes = "SPECIFIC_ATTRIBUTES"


def get_projection(
    projection: Union[ProjectionExpression, F, None]
) -> Tuple[Union[str, None], Dict[str, Any]]:
    if projection is None:
        return None, {}

    if isinstance(projection, ProjectionExpression):
        return projection.encode()

    else:
        encoder = Encoder("#N")
        return projection.encode(encoder), encoder.finalize()
