from __future__ import annotations

import abc
import decimal
from dataclasses import dataclass, field
from itertools import chain, count
from typing import *

from .errors import CannotAddToNestedField
from .types import AttributeType, Numeric, ParametersDict
from .utils import low_level_serialize

_ParametersCache = Dict[Tuple[Any, Any], str]

Addable = Union[Numeric, Set[bytes], Set[str], Set[Numeric]]


class ProjectionExpression(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def encode(self, params: Parameters) -> str:
        pass

    @abc.abstractmethod
    def __and__(self, other: F) -> ProjectionExpression:
        pass


@dataclass(frozen=True)
class KeyPath:
    root: str
    parts: Sequence[Union[str, int]] = ()


class F(ProjectionExpression):
    """
    This class represents a Field or Attribute in a DynamoDB Item.

    For paths to items, provide them as separate arguments. For example,
    to reference to the second item in a list stored in the key "foo", use
    `F("foo", 1)`.

    It can be used to create Projection Expressions using the & operator,
    for example `F("foo") & F("bar")` is the Projection expression to
    return the two fields "foo" and "bar".

    It is also used to create Condition Expressions. See the various
    methods below.

    Lastly, it is also used to create Update Expressions, see methods
    at the end of the class.
    """

    path: KeyPath

    def __init__(self, root: str, *path: Union[str, int]):
        self.path = KeyPath(root, path)

    def __hash__(self) -> int:
        return hash(self.path)

    # Projection Expressions

    def __and__(self, other: F) -> ProjectionExpression:
        return FieldList([self, other])

    def encode(self, params: Parameters) -> str:
        return params.encode_path(self.path)

    # Condition Expressions

    def does_not_exist(self) -> Condition:
        """
        Checks that a field does not exist.
        """
        return DoesNotExist(self)

    def exists(self) -> Condition:
        """
        Checks that a field exist.
        """
        return Exists(self)

    def attribute_type(self, attribute_type: AttributeType) -> Condition:
        """
        Checks the attribute type of a field.
        """
        return AttributeTypeCondition(self, attribute_type)

    def begins_with(self, substr: str) -> Condition:
        """
        Checks that a field begins with a substring.
        The substring must not be empty.
        Fields matching the substring provided completely are not returned.
        """
        if not substr:
            raise ValueError("Substring may not be empty")
        return BeginsWith(self, substr)

    def between(self, low: Any, high: Any) -> Condition:
        """
        Checks that a field is between two given values.
        """
        return Between(self, low, high)

    def contains(
        self, value: Union[str, bytes, int, float, decimal.Decimal]
    ) -> Condition:
        """
        Checks if a set or list contains a certain value.
        If a string or bytes object is used as a value, they must not be empty.
        """
        if isinstance(value, (bytes, str)) and not value:
            raise ValueError("Value may not be empty")
        return Contains(self, value)

    def size(self) -> Size:
        """
        Allows checking for the item size. Does not return a condition,
        to return a condition, call the appropriate method on the Size
        class instance returned.
        """
        return Size(self)

    def is_in(self, values: Sequence[Any]) -> Condition:
        """
        Checks if the field is in a sequence of values.
        Between one and one hundred values may be provided.
        """
        return In(self, values)

    def gt(self, other: Any) -> Condition:
        """
        Checks if a field is greater than a value.
        """
        return Comparison(self, ">", other)

    def gte(self, other: Any) -> Condition:
        """
        Checks if a field is greater than a value.
        """
        return Comparison(self, ">=", other)

    def lt(self, other: Any) -> Condition:
        """
        Checks if a field is less than a value.
        """
        return Comparison(self, "<", other)

    def lte(self, other: Any) -> Condition:
        """
        Checks if a field is less or equal than a value.
        """
        return Comparison(self, "<=", other)

    def equals(self, other: Any) -> Condition:
        """
        Checks if a field is equal to value.
        """
        return Comparison(self, "=", other)

    def not_equals(self, other: Any) -> Condition:
        """
        Checks if a field is not equal to a value.
        """
        return Comparison(self, "<>", other)

    # Update Expressions

    def set(self, value: Any) -> UpdateExpression:
        """
        Set a field to a value.
        """
        if isinstance(value, (bytes, str)) and not value:
            return UpdateExpression(remove={self})
        return UpdateExpression(set_updates={self: Value(value)})

    def set_if_not_exists(self, value: Any) -> UpdateExpression:
        """
        Set a field to a value if the field does not exist in the item yet.
        """
        if isinstance(value, (bytes, str)) and not value:
            return UpdateExpression()
        return UpdateExpression(set_updates={self: IfNotExists(value)})

    def change(self, diff: Numeric) -> UpdateExpression:
        """
        Change a numeric field by a given value.
        """
        return UpdateExpression(set_updates={self: Modify(diff)})

    def append(self, value: List[Any]) -> UpdateExpression:
        """
        Add items to a list field. Note that the value passed in should be a
        list, not an individual item.
        """
        return UpdateExpression(set_updates={self: Append(list(value))})

    def remove(self) -> UpdateExpression:
        """
        Remove a field.
        """
        return UpdateExpression(remove={self})

    def add(self, value: Addable) -> UpdateExpression:
        """
        Add a value to a field. Only allowed for top level fields.

        For numeric fields add a numeric value.
        For set fields, this will set the field to the union of the existing
        set and the set provided.
        """
        if self.path.parts:
            raise CannotAddToNestedField()
        return UpdateExpression(add={self: value})

    def delete(self, value: Set[Any]) -> UpdateExpression:
        """
        Deletes all items in the set provided from the set stored in this field.
        """
        return UpdateExpression(delete={self: value})


class KeyCondition(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def encode(self, params: Parameters) -> str:
        pass


@dataclass(frozen=True)
class HashKey(KeyCondition):
    """
    Used for Key Conditions. To also constrain the Key Condition by the
    range key, create an instance of RangeKey, call a method on it and
    combine the HashKey with the return value of that method call using
    the & operator.
    """

    name: str
    value: Any

    def encode(self, params: Parameters) -> str:
        return f"{params.encode_path(KeyPath(self.name))} = {params.encode_value(self.value)}"

    def __and__(self, other: Condition) -> KeyCondition:
        return HashAndRangeKeyCondition(self, other)


@dataclass(frozen=True)
class RangeKey:
    """
    Can be used to further constrain a Key Condition. Must be used
    together with a HashKey instance.

    The provided methods behave the same as their counterparts in the
    F class.
    """

    name: str

    def begins_with(self, substr: str) -> Condition:
        if not substr:
            raise ValueError("Substring may not be empty")
        return BeginsWith(F(self.name), substr)

    def between(self, low: Any, high: Any) -> Condition:
        return Between(F(self.name), low, high)

    def contains(
        self, value: Union[str, bytes, int, float, decimal.Decimal]
    ) -> Condition:
        return Contains(F(self.name), value)

    def size(self) -> Size:
        return Size(F(self.name))

    def is_in(self, values: Sequence[Any]) -> Condition:
        return In(F(self.name), values)

    def gt(self, other: Any) -> Condition:
        return Comparison(F(self.name), ">", other)

    def gte(self, other: Any) -> Condition:
        return Comparison(F(self.name), ">=", other)

    def lt(self, other: Any) -> Condition:
        return Comparison(F(self.name), "<", other)

    def lte(self, other: Any) -> Condition:
        return Comparison(F(self.name), "<=", other)

    def equals(self, other: Any) -> Condition:
        return Comparison(F(self.name), "=", other)

    def not_equals(self, other: Any) -> Condition:
        return Comparison(F(self.name), "<>", other)


class Parameters:
    def __init__(self) -> None:
        self.names: Dict[str, str] = {}
        self.values: Dict[str, Dict[str, Any]] = {}
        self.names_gen: Iterator[int] = count()
        self.values_gen: Iterator[int] = count()
        self.names_cache: _ParametersCache = {}
        self.values_cache: _ParametersCache = {}

    def encode_name(self, name: Union[str, int]) -> str:
        return self._encode(
            "#n", name, self.names, self.names_gen, self.names_cache, name
        )

    def encode_value(self, value: Any) -> str:
        tag, value = low_level_serialize(value)
        return self._encode(
            ":v",
            {tag: value},
            self.values,
            self.values_gen,
            self.values_cache,
            (tag, value),
        )

    def encode_path(self, path: KeyPath) -> str:
        return "".join(
            chain(
                [self.encode_name(path.root)],
                (
                    f"[{part}]"
                    if isinstance(part, int)
                    else f".{self.encode_name(part)}"
                    for part in path.parts
                ),
            )
        )

    def to_request_payload(self) -> ParametersDict:
        payload: ParametersDict = {}
        if self.names:
            payload["ExpressionAttributeNames"] = self.names
        if self.values:
            payload["ExpressionAttributeValues"] = {
                key: value for key, value in self.values.items()
            }
        return payload

    def _encode(
        self,
        prefix: str,
        thing: Any,
        data: Dict[str, Any],
        index_gen: Iterator[int],
        cache: _ParametersCache,
        cache_key: Any,
    ) -> str:
        try:
            return cache[cache_key]
        except KeyError:
            can_cache = True
        except TypeError:
            can_cache = False

        encoded = f"{prefix}{next(index_gen)}"
        data[encoded] = thing
        if can_cache:
            cache[cache_key] = encoded
        return encoded


@dataclass(frozen=True)
class HashAndRangeKeyCondition(KeyCondition):
    hash_key: HashKey
    range_key_condition: Condition

    def encode(self, params: Parameters) -> str:
        return f"{self.hash_key.encode(params)} AND {self.range_key_condition.encode(params)}"


class Condition(metaclass=abc.ABCMeta):
    def __and__(self, other: Condition) -> Condition:
        return AndCondition(self, other)

    def __or__(self, other: Condition) -> Condition:
        return OrCondition(self, other)

    def __invert__(self) -> Condition:
        return NotCondition(self)

    @abc.abstractmethod
    def encode(self, params: Parameters) -> str:
        pass


@dataclass(frozen=True)
class NotCondition(Condition):
    base: Condition

    def encode(self, params: Parameters) -> str:
        return f"(NOT {self.base.encode(params)})"


@dataclass(frozen=True)
class AndCondition(Condition):
    lhs: Condition
    rhs: Condition

    def encode(self, params: Parameters) -> str:
        return f"({self.lhs.encode(params)} AND {self.rhs.encode(params)})"


@dataclass(frozen=True)
class OrCondition(Condition):
    lhs: Condition
    rhs: Condition

    def encode(self, params: Parameters) -> str:
        return f"({self.lhs.encode(params)} OR {self.rhs.encode(params)})"


@dataclass(frozen=True)
class DoesNotExist(Condition):
    field: F

    def encode(self, params: Parameters) -> str:
        return f"attribute_not_exists({params.encode_path(self.field.path)})"


@dataclass(frozen=True)
class Exists(Condition):
    field: F

    def encode(self, params: Parameters) -> str:
        return f"attribute_exists({params.encode_path(self.field.path)})"


@dataclass(frozen=True)
class AttributeTypeCondition(Condition):
    field: F
    attribute_type: AttributeType

    def encode(self, params: Parameters) -> str:
        return f"attribute_type({params.encode_path(self.field.path)}, {self.attribute_type.value})"


@dataclass(frozen=True)
class BeginsWith(Condition):
    field: F
    substr: str

    def encode(self, params: Parameters) -> str:
        return f"begins_with({params.encode_path(self.field.path)}, {params.encode_value(self.substr)})"


@dataclass(frozen=True)
class Between(Condition):
    field: F
    low: Any
    high: Any

    def encode(self, params: Parameters) -> str:
        return f"{params.encode_path(self.field.path)} BETWEEN {params.encode_value(self.low)} AND {params.encode_value(self.high)}"


@dataclass(frozen=True)
class Contains(Condition):
    field: F
    value: Union[str, bytes, int, float, decimal.Decimal]

    def encode(self, params: Parameters) -> str:
        return f"contains({params.encode_path(self.field.path)}, {params.encode_value(self.value)})"


@dataclass(frozen=True)
class In(Condition):
    field: F
    values: Sequence[Any]

    def encode(self, params: Parameters) -> str:
        encoded_values = [
            encoded
            for encoded in (params.encode_value(value) for value in self.values)
            if encoded is not None
        ]
        if len(encoded_values) < 1:
            raise ValueError("IN Condition requires at least one value")
        if len(encoded_values) > 100:
            raise ValueError("IN Condition may not contain more than 100 values")
        values = ",".join(encoded_values)
        return f"{params.encode_path(self.field.path)} IN ({values})"


@dataclass(frozen=True)
class Comparison(Condition):
    field: F
    operator: str
    other: Any

    def encode(self, params: Parameters) -> str:
        if isinstance(self.other, F):
            other = params.encode_path(self.other.path)
        else:
            other = params.encode_value(self.other)
        return f"{params.encode_path(self.field.path)} {self.operator} {other}"


@dataclass(frozen=True)
class Size:
    field: F

    def equals(self, other: Any) -> Condition:
        return SizeCondition(self.field, "=", other)

    def not_equals(self, other: Any) -> Condition:
        return SizeCondition(self.field, "<>", other)

    def gt(self, other: Any) -> Condition:
        return SizeCondition(self.field, ">", other)

    def gte(self, other: Any) -> Condition:
        return SizeCondition(self.field, ">=", other)

    def lte(self, other: Any) -> Condition:
        return SizeCondition(self.field, "<=", other)

    def lt(self, other: Any) -> Condition:
        return SizeCondition(self.field, "<", other)


@dataclass(frozen=True)
class SizeCondition(Condition):
    field: F
    operator: str
    value: Any

    def encode(self, params: Parameters) -> str:
        if isinstance(self.value, F):
            value = params.encode_path(self.value.path)
        else:
            value = params.encode_value(self.value)
        return f"size({params.encode_path(self.field.path)}) {self.operator} {value}"


class SetAction(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def encode(self, params: Parameters, field: F) -> str:
        pass


@dataclass(frozen=True)
class Value(SetAction):
    value: Any

    def encode(self, params: Parameters, field: F) -> str:
        return params.encode_value(self.value)


@dataclass(frozen=True)
class IfNotExists(SetAction):
    value: Any

    def encode(self, params: Parameters, field: F) -> str:
        return f"if_not_exists({params.encode_path(field.path)}, {params.encode_value(self.value)})"


@dataclass(frozen=True)
class Modify(SetAction):
    change: Numeric

    def encode(self, params: Parameters, field: F) -> str:
        if self.change < 0:
            operator = "-"
            value = self.change * -1
        else:
            operator = "+"
            value = self.change
        return (
            f"{params.encode_path(field.path)} {operator} {params.encode_value(value)}"
        )


@dataclass(frozen=True)
class Append(SetAction):
    values: List[Any]

    def encode(self, params: Parameters, field: F) -> str:
        return f"list_append({params.encode_path(field.path)}, {params.encode_value(self.values)})"


@dataclass(frozen=True)
class UpdateExpression:
    set_updates: Dict[F, SetAction] = field(default_factory=dict)
    remove: Set[F] = field(default_factory=set)
    add: Dict[F, Addable] = field(default_factory=dict)
    delete: Dict[F, Union[Set[bytes], Set[Numeric], Set[str]]] = field(
        default_factory=dict
    )

    def __and__(self, other: UpdateExpression) -> UpdateExpression:
        return UpdateExpression(
            set_updates={**self.set_updates, **other.set_updates},
            remove=self.remove.union(other.remove),
            add={**self.add, **other.add},
            delete={**self.delete, **other.delete},
        )

    def encode(self, params: Parameters) -> Optional[str]:
        bits = []
        if self.set_updates:
            set_expr = ", ".join(
                f"{params.encode_path(field.path)} = {action.encode(params, field)}"
                for field, action in self.set_updates.items()
            )
            bits.append(f"SET {set_expr}")
        if self.remove:
            remove_expr = ", ".join(
                params.encode_path(field.path) for field in self.remove
            )
            bits.append(f"REMOVE {remove_expr}")
        if self.add:
            add_expr = ", ".join(
                f"{params.encode_path(field.path)} {params.encode_value(value)}"
                for field, value in self.add.items()
            )
            bits.append(f"ADD {add_expr}")
        if self.delete:
            del_expr = ", ".join(
                f"{params.encode_path(field.path)} {params.encode_value(value)}"
                for field, value in self.delete.items()
            )
            bits.append(f"DELETE {del_expr}")
        if bits:
            return " ".join(bits)
        return None


@dataclass(frozen=True)
class FieldList(ProjectionExpression):
    fields: List[F]

    def __and__(self, field: F) -> ProjectionExpression:
        return FieldList(self.fields + [field])

    def encode(self, params: Parameters) -> str:
        return ",".join(params.encode_path(field.path) for field in self.fields)
