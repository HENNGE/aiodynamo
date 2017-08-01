import abc
from collections import defaultdict
from enum import Enum
from functools import partial
from itertools import chain
from typing import (
    List, Dict, Any, TypeVar, Union, AsyncIterator, Tuple,
    Callable,
    Set,
)

import attr
from boto3.dynamodb.conditions import ConditionBase, ConditionExpressionBuilder
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

from aiodynamo.utils import unroll

NOTHING = object()


Item = TypeVar('Item', bound=Dict[str, Any])
DynamoItem = TypeVar('DynamoItem', bound=Dict[str, Dict[str, Any]])
TableName = TypeVar('TableName', bound=str)
Path = List[Union[str, int]]
PathEncoder = Callable[[Path], str]
EncoderFunc = Callable[[Any], str]


@attr.s
class Throughput:
    read: int = attr.ib()
    write: int = attr.ib()

    def encode(self):
        return {
            'ReadCapacityUnits': self.read,
            'WriteCapacityUnits': self.write,
        }


class KeyType(Enum):
    string = 'S'
    number = 'N'
    binary = 'B'


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
        return {
            key.name: key.type.value for key in self
        }

    def encode(self) -> List[Dict[str, str]]:
        return [
            {
                'AttributeName': key.name,
                'KeyType': key_type
            } for key, key_type in zip(self, ['HASH', 'RANGE'])
        ]


class ProjectionType(Enum):
    all = 'ALL'
    keys_only = 'KEYS_ONLY'
    include = 'INCLUDE'


@attr.s
class Projection:
    type: ProjectionType = attr.ib()
    attrs: List[str] = attr.ib(default=None)

    def encode(self):
        return {
            'ProjectionType': self.type.value,
            'NonKeyAttributes': self.attrs or []
        }


@attr.s
class LocalSecondaryIndex:
    name: str = attr.ib()
    schema: KeySchema = attr.ib()
    projection: Projection = attr.ib()

    def encode(self):
        return {
            'IndexName': self.name,
            'KeySchema': self.schema.encode(),
            'Projection': self.projection.encode(),
        }


@attr.s
class GlobalSecondaryIndex(LocalSecondaryIndex):
    throughput: Throughput = attr.ib()

    def encode(self):
        return {
            **super().encode(),
            'ProvisionedThroughput': self.throughput.encode()
        }


class StreamViewType(Enum):
    keys_only = 'KEYS_ONLY'
    new_image = 'NEW_IMAGE'
    old_image = 'OLD_IMAGE'
    new_and_old_images = 'NEW_AND_OLD_IMAGES'


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
    none = 'NONE'
    all_old = 'ALL_OLD'
    updated_old = 'UPDATED_OLD'
    all_new = 'ALL_NEW'
    updated_new = 'UPDATED_NEW'


Serializer = TypeSerializer()
Deserializer = TypeDeserializer()


def py2dy(data: Union[Item, None]) -> Union[DynamoItem, None]:
    if data is None:
        return data
    return {
        key: Serializer.serialize(value)
        for key, value in data.items()
    }


def dy2py(data: DynamoItem) -> Item:
    return {
        key: Deserializer.deserialize(value)
        for key, value in data.items()
    }


class ActionTypes(Enum):
    set = 'SET'
    remove = 'REMOVE'
    add = 'ADD'
    delete = 'DELETE'


class BaseAction(metaclass=abc.ABCMeta):
    type = abc.abstractproperty()

    def __and__(self, other: 'BaseAction') -> 'UpdateExpression':
        return UpdateExpression(self, other)

    def encode(self, name_encoder: 'Encoder', value_encoder: 'Encoder') -> str:
        return self._encode(name_encoder.encode_path, value_encoder.encode)

    @abc.abstractmethod
    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        ...


@attr.s
class SetAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib()
    ine: 'F' = attr.ib(default=NOTHING)

    type = ActionTypes.set

    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        if self.ine is not NOTHING:
            return f'{N(self.path)} = if_not_exists({N(self.ine.path)}, {V(self.value)}'
        else:
            return f'{N(self.path)} = {V(self.value)}'

    def if_not_exists(self, key: 'F') -> 'SetAction':
        return attr.evolve(self, ine=key)


@attr.s
class ChangeAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib()

    type = ActionTypes.set

    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        if self.value > 0:
            op = '+'
            value = self.value
        else:
            value = self.value * -1
            op = '-'
        return f'{N(self.path)} = {N(self.path)} {op} {V(value)}'


@attr.s
class AppendAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib()

    type = ActionTypes.set

    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        return f'{N(self.path)} = list_append({N(self.path)}, {V(self.value)})'


@attr.s
class RemoveAction(BaseAction):
    path: Path = attr.ib()

    type = ActionTypes.remove

    def _encode(self, N, V) -> str:
        return N(self.path)


@attr.s
class DeleteAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib()

    type = ActionTypes.delete

    def _encode(self, N: PathEncoder, V: EncoderFunc) -> str:
        return f'{N(self.path)} {V(self.value)}'


@attr.s
class AddAction(BaseAction):
    path: Path = attr.ib()
    value: Any = attr.ib()

    type = ActionTypes.add

    def _encode(self, N: PathEncoder, V: EncoderFunc):
        return f'{N(self.path)} {V(self.value)}'


class F:
    def __init__(self, *path):
        self.path: Path = path

    def __and__(self, other: 'F') -> 'ProjectionExpression':
        pe = ProjectionExpression()
        return pe & self & other

    def encode(self, encoder: 'Encoder') -> str:
        return encoder.encode_path(self.path)

    def set(self, value: any):
        return SetAction(self.path, value)

    def change(self, diff: int):
        return ChangeAction(self.path, diff)

    def append(self, value: List[Any]):
        return AppendAction(self.path, list(value))

    def remove(self):
        return RemoveAction(self.path)

    def add(self, value: Set[Any]):
        return AddAction(self.path, value)

    def delete(self, value: Set[Any]):
        return DeleteAction(self.path, value)


class UpdateExpression:
    def __init__(self, *updates: BaseAction):
        self.updates = updates

    def __and__(self, other: BaseAction) -> 'UpdateExpression':
        return UpdateExpression(*self.updates, other)

    def encode(self) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
        name_encoder = Encoder('#N')
        value_encoder = Encoder(':V')
        parts = defaultdict(list)
        for action in self.updates:
            parts[action.type].append(action.encode(name_encoder, value_encoder))
        part_list = [
            f'{action.value} {", ".join(values)}' for action, values in parts.items()
        ]
        return ' '.join(part_list), name_encoder.finalize(), value_encoder.finalize()


@attr.s
class ProjectionExpression:
    fields: List[F] = attr.ib(default=attr.Factory(list))

    def __and__(self, field: F) -> 'ProjectionExpression':
        return ProjectionExpression(self.fields + [field])

    def encode(self) -> Tuple[str, Dict[str, Any]]:
        name_encoder = Encoder('#N')
        return ','.join(field.encode(name_encoder) for field in self.fields), name_encoder.finalize()


_Key = TypeVar('_Key')
_Val = TypeVar('_Val')


def immutable(thing: Any):
    if isinstance(thing, list):
        return tuple(thing)
    elif isinstance(thing, set):
        return frozenset(thing)
    else:
        return thing


@attr.s
class Encoder:
    prefix: str = attr.ib()
    data = attr.ib(default=attr.Factory(dict))

    def finalize(self) -> Dict[str, str]:
        return dict(self.data.values())

    def encode(self, name: Any) -> str:
        key = immutable(name)
        if key not in self.data:
            self.data[key] = (f'{self.prefix}{len(self.data)}', name)
        return self.data[key][0]

    def encode_path(self, path: Path) -> str:
        bits = [self.encode(path[0])]
        for part in path[1:]:
            if isinstance(part, int):
                bits.append(f'[{part}]')
            else:
                bits.append(f'.{self.encode(part)}')
        return ''.join(bits)


def clean(**kwargs):
    return {
        key: value for key, value in kwargs.items() if value
    }


class ItemNotFound(Exception):
    pass


class Select(Enum):
    all_attributes = 'ALL_ATTRIBUTES'
    all_projected_attributes = 'ALL_PROJECTED_ATTRIBUTES'
    count = 'COUNT'
    specific_attributes = 'SPECIFIC_ATTRIBUTES'


def get_projection(projection: Union[ProjectionExpression, F, None]) -> Tuple[Union[str, None], Dict[str, Any]]:
    if projection is None:
        return None, {}
    if isinstance(projection, ProjectionExpression):
        return projection.encode()
    else:
        encoder = Encoder('#N')
        return projection.encode(encoder), encoder.finalize()


@attr.s
class Client:
    core = attr.ib()

    async def create_table(self,
                           name: str,
                           throughput: Throughput,
                           keys: KeySchema,
                           *,
                           lsis: List[LocalSecondaryIndex]=None,
                           gsis: List[GlobalSecondaryIndex]=None,
                           stream: StreamSpecification=None):
        lsis: List[LocalSecondaryIndex] = lsis or []
        gsis: List[GlobalSecondaryIndex] = gsis or []
        stream = stream or StreamSpecification()
        attributes = {}
        attributes.update(keys.to_attributes())
        for index in chain(lsis, gsis):
            attributes.update(index.to_attributes())
        attribute_definitions = [
            {
                'AttributeName': key,
                'AttributeType': value
            } for key, value in attributes.items()
        ]
        key_schema = keys.encode()
        local_secondary_indexes = [
            index.encode() for index in lsis
        ]
        global_secondary_indexes = [
            index.encode() for index in gsis
        ]
        provisioned_throughput = throughput.encode()
        stream_specification = stream.encode()
        await self.core.create_table(**clean(
            AttributeDefinitions=attribute_definitions,
            TableName=name,
            KeySchema=key_schema,
            LocalSecondaryIndexes=local_secondary_indexes,
            GlobalSecondaryIndexex=global_secondary_indexes,
            ProvisionedThroughput=provisioned_throughput,
            StreamSpecification=stream_specification,
        ))
        return None

    async def delete_item(self,
                          table: str,
                          key: Dict[str, Any],
                          *,
                          return_values: ReturnValues=ReturnValues.none,
                          condition: ConditionBase=None) -> Union[None, Item]:
        key = py2dy(key)
        if condition:
            condition_expression, expression_attribute_names, expression_attribute_values = ConditionExpressionBuilder().build_expression(condition)
        else:
            condition_expression = expression_attribute_names = expression_attribute_values = None
        resp = await self.core.delete_item(**clean(
            TableName=table,
            Key=key,
            ReturnValues=return_values.value,
            ConditionExpression=condition_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttribuetValues=expression_attribute_values,
        ))
        if 'Attributes' in resp:
            return dy2py(resp['Attributes'])
        else:
            return None

    async def delete_table(self,
                           table: TableName):
        await self.core.delete_table(TableName=table)

    async def get_item(self,
                       table: TableName,
                       key: Dict[str, Any],
                       *,
                       projection: ProjectionExpression=None) -> Item:
        projection_expression, expression_attribute_names = get_projection(projection)
        resp = await self.core.get_item(**clean(
            TableName=table,
            Key=py2dy(key),
            ProjectionExpression=projection_expression,
            ExpressionAttributeNames=expression_attribute_names,
        ))
        if 'Item' in resp:
            return dy2py(resp['Item'])
        else:
            raise ItemNotFound(key)

    async def put_item(self,
                       table: TableName,
                       item: Dict[str, Any],
                       *,
                       return_values: ReturnValues=ReturnValues.none,
                       condition: ConditionBase=None) -> Union[None, Item]:
        if condition:
            condition_expression, expression_attribute_names, expression_attribute_values = ConditionExpressionBuilder().build_expression(condition)
        else:
            condition_expression = expression_attribute_names = expression_attribute_values = None
        resp = await self.core.put_item(**clean(
            TableName=table,
            Item=py2dy(item),
            ReturnValues=return_values.value,
            ConditionExpression=condition_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=py2dy(expression_attribute_values),
        ))
        if 'Attributes' in resp:
            return dy2py(resp['Attributes'])
        else:
            return None

    async def query(self,
                    table: TableName,
                    key_condition: ConditionBase,
                    *,
                    start_key: Dict[str, Any]=None,
                    filter_expression: ConditionBase=None,
                    scan_forward: bool=True,
                    index: str=None,
                    limit: int=None,
                    projection: ProjectionExpression=None,
                    select: Select=Select.all_attributes) -> AsyncIterator[Item]:
        if projection:
            select = Select.specific_attributes
        if select is Select.count:
            raise TypeError('Cannot use Select.count with query, use count instead')
        expression_attribute_names = {}
        expression_attribute_values = {}

        projection_expression, ean = get_projection(projection)
        expression_attribute_names.update(ean)

        builder = ConditionExpressionBuilder()

        if filter_expression:
            filter_expression, ean, eav = builder.build_expression(filter_expression)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)

        if key_condition:
            key_condition_expression, ean, eav = builder.build_expression(key_condition, True)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)
        else:
            key_condition_expression = None

        coro_func = partial(self.core.query, **clean(
            TableName=table,
            IndexName=index,
            ScanIndexForward=scan_forward,
            ProjectionExpression=projection_expression,
            FilterExpression=filter_expression,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=py2dy(expression_attribute_values),
            Select=select.value,
        ))
        async for raw in unroll(
            coro_func,
            'ExclusiveStartKey',
            'LastEvaluatedKey',
            'Items',
            py2dy(start_key) if start_key else None,
            limit,
            'Limit'
        ): yield dy2py(raw)

    async def scan(self,
                   table: TableName,
                   *,
                   index: str=None,
                   limit: int=None,
                   start_key: Dict[str, Any]=None,
                   projection: ProjectionExpression=None,
                   filter_expression: ConditionBase=None) -> AsyncIterator[Item]:
        expression_attribute_names = {}
        expression_attribute_values = {}

        projection_expression, ean = get_projection(projection)
        expression_attribute_names.update(ean)

        builder = ConditionExpressionBuilder()

        if filter_expression:
            filter_expression, ean, eav = builder.build_expression(filter_expression)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)

        coro_func = partial(self.core.scan, **clean(
            TableName=table,
            IndexName=index,
            ProjectionExpression=projection_expression,
            FilterExpression=filter_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=py2dy(expression_attribute_values),
        ))
        async for raw in unroll(
            coro_func,
            'ExclusiveStartKey',
            'LastEvaluatedKey',
            'Items',
            py2dy(start_key) if start_key else None,
            limit,
            'Limit'
        ): yield dy2py(raw)

    async def count(self,
                    table: TableName,
                    key_condition: ConditionBase,
                    *,
                    start_key: Dict[str, Any]=None,
                    filter_expression: ConditionBase=None,
                    index: str=None) -> int:
        expression_attribute_names = {}
        expression_attribute_values = {}

        builder = ConditionExpressionBuilder()

        if filter_expression:
            filter_expression, ean, eav = builder.build_expression(filter_expression)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)

        if key_condition:
            key_condition_expression, ean, eav = builder.build_expression(key_condition, True)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)
        else:
            key_condition_expression = None

        coro_func = partial(self.core.query, **clean(
            TableName=table,
            IndexName=index,
            FilterExpression=filter_expression,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=py2dy(expression_attribute_values),
            Select=Select.count.value,
        ))
        count_sum = 0
        async for count in unroll(
            coro_func,
            'ExclusiveStartKey',
            'LastEvaluatedKey',
            'Count',
            py2dy(start_key) if start_key else None,
            process=lambda x: [x]
        ): count_sum += count
        return count_sum

    async def update_item(self,
                          table: TableName,
                          key: Item,
                          update_expression: UpdateExpression,
                          *,
                          return_values: ReturnValues=ReturnValues.none,
                          condition: ConditionBase=None) -> Union[Item, None]:

        update_expression, expression_attribute_names, expression_attribute_values = update_expression.encode()

        builder = ConditionExpressionBuilder()
        if condition:
            condition_expression, ean, eav = builder.build_expression(condition)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)
        else:
            condition_expression = None
        resp = await self.core.update_item(**clean(
            TableName=table,
            Key=py2dy(key),
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=py2dy(expression_attribute_values),
            ConditionExpression=condition_expression,
            ReturnValues=return_values.value
        ))
        if 'Attributes' in resp:
            return dy2py(resp['Attributes'])
        else:
            return None
