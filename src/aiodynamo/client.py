from enum import Enum
from functools import partial
from itertools import chain
from typing import List, Dict, Any, TypeVar, Union, AsyncIterator

import attr
from boto3.dynamodb.conditions import ConditionBase, ConditionExpressionBuilder
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

from aiodynamo.utils import unroll

NOTHING = object()


Item = TypeVar('Item', Dict[str, Any])
DynamoItem = TypeVar('DynamoItem', Dict[str, Dict[str, Any]])
TableName = TypeVar('TableName', str)


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
class Key:
    name: str = attr.ib()
    type: KeyType = attr.ib()


@attr.s
class KeySchema:
    hash_key: Key = attr.ib()
    range_key: Key = attr.ib(default=None)

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


def py2dy(data: Item) -> DynamoItem:
    return {
        key: Serializer.serialize(value)
        for key, value in data.items()
    }


def dy2py(data: DynamoItem) -> Item:
    return {
        key: Deserializer.deserialize(value)
        for key, value in data.items()
    }


@attr.s
class ProjectionExpression:
    expression: str = attr.ib()
    attribute_names: Dict[str, str] = attr.ib()


class Dot:
    pass


@attr.s
class Index:
    value = attr.ib()


@attr.s
class Attr:
    _chain = attr.ib()

    def __getattr__(self, item):
        return Attr(self._chain + [Dot, item])

    def __getitem__(self, item):
        if isinstance(item, str):
            return Attr(self._chain + [Dot, item])
        if not isinstance(item, int) or item < 0:
            raise TypeError('Attribute index must be positive integer')
        return Attr(self._chain + [Index(item)])

    def encode(self, encoder):
        clean = []
        for bit in self._chain:
            if isinstance(bit, str):
                clean.append(encoder.encode(bit))
            elif bit is Dot:
                clean.append('.')
            elif isinstance(bit, Index):
                clean.append(f'[{bit.value}]')
            else:
                raise TypeError('Unexpected Attr chain bit')
        return ''.join(clean)


_Key = TypeVar('_Key')
_Val = TypeVar('_Val')
def flip(data: Dict[_Key, _Val]) -> Dict[_Val, _Key]:
    return {value: key for key, value in data.items()}


def _insert(target: Dict[str, str], key: str, prefix: str) -> str:
    if key not in target:
        target[key] = f'{prefix}{len(target)}'
    return target[key]


@attr.s
class Encoder:
    prefix: str = attr.ib()
    values = attr.ib(default=attr.Factory(dict))

    def finalize(self) -> Dict[str, str]:
        return flip(self.values)

    def encode(self, name: str) -> str:
        return _insert(self.values, name, self.prefix)


@attr.s
class Expression:
    value = attr.ib(default=None)
    substitutes = attr.ib(default=attr.Factory(dict))

    def encode(self):
        return attr.astuple(self)


def project(*args) -> Expression:
    encoder = Encoder('#N')
    bits = []
    for attribute in args:
        if isinstance(attribute, str):
            bits.append(encoder.encode(attribute))
        elif isinstance(attribute, Attr):
            bits.append(attribute.encode(encoder))
        else:
            raise TypeError('Projection must be Attr or str')
    return Expression(','.join(bits), encoder.finalize())


def attribute(name):
    return Attr([name])


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
        condition_expression, expression_attribute_names, expression_attribute_values = ConditionExpressionBuilder().build_expression(condition)
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
                       projection: Expression=None) -> Item:
        projection = projection or Expression()
        projection_expression, expression_attribute_names = projection.encode()
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
            ExpressionAttributeValues=expression_attribute_values,
        ))
        if 'Attributes' in resp:
            return dy2py(resp['Attributes'])
        else:
            return None

    def query(self,
              table: TableName,
              *,
              start_key: Dict[str, Any]=None,
              filter_expression: ConditionBase=None,
              scan_forward: bool=True,
              key_condition: ConditionBase=None,
              index: str=None,
              limit: int=None,
              projection: Expression=None,
              select: Select=Select.all_attributes) -> AsyncIterator[Item]:
        if projection:
            select = Select.specific_attributes
        if select is Select.count:
            raise TypeError('Cannot use Select.count with query, use count instead')
        projection = projection or Expression()
        expression_attribute_names = {}
        expression_attribute_values = {}

        projection_expression, ean = projection.encode()
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
            ExpressionAttributeValues=expression_attribute_values,
            Select=select.value,
        ))
        return unroll(
            coro_func,
            'ExclusiveStartKey',
            'LastEvaluatedKey',
            'Items',
            py2dy(start_key) if start_key else None,
            limit,
            'Limit'
        )

    def scan(self,
             table: TableName,
             *,
             index: str=None,
             limit: int=None,
             start_key: Dict[str, Any]=None,
             projection: Expression=None,
             filter_expression: ConditionBase=None) -> AsyncIterator[Item]:
        projection = projection or Expression()
        expression_attribute_names = {}
        expression_attribute_values = {}

        projection_expression, ean = projection.encode()
        expression_attribute_names.update(ean)

        builder = ConditionExpressionBuilder()

        if filter_expression:
            filter_expression, ean, eav = builder.build_expression(filter_expression)
            expression_attribute_names.update(ean)
            expression_attribute_values.update(eav)

        coro_func = partial(self.core.query, **clean(
            TableName=table,
            IndexName=index,
            ProjectionExpression=projection_expression,
            FilterExpression=filter_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
        ))
        return unroll(
            coro_func,
            'ExclusiveStartKey',
            'LastEvaluatedKey',
            'Items',
            py2dy(start_key) if start_key else None,
            limit,
            'Limit'
        )

    async def count(self,
                    table: TableName,
                    *,
                    start_key: Dict[str, Any]=None,
                    filter_expression: ConditionBase=None,
                    key_condition: ConditionBase=None,
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
            ExpressionAttributeValues=expression_attribute_values,
            Select=Select.count.value,
        ))
        return sum(count async for count in unroll(
            coro_func,
            'ExclusiveStartKey',
            'LastEvaluatedKey',
            'Count',
            py2dy(start_key) if start_key else None,
            process=lambda x: [x]
        ))

    async def update_item(self, *args, **kwargs):
        # TODO: how to build UpdateExpression!?
        # https://botocore.readthedocs.io/en/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
        raise NotImplementedError()

    async def update_table(self, *args, **kwargs):
        raise NotImplementedError()
