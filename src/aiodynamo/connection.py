from enum import auto, Enum
from typing import (
    Dict, Type, Optional, List, Iterator, Tuple, TypeVar,
    Sequence,
    AsyncIterator,
    Union,
    Any,
)

from aiobotocore import get_session
import attr
from aiobotocore.client import AioBaseClient
from boto3.dynamodb import conditions
from botocore.exceptions import ClientError

from aiodynamo.constants import NOTHING
from aiodynamo.types import DynamoValue
from . import helpers, exceptions, models
from .models import TModel


async def _iterator(config: 'BotoCoreIterator'):
    token = config.initial
    got = 0
    while True:
        if token is not None:
            token_kwargs = {
                config.request_key: token
            }
        else:
            token_kwargs = {}
        response = await config.func(**{**config.kwargs, **token_kwargs})
        if config.container:
            for item in response.get(config.container, []):
                yield item
                got += 1
                if config.limit and config.limit == got:
                    return
        else:
            yield response
        token = response.get(config.response_key, None)
        if token is None:
            break


@attr.s
class BotoCoreIterator:
    func = attr.ib()
    kwargs = attr.ib()
    request_key = attr.ib()
    response_key = attr.ib()
    container = attr.ib(default=None)
    initial = attr.ib(default=None)
    limit = attr.ib(default=None)

    def __aiter__(self):
        return _iterator(self)


@attr.s
class Meta:
    model: Type[TModel] = attr.ib()
    connection: 'Connection' = attr.ib()
    table: str = attr.ib(init=False)
    config: models.Config = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.table = self.connection.router[self.model]
        self.config = models.get_config(self.model)

    def key(self, *key):
        key = self.config.encode_key(*key)
        return helpers.serialize(key)

    def from_database(self, raw_item):
        data = helpers.deserialize(raw_item)
        return self.config.from_database(data)

    def encode(self, instance):
        data = self.config.encode(instance)
        return helpers.serialize(data)

    def get_key(self, instance):
        key = self.config.get_key(instance)
        return helpers.serialize(key)

    def key_conditions(self, key=NOTHING) -> Dict[str, Any]:
        if key is NOTHING:
            if self.config.range_key:
                raise ValueError('Must provide a key for non hash key only tables')
            return {}
        else:
            key = self.config.encode_key(key)
            name, value = list(key.items())[0]
            condition = conditions.Key(name).eq(value)

            builder = conditions.ConditionExpressionBuilder()
            kce, ean, eav = builder.build_expression(condition, True)
            kwargs = {
                'KeyConditionExpression': kce,
            }
            if ean:
                kwargs['ExpressionAttributeNames'] = ean
            if eav:
                kwargs['ExpressionAttributeValues'] = helpers.serialize(eav)
            return kwargs


class Guards(Enum):
    none = auto()
    exists = auto()
    not_exists = auto()


class Connection:
    def __init__(self, *, router: Dict[Type[TModel], str], client: Optional[AioBaseClient]=None):
        self.router = router
        self.client = client or get_session().create_client('dynamodb')

    def meta(self, cls: Type[TModel]) -> Meta:
        return Meta(cls, self)

    async def create_table(self, cls, *, read_cap: int, write_cap: int):
        meta = self.meta(cls)
        try:
            await self.client.create_table(
                TableName=meta.table,
                KeySchema=meta.config.key_schema,
                AttributeDefinitions=meta.config.key_attributes,
                ProvisionedThroughput={
                    'ReadCapacityUnits': read_cap,
                    'WriteCapacityUnits': write_cap,
                }
            )
        except ClientError as exc:
            if helpers.boto_err(exc, 'ResourceInUseException'):
                raise exceptions.TableAlreadyExists()
            else:
                raise

    async def create_table_if_not_exists(self, cls: Type[TModel], *, read_cap: int, write_cap: int):
        try:
            await self.create_table(cls, read_cap=read_cap, write_cap=write_cap)
        except exceptions.TableAlreadyExists:
            pass

    async def get(self, cls: Type[TModel], *key: DynamoValue) -> TModel:
        meta = self.meta(cls)
        response = await self.client.get_item(
            TableName=meta.table,
            Key=meta.key(*key)
        )
        try:
            raw_item = response['Item']
        except KeyError:
            raise exceptions.NotFound()
        else:
            return meta.from_database(raw_item)

    async def save(self, instance: TModel, guard=Guards.none):
        # TODO: Implement guards
        meta = self.meta(instance.__class__)
        data = meta.encode(instance)
        await self.client.put_item(
            TableName=meta.table,
            Item=data,
            ReturnValues='NONE',
        )

    async def delete(self, instance):
        meta = self.meta(instance.__class__)
        encoded_key = meta.get_key(instance)
        response = await self.client.delete_item(
            TableName=meta.table,
            Key=encoded_key
        )

    async def query_attrs(self, cls: Type[TModel], attrs: List[str], key: DynamoValue=NOTHING) -> AsyncIterator[Sequence[DynamoValue]]:
        meta = self.meta(cls)
        kwargs = meta.key_conditions(key)

        pe_ean = {}
        for index, attr in enumerate(attrs):
            if attr not in meta.config.fields:
                raise ValueError(f'Invalid attr {attr} not found in field definition')
            pe_ean[f'#a{index}'] = attr
        pe = ','.join(pe_ean.keys())
        kwargs.setdefault('ExpressionAttributeNames', {})
        kwargs['ExpressionAttributeNames'].update(pe_ean)
        kwargs['TableName'] = meta.table
        kwargs['ProjectionExpression'] = pe

        if key is NOTHING:
            func = self.client.scan
        else:
            func = self.client.query

        iterator = BotoCoreIterator(
            func=func,
            kwargs=kwargs,
            request_key='ExclusiveStartKey',
            response_key='LastEvaluatedKey',
            container='Items'
        )

        async for raw_item in iterator:
            item = helpers.deserialize(raw_item)
            yield tuple(item.get(attr, meta.config.fields[attr].default) for attr in attrs)

    async def query(self, cls: Type[TModel], key: DynamoValue=NOTHING, start=None, limit=None) -> AsyncIterator[TModel]:
        meta = self.meta(cls)
        kwargs = meta.key_conditions(key)
        kwargs['TableName'] = meta.table

        if key is NOTHING:
            func = self.client.scan
        else:
            func = self.client.query

        iterator = BotoCoreIterator(
            func=func,
            kwargs=kwargs,
            request_key='ExclusiveStartKey',
            response_key='LastEvaluatedKey',
            container='Items',
            initial=start,
            limit=limit
        )

        async for raw_item in iterator:
            yield meta.from_database(raw_item)

    async def count(self, cls: Type[TModel], key: DynamoValue=NOTHING) -> int:
        meta = self.meta(cls)
        kwargs = meta.key_conditions(key)
        kwargs['Select'] = 'COUNT'
        kwargs['TableName'] = meta.table

        if key is NOTHING:
            func = self.client.scan
        else:
            func = self.client.query

        iterator = BotoCoreIterator(
            func=func,
            kwargs=kwargs,
            request_key='ExclusiveStartKey',
            response_key='LastEvaluatedKey',
        )

        count = 0
        async for page in iterator:
            count += page['Count']
        return count
