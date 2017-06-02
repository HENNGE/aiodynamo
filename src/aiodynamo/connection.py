from typing import (
    Dict, Type, Optional, List, Iterator, Tuple, TypeVar,
    Sequence,
    AsyncIterator,
)

from aiobotocore import get_session
import attr
from aiobotocore.client import AioBaseClient
from boto3.dynamodb import conditions
from botocore.exceptions import ClientError

from aiodynamo.types import DynamoValue
from . import helpers, exceptions, models
from .models import TModel


def key_to_condition(key: Dict[str, DynamoValue]) -> Tuple[str, Dict[str, str], Dict[str, DynamoValue]]:
    condition = None
    for name, value in key.items():
        if condition is None:
            condition = conditions.Key(name).eq(value)
        else:
            condition &= conditions.Key(name).eq(value)

    builder = conditions.ConditionExpressionBuilder()
    return builder.build_expression(condition, True)


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


class Connection:
    def __init__(self, *, router: Dict[Type[TModel], str], client: Optional[AioBaseClient]=None):
        self.router = router
        self.client = client or get_session().create_client('dynamodb')

    async def create_table(self, cls, *, read_cap: int, write_cap: int):
        table = self.router[cls]
        config = models.get_config(cls)
        try:
            await self.client.create_table(
                TableName=table,
                KeySchema=config.key_schema,
                AttributeDefinitions=config.key_attributes,
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
        table = self.router[cls]
        config = models.get_config(cls)
        key = config.encode_key(*key)
        encoded_key = helpers.serialize(key)
        response = await self.client.get_item(
            TableName=table,
            Key=encoded_key
        )
        try:
            raw_item = response['Item']
        except KeyError:
            raise exceptions.NotFound()
        else:
            data = helpers.deserialize(raw_item)
            return config.from_database(data)

    async def save(self, instance: TModel):
        cls = instance.__class__
        table = self.router[cls]
        config = models.get_config(cls)
        data = config.encode(instance)
        encoded_data = helpers.serialize(data)
        await self.client.put_item(
            TableName=table,
            Item=encoded_data,
            ReturnValues='NONE',
        )

    async def delete(self, instance):
        cls = instance.__class__
        table = self.router[cls]
        config = models.get_config(cls)
        key = config.get_key(instance)
        encoded_key = helpers.serialize(key)
        response = await self.client.delete_item(
            TableName=table,
            Key=encoded_key
        )

    async def query_attrs(self, cls: Type[TModel], key: DynamoValue, attrs: List[str]) -> AsyncIterator[Sequence[DynamoValue]]:
        table = self.router[cls]
        config = models.get_config(cls)
        key = config.encode_key(key)
        kce, ean, eav = key_to_condition(key)

        pe_ean = {}
        for index, attr in enumerate(attrs):
            if attr not in config.fields:
                raise ValueError(f'Invalid attr {attr} not found in field definition')
            pe_ean[f'#a{index}'] = attr
        pe = ','.join(pe_ean.keys())
        ean.update(pe_ean)

        kwargs = {
            'KeyConditionExpression': kce,
            'TableName': table,
            'ProjectionExpression': pe,
        }
        if ean:
            kwargs['ExpressionAttributeNames'] = ean
        if eav:
            kwargs['ExpressionAttributeValues'] = helpers.serialize(eav)
        iterator = BotoCoreIterator(
            func=self.client.query,
            kwargs=kwargs,
            request_key='ExclusiveStartKey',
            response_key='LastEvaluatedKey',
            container='Items'
        )

        async for raw_item in iterator:
            item = helpers.deserialize(raw_item)
            yield tuple(item.get(attr, config.fields[attr].default) for attr in attrs)

    async def query(self, cls: Type[TModel], key: DynamoValue, start=None, limit=None) -> AsyncIterator[TModel]:
        table = self.router[cls]
        config = models.get_config(cls)
        key = config.encode_key(key)
        kce, ean, eav = key_to_condition(key)

        kwargs = {
            'KeyConditionExpression': kce,
            'TableName': table,
        }
        if ean:
            kwargs['ExpressionAttributeNames'] = ean
        if eav:
            kwargs['ExpressionAttributeValues'] = helpers.serialize(eav)
        iterator = BotoCoreIterator(
            func=self.client.query,
            kwargs=kwargs,
            request_key='ExclusiveStartKey',
            response_key='LastEvaluatedKey',
            container='Items',
            initial=start,
            limit=limit
        )

        async for raw_item in iterator:
            data = helpers.deserialize(raw_item)
            yield config.from_database(data)

    async def count(self, cls: Type[TModel], *key: DynamoValue) -> int:
        table = self.router[cls]
        config = models.get_config(cls)
        key = config.encode_key(*key)
        kce, ean, eav = key_to_condition(key)

        kwargs = {
            'KeyConditionExpression': kce,
            'Select': 'COUNT',
            'TableName': table,
        }
        if ean:
            kwargs['ExpressionAttributeNames'] = ean
        if eav:
            kwargs['ExpressionAttributeValues'] = helpers.serialize(eav)
        iterator = BotoCoreIterator(
            func=self.client.query,
            kwargs=kwargs,
            request_key='ExclusiveStartKey',
            response_key='LastEvaluatedKey',
        )

        count = 0
        async for page in iterator:
            count += page['Count']
        return count
