from inspect import isclass

import attr
from boto3.dynamodb.conditions import ConditionBase
from botocore.exceptions import ClientError
from typing import Dict, Type, AsyncIterator, Union, Optional, List

from aiobotocore import get_session

from . import helpers
from .types import TModel, EncodedObject
from .models import ModelConfig
from .helpers import boto_err, Substitutes
from .exceptions import NotFound, NotModified, TableAlreadyExists


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
        for item in response.get(config.container, []):
            yield item
            got += 1
            if config.limit and config.limit == got:
                break
        token = response.get(config.response_key, None)
        if token is None:
            break


@attr.s
class BotoCoreIterator:
    func = attr.ib()
    kwargs = attr.ib()
    request_key = attr.ib()
    response_key = attr.ib()
    container = attr.ib()
    initial = attr.ib(default=None)
    limit = attr.ib(default=None)

    def __aiter__(self):
        return _iterator(self)


def encode_attrs(attrs, substitutes):
    return ','.join(map(substitutes.name, attrs))


def attr_builder(attrs):
    cls = attr.make_class('PartialObject', attrs)

    def builder(raw_item: EncodedObject) -> cls:
        data = helpers.deserialize(raw_item)
        return cls(**data)

    return builder



class Connection:
    def __init__(self, *, router: Dict[TModel, str], client=None):
        self.router = router
        self.client = client or get_session().create_client('dynamodb')

    def get_table_name(self, instance_or_class: Union[TModel, Type[TModel]]) -> str:
        if isclass(instance_or_class):
            return self.router[instance_or_class]
        else:
            return self.router[instance_or_class.__class__]

    async def create_table(self, cls, *, read_cap: int, write_cap: int):
        table = self.get_table_name(cls)
        config = ModelConfig.get(cls)
        try:
            await self.client.create_table(
                TableName=table,
                KeySchema=config.key_schema(),
                AttributeDefinitions=config.key_attributes(),
                ProvisionedThroughput={
                    'ReadCapacityUnits': read_cap,
                    'WriteCapacityUnits': write_cap,
                }
            )
        except ClientError as exc:
            if boto_err(exc, 'ResourceInUseException'):
                raise TableAlreadyExists()
            else:
                raise

    async def create_table_if_not_exists(self, cls, *, read_cap: int, write_cap: int):
        try:
            await self.create_table(cls, read_cap=read_cap, write_cap=write_cap)
        except TableAlreadyExists:
            pass

    async def save(self, instance):
        table = self.get_table_name(instance)
        config = ModelConfig.get(instance)
        data = config.gather(instance)
        aliased = config.alias(data)
        encoded_data = helpers.serialize(aliased)
        await self.client.put_item(
            TableName=table,
            Item=encoded_data,
            ReturnValues='NONE',
        )

    async def update(self, instance):
        try:
            old = instance.__aiodynamodb_old__
        except AttributeError:
            raise NotModified('Cannot update non-modified instance')
        cls = instance.__class__
        table = self.get_table_name(cls)
        config = ModelConfig.get(instance)
        diff = helpers.get_diff(cls, config.gather(instance), config.gather(old))
        data = config.gather(instance)
        key = config.pop_key(data)
        encoded_key = helpers.serialize(key)
        alias_diff = config.alias(diff)
        ue, ean, eav = helpers.encode_update_expression(alias_diff)
        await self.client.update_item(
            TableName=table,
            Key=encoded_key,
            UpdateExpression=ue,
            ExpressionAttributeNames=ean,
            ExpressionAttributeValues=eav,
        )

    async def delete(self, instance):
        table = self.get_table_name(instance)
        config = ModelConfig.get(instance)
        data = config.gather(instance)
        key = config.pop_key(data)
        encoded_key = helpers.serialize(key)
        await self.client.delete_item(
            TableName=table,
            Key=encoded_key,
            ReturnValues='NONE',
        )

    async def lookup(self, cls, **key):
        table = self.get_table_name(cls)
        config = ModelConfig.get(cls)
        key = config.build_key(**key)
        encoded_key = helpers.serialize(key)
        response = await self.client.get_item(
            TableName=table,
            Key=encoded_key
        )
        try:
            raw_item = response['Item']
        except KeyError:
            raise NotFound()
        else:
            return config.from_database(raw_item)

    async def query(self,
                    cls: Type[TModel],
                    *,
                    attrs: Optional[List[str]]=None,
                    limit: Optional[int]=None,
                    start_key: Optional[Union[str, bytes, int]]=None,
                    range_filter: Optional[ConditionBase]=None,
                    **hash_key) -> AsyncIterator[TModel]:
        table = self.get_table_name(cls)
        config = ModelConfig.get(cls)
        h_key, h_value = config.build_hash_key(**hash_key)

        substitutes = Substitutes()

        sub_hash_key = substitutes.name(h_key)
        sub_hash_value = substitutes.value(h_value)

        kce = f'{sub_hash_key} = {sub_hash_value}'

        kwargs = {
            'TableName': table,
            'KeyConditionExpression': kce,
        }

        if attrs:
            kwargs['ProjectionExpression'] = encode_attrs(attrs, substitutes)

        kwargs.update({
            'ExpressionAttributeNames': substitutes.get_names(),
            'ExpressionAttributeValues': helpers.serialize(
                substitutes.get_values()
            )
        })

        if range_filter:
            info = range_filter.get_expression()
            r_key, r_value = info['values']
            sub_r_key = substitutes.name(r_key.name)
            sub_r_value = substitutes.value(r_value)
            range_filter_expr = info['format'].format(
                sub_r_key,
                sub_r_value,
                operator=info['operator'],
            )
            kce += f' AND {range_filter_expr}'

        if start_key is not None:
            start_key = helpers.serialize({
                h_key: h_value,
                **config.build_range_key(start_key)
            })

        iterator = BotoCoreIterator(
            func=self.client.query,
            kwargs=kwargs,
            request_key='ExclusiveStartKey',
            response_key='LastEvaluatedKey',
            container='Items',
            limit=limit,
            initial=start_key,
        )

        if attrs:
            builder = attr_builder(attrs)
        else:
            builder = config.from_database

        async for item in iterator:
            yield builder(item)
