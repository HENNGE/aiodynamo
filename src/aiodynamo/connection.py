import attr
from typing import Dict, Type, AsyncIterator

from aiobotocore import get_session

from aiodynamo.types import TModel
from . import helpers
from .exceptions import NotFound, NotModified


async def _iterator(config: 'BotoCoreIterator'):
    token = None
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

    def __aiter__(self):
        return _iterator(self)


class Connection:
    def __init__(self, *, router: Dict[TModel, str], client=None):
        self.router = router
        self.client = client or get_session().create_client('dynamodb')

    async def create_table(self, cls, *, read_cap: int, write_cap: int):
        table = self.router[cls]
        config = helpers.get_config(cls)
        await self.client.create_table(
            TableName=table,
            KeySchema=config.key_schema(),
            AttributeDefinitions=config.key_attributes(),
            ProvisionedThroughput={
                'ReadCapacityUnits': read_cap,
                'WriteCapacityUnits': write_cap,
            }
        )

    async def save(self, instance):
        """
        Save instance to the database.
        """
        table = self.router[instance.__class__]
        config = helpers.get_config(instance)
        data = attr.asdict(instance)
        encoded_data = helpers.serialize(data)
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
        table = self.router[cls]
        config = helpers.get_config(instance)
        diff = helpers.get_diff(cls, attr.asdict(instance), attr.asdict(old))
        data = attr.asdict(instance)
        key = config.pop_key(data)
        encoded_key = helpers.serialize(key)
        ue, ean, eav = helpers.encode_update_expression(diff)
        await self.client.update_item(
            TableName=table,
            Key=encoded_key,
            UpdateExpression=ue,
            ExpressionAttributeNames=ean,
            ExpressionAttributeValues=eav,
        )

    async def delete(self, instance):
        """
        Delete instance from the database.
        """
        table = self.router[instance.__class__]
        config = helpers.get_config(instance)
        data = attr.asdict(instance)
        key = config.pop_key(data)
        encoded_key = helpers.serialize(key)
        await self.client.delete_item(
            TableName=table,
            Key=encoded_key,
            ReturnValues='NONE',
        )

    async def lookup(self, cls, **key):
        table = self.router[cls]
        config = helpers.get_config(cls)
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
            return cls(**helpers.deserialize(raw_item))

    async def query(self, cls: Type[TModel], **kwargs) -> AsyncIterator[TModel]:
        table = self.router[cls]
        config = helpers.get_config(cls)
        key, value = config.build_hash_key(**kwargs)
        iterator = BotoCoreIterator(
            func=self.client.query,
            kwargs={
                'TableName': table,
                'KeyConditionExpression': '#k = :v',
                'ExpressionAttributeNames': {
                    '#k': key,
                },
                'ExpressionAttributeValues': helpers.serialize({
                    ':v': value
                })
            },
            request_key='ExclusiveStartKey',
            response_key='LastEvaluatedKey',
            container='Items'
        )
        async for item in iterator:
            yield cls(**helpers.deserialize(item))
