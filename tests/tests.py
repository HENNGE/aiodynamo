import asyncio
import os
import warnings
from functools import wraps
import itertools

import attr
import pytest
from aiobotocore import get_session

from aiodynamo import Connection, model, Keys, field, hash_key, range_key
from aiodynamo.exceptions import NotModified, NotFound, InvalidModel
from aiodynamo.helpers import remove_empty_strings


@attr.s
class ConnectionContext:
    models = attr.ib()

    @staticmethod
    def table_name(model):
        return f'table-{model.__name__}'

    async def __aenter__(self) -> Connection:
        try:
            endpoint_url = os.environ['DYNAMODB_ENDPOINT_URL']
        except KeyError:
            raise pytest.skip('No endpoint url specified')
        client = get_session().create_client(
            'dynamodb',
            endpoint_url=endpoint_url,
            region_name='fake-region',
            aws_access_key_id='fake-aws-access-key-id',
            aws_secret_access_key='fake-aws-secret-access-key',
        )
        router = {
            model: self.table_name(model)
            for model in self.models
        }
        self.db = Connection(router=router, client=client)
        await asyncio.wait([
            self.db.create_table(model, read_cap=5, write_cap=5)
            for model in self.models
        ])
        return self.db

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.wait([
            self.db.client.delete_table(TableName=self.table_name(model))
            for model in self.models
        ])
        self.db.client.close()


def connection(*models):
    return ConnectionContext(models)


def check_unawaited(logs):
    return [
        str(log.message) for log in logs
        if not str(log.message).endswith(' was never awaited.')
    ]


def runner(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with warnings.catch_warnings(record=True) as logs:
            loop = asyncio.get_event_loop()
            task = loop.create_task(func(*args, **kwargs))
            loop.run_until_complete(task)
            assert not check_unawaited(logs)
    return wrapper


@runner
async def test_basic():
    @model(keys=Keys.HashRange)
    class MyModel:
        r = range_key(str)
        h = hash_key(str)

    async with connection(MyModel) as db:
        instance = MyModel(r='r', h='h')
        await db.save(instance)
        db_instance = await db.lookup(MyModel, h='h', r='r')
        assert instance == db_instance
        await db.delete(db_instance)
        with pytest.raises(NotFound):
            await db.lookup(MyModel, h='h', r='r')


@runner
async def test_update():
    @model(keys=Keys.HashRange)
    class MyModel:
        r = range_key(str)
        h = hash_key(str)
        a = field()
        b = field()

    async with connection(MyModel) as db:
        instance = MyModel(r='r', h='h', a='a', b='b')
        await db.save(instance)
        modified = instance.modify(a= 'not a')
        with pytest.raises(NotModified):
            await db.update(instance)
        await db.update(modified)
        db_instance = await db.lookup(MyModel, r='r', h='h')
        assert db_instance == MyModel(r='r', h='h', a='not a', b='b')


@runner
async def test_query():
    @model(keys=Keys.HashRange)
    class MyModel:
        r = range_key(str)
        h = hash_key(str)
        x = field()

    async with connection(MyModel) as db:
        instances = [
            MyModel(r=f'r{index}', h='h', x=index) for index in range(10)
        ]
        await asyncio.wait(map(db.save, instances))
        db_instances = [instance async for instance in db.query(MyModel, h='h')]
        assert instances == db_instances


@runner
async def test_fixed_hash():
    @model(keys=Keys.HashRange)
    class MyModel:
        h = hash_key(str, constant='hashkey')
        r = range_key(str)

    async with connection(MyModel) as db:
        instance = MyModel(r='test')
        assert instance.h == 'hashkey'
        await db.save(instance)
        db_instance = await db.lookup(MyModel, r='test')
        assert instance == db_instance
        assert db_instance.h == 'hashkey'


@runner
async def test_alias():
    @model(keys=Keys.Hash)
    class MyModel:
        h = hash_key(str, alias='hash_key')

    async with connection(MyModel) as db:
        response = await db.client.describe_table(
            TableName=ConnectionContext.table_name(MyModel)
        )
        assert response['Table']['KeySchema'] == [{
            'AttributeName': 'hash_key',
            'KeyType': 'HASH'
        }]
        instance = MyModel(h='h')
        await db.save(instance)
        db_instance = await db.lookup(MyModel, h='h')
        assert instance == db_instance


@runner
async def test_alias_hr():
    @model(keys=Keys.HashRange)
    class MyModel:
        h = hash_key(str, alias='hash_key')
        r = range_key(str, alias='range_key')

    async with connection(MyModel) as db:
        response = await db.client.describe_table(
            TableName=ConnectionContext.table_name(MyModel)
        )
        assert response['Table']['KeySchema'] == [{
            'AttributeName': 'hash_key',
            'KeyType': 'HASH'
        },{
            'AttributeName': 'range_key',
            'KeyType': 'RANGE'
        }]
        instance = MyModel(h='hv', r='rv')
        await db.save(instance)
        db_instance = await db.lookup(MyModel, h='hv', r='rv')
        assert instance == db_instance


@runner
async def test_non_field():
    @model(keys=Keys.Hash)
    class MyModel:
        key = hash_key(str)
        attr = attr.ib(default='not-attr')

    async with connection(MyModel) as db:
        instance = MyModel(key='mykey', attr='attr')
        await db.save(instance)
        db_instance = await db.lookup(MyModel, key='mykey')
        assert db_instance != instance
        assert db_instance.attr == 'not-attr'
        assert attr.assoc(db_instance, attr='attr') == instance


@runner
async def test_to_from_db_convert():
    def prefixed(prefix: str):
        def to_db(suffix: str) -> str:
            return prefix + suffix

        def from_db(value: str) -> str:
            if value.startswith(prefix):
                return value[len(prefix):]
            else:
                return value

        to_db.from_db = from_db
        return to_db

    @model(keys=Keys.Hash)
    class MyModel:
        prefixed_key = hash_key(str, convert=prefixed('TEST'))

        @property
        def unprefixed(self):
            return self.prefixed_key[4:]

    async with connection(MyModel) as db:
        instance = MyModel(prefixed_key='suffix')
        assert instance.prefixed_key == 'TESTsuffix'
        assert instance.unprefixed == 'suffix'
        await db.save(instance)
        response = await db.client.get_item(
            TableName=ConnectionContext.table_name(MyModel),
            Key={'prefixed_key': {'S': 'TESTsuffix'}}
        )
        assert response['Item']['prefixed_key'] == {'S': 'TESTsuffix'}
        db_instance = await db.lookup(MyModel, prefixed_key='suffix')
        assert db_instance == instance


@runner
async def test_auto_field():
    counter = itertools.count(start=1)

    @model(keys=Keys.Hash)
    class MyModel:
        key = hash_key(str)
        auto_field = field(auto=lambda: next(counter), default=0)

    async with connection(MyModel) as db:
        instance = MyModel(key='test')
        assert instance.auto_field == 0
        await db.save(instance)
        db_instance = await db.lookup(MyModel, key='test')
        assert db_instance != instance
        assert db_instance.auto_field == 1


@runner
async def test_routing():
    @model(keys=Keys.Hash)
    class A:
        key = hash_key(str)

    @model(keys=Keys.Hash)
    class B:
        key = hash_key(str)

    async with connection(A, B) as db:
        a = A(key='test')
        b = B(key='test')

        assert a != b
        assert attr.asdict(a) == attr.asdict(b)

        await db.save(a)
        await db.save(b)

        db_a = await db.lookup(A, key='test')
        db_b = await db.lookup(B, key='test')

        assert db_a != db_b

        assert db_a == a
        assert db_b == b


@runner
@pytest.mark.parametrize('test_value', [
    '',
    [''],
    {'key': ''},
    {'key1': '', 'key2': 'key2'},
    {'', 'notempty'},
    {''},
    0,
    [0],
    [None],
    None
], ids=repr)
async def test_empty_strings(test_value):
    @model(keys=Keys.Hash)
    class MyModel:
        key = hash_key(str)
        value = field(default=type(test_value))

    async with connection(MyModel) as db:
        instance = MyModel(key='key', value=test_value)
        await db.save(instance)
        db_instance = await db.lookup(MyModel, key='key')
        assert db_instance == instance


@pytest.mark.parametrize('value, expected', [
    ('', ''),
    ([''], []),
    ({'key': ''}, {}),
    ({'key1': '', 'key2': 'key2'}, {'key2': 'key2'}),
    ({'', 'notempty'}, {'notempty'}),
    ({''}, set()),
    (0, 0),
    ([0], [0]),
    ([None], [None]),
    (None, None)
], ids=repr)
def test_remove_empty_strings(value, expected):
    assert remove_empty_strings(value) == expected


def test_double_hash_key():
    class MyModel:
        h1 = hash_key(str)
        h2 = hash_key(str)

    with pytest.raises(InvalidModel):
        model(keys=Keys.HashRange)(MyModel)


@runner
async def test_projection_expression():
    @model(keys=Keys.HashRange)
    class MyModel:
        h = hash_key(str)
        r = range_key(str)
        v = field()

    async with connection(MyModel) as db:
        await db.save(MyModel(h='h', r='a', v='1'))
        await db.save(MyModel(h='h', r='b', v='2'))
        values = ['1', '2']
        async for partial in db.query(MyModel, h='h', attrs=['v']):
            expected = values.pop(0)
            assert partial.v == expected
            assert attr.asdict(partial) == {'v': expected}


@runner
async def test_start_key():
    @model(keys=Keys.HashRange)
    class MyModel:
        h = hash_key(str)
        r = range_key(str)
        v = field()

    async with connection(MyModel) as db:
        await db.save(MyModel(h='h', r='a', v='1'))
        instance = MyModel(h='h', r='b', v='2')
        await db.save(instance)
        count = 0
        async for db_instance in db.query(MyModel, h='h', start_key='a'):
            assert db_instance == instance
            count += 1
        assert count == 1


@runner
async def test_limit():
    @model(keys=Keys.HashRange)
    class MyModel:
        h = hash_key(str)
        r = range_key(str)
        v = field()

    async with connection(MyModel) as db:
        instance = MyModel(h='h', r='a', v='1')
        await db.save(instance)
        await db.save(MyModel(h='h', r='b', v='2'))
        count = 0
        async for db_instance in db.query(MyModel, h='h', limit=1):
            assert db_instance == instance
            count += 1
        assert count == 1
