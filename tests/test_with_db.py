import asyncio
import os
import warnings
from functools import wraps

import attr
import pytest
from aiobotocore import get_session

from aiodynamo.connection import Connection
from aiodynamo.converters import Integer
from aiodynamo.exceptions import NotFound, TableAlreadyExists
from aiodynamo.helpers import remove_empty_strings
from aiodynamo.models import register, Key, Model, field, ConstKey


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
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')

    async with connection(MyModel) as db:
        instance = MyModel(h='h', r='r')
        await db.save(instance)
        db_instance = await db.get(MyModel, 'h', 'r')
        assert instance == db_instance
        await db.delete(db_instance)
        with pytest.raises(NotFound):
            await db.get(MyModel, 'h', 'r')


@runner
async def test_update():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')
        a = field('')
        b = field('')

    async with connection(MyModel) as db:
        instance = MyModel(h='h', r='r', a='a', b='b')
        await db.save(instance)
        modified = instance.modify(a='not a')
        await db.save(modified)
        db_instance = await db.get(MyModel, 'h', 'r')
        assert db_instance == MyModel(h='h', r='r', a='not a', b='b')


@runner
async def test_query():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')
        x = field(0, Integer)

    async with connection(MyModel) as db:
        instances = [
            MyModel(h='h', r=f'r{index}', x=index) for index in range(10)
        ]
        await asyncio.wait(map(db.save, instances))
        db_instances = [instance async for instance in db.query(MyModel, 'h')]
        assert instances == db_instances


@runner
async def test_scan():
    @register(hash_key=Key('h', str))
    class MyModel(Model):
        h = field('')
        x = field(0, Integer)

    async with connection(MyModel) as db:
        instances = [
            MyModel(h=f'h{index}', x=index) for index in range(10)
        ]
        await asyncio.wait(map(db.save, instances))
        scanned = [instance async for instance in db.query(MyModel)]
        db_instances = list(sorted(scanned, key=lambda x: x.h))
        assert instances == db_instances


@runner
async def test_query_without_key():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')
        x = field(0, Integer)

    async with connection(MyModel) as db:
        with pytest.raises(ValueError):
            [instance async for instance in db.query(MyModel)]


@runner
async def test_fixed_hash():
    @register(hash_key=ConstKey('h', str, 'hashkey'), range_key=Key('r', str))
    class MyModel(Model):
        h = field('hashkey')
        r = field('')

    async with connection(MyModel) as db:
        instance = MyModel(r='test')
        assert instance.h == 'hashkey'
        await db.save(instance)
        db_instance = await db.get(MyModel, 'test')
        assert instance == db_instance
        assert db_instance.h == 'hashkey'


@runner
async def test_routing():
    @register(hash_key=Key('key', str))
    class A(Model):
        key = field('')

    @register(hash_key=Key('key', str))
    class B(Model):
        key = field('')

    async with connection(A, B) as db:
        a = A(key='test')
        b = B(key='test')

        assert a != b
        assert attr.asdict(a) == attr.asdict(b)

        await db.save(a)
        await db.save(b)

        db_a = await db.get(A, 'test')
        db_b = await db.get(B, 'test')

        assert db_a != db_b

        assert db_a == a
        assert db_b == b


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


@runner
async def test_query_attrs():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')
        v = field('')

    async with connection(MyModel) as db:
        await db.save(MyModel(h='h', r='a', v='1'))
        await db.save(MyModel(h='h', r='b', v='2'))
        values = ['1', '2']
        async for v, in db.query_attrs(MyModel, ['v'], 'h'):
            expected = values.pop(0)
            assert v == expected


@runner
async def test_scan_attrs():
    @register(hash_key=Key('h', str))
    class MyModel(Model):
        h = field('')
        v = field('')

    async with connection(MyModel) as db:
        await db.save(MyModel(h='h1', v='1'))
        await db.save(MyModel(h='h2', v='2'))
        values = ['1', '2']
        actual = [v[0] async for v in db.query_attrs(MyModel, ['v'])]
        assert values == actual


@runner
async def test_query_start_key():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')
        v = field('')

    async with connection(MyModel) as db:
        await db.save(MyModel(h='h', r='a', v='1'))
        instance = MyModel(h='h', r='b', v='2')
        await db.save(instance)
        count = 0
        async for db_instance in db.query(MyModel, 'h', start={'h': {'S': 'h'}, 'r': {'S': 'a'}}):
            assert db_instance == instance
            count += 1
        assert count == 1


@runner
async def test_limit():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')
        v = field('')

    async with connection(MyModel) as db:
        instance = MyModel(h='h', r='a', v='1')
        await db.save(instance)
        await db.save(MyModel(h='h', r='b', v='2'))
        count = 0
        async for db_instance in db.query(MyModel, 'h', limit=1):
            assert db_instance == instance
            count += 1
        assert count == 1


@runner
async def test_count_scan():
    @register(hash_key=Key('num', int))
    class MyModel(Model):
        num = field(0)

    async with connection(MyModel) as db:
        assert await db.count(MyModel) == 0
        instance = MyModel(1)
        await db.save(instance)
        assert await db.count(MyModel) == 1


@runner
async def test_count_query():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')

    async with connection(MyModel) as db:
        assert await db.count(MyModel, 'hash') == 0
        await db.save(MyModel(h='nothash', r='something'))
        assert await db.count(MyModel, 'hash') == 0
        await db.save(MyModel(h='hash', r='otherthing'))
        assert await db.count(MyModel, 'hash') == 1


@runner
async def test_create_existing_table():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')

    async with connection(MyModel) as db:
        with pytest.raises(TableAlreadyExists):
            await db.create_table(MyModel, read_cap=5, write_cap=5)


@runner
async def test_create_existing_table_if_not_exists():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')

    async with connection(MyModel) as db:
        await db.create_table_if_not_exists(MyModel, read_cap=5, write_cap=5)


@runner
async def test_query_invalid_attr():
    @register(hash_key=Key('h', str), range_key=Key('r', str))
    class MyModel(Model):
        h = field('')
        r = field('')
        v = field('')

    async with connection(MyModel) as db:
        with pytest.raises(ValueError):
            [x async for x in db.query_attrs(MyModel, ['foo'], 'h')]
