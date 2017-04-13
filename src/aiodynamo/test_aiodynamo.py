import asyncio
from functools import wraps

import attr

from . import Connection, model, Keys, field, hash_key, range_key
from .exceptions import NotModified, NotFound

import multiprocessing

import pytest
from aiobotocore import get_session
from moto.server import DomainDispatcherApplication, create_backend_app
from werkzeug.serving import make_server


def dynamo(port_queue):
    main_app = DomainDispatcherApplication(
        create_backend_app,
        service='dynamodb2'
    )
    server = make_server(host='localhost', port=0, app=main_app, threaded=True)
    port_queue.put(server.port)
    server.serve_forever()


@pytest.fixture()
def dynamo_client():
    port_queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=dynamo, args=(port_queue,))
    client = None
    try:
        process.start()
        port = port_queue.get(timeout=5)
        client = get_session().create_client(
            'dynamodb',
            endpoint_url=f'http://localhost:{port}',
            region_name='us-east-1',
            aws_access_key_id='local',
            aws_secret_access_key='local',
        )
        yield client
    finally:
        if client:
            client.close()
        process.terminate()


def runner(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        task = loop.create_task(func(*args, **kwargs))
        loop.run_until_complete(task)
    return wrapper


@runner
async def test_basic(dynamo_client):
    @model(keys=Keys.HashRange)
    class MyModel:
        r = range_key(str)
        h = hash_key(str)

    router = {
        MyModel: 'my-table'
    }

    db = Connection(router=router, client=dynamo_client)
    await db.create_table(MyModel, read_cap=5, write_cap=5)
    instance = MyModel(r='r', h='h')
    await db.save(instance)
    db_instance = await db.lookup(MyModel, h='h', r='r')
    assert instance == db_instance
    await db.delete(db_instance)
    with pytest.raises(NotFound):
        await db.lookup(MyModel, h='h', r='r')


@runner
async def test_update(dynamo_client):
    @model(keys=Keys.HashRange)
    class MyModel:
        r = range_key(str)
        h = hash_key(str)
        a = field()
        b = field()

    router = {
        MyModel: 'my-table'
    }

    db = Connection(router=router, client=dynamo_client)
    await db.create_table(MyModel, read_cap=5, write_cap=5)
    instance = MyModel(r='r', h='h', a='a', b='b')
    await db.save(instance)
    modified = instance.modify(a= 'not a')
    with pytest.raises(NotModified):
        await db.update(instance)
    await db.update(modified)
    db_instance = await db.lookup(MyModel, r='r', h='h')
    assert db_instance == MyModel(r='r', h='h', a='not a', b='b')


@runner
async def test_query(dynamo_client):
    @model(keys=Keys.HashRange)
    class MyModel:
        r = range_key(str)
        h = hash_key(str)
        x = attr.ib()
    router = {
        MyModel: 'my-table'
    }
    db = Connection(router=router, client=dynamo_client)
    await db.create_table(MyModel, read_cap=5, write_cap=5)
    instances = [
        MyModel(r=f'r{index}', h='h', x=index) for index in range(10)
    ]
    await asyncio.wait(map(db.save, instances))
    db_instances = [instance async for instance in db.query(MyModel, h='h')]
    assert instances == db_instances


@runner
async def test_fixed_hash(dynamo_client):
    @model(keys=Keys.HashRange)
    class MyModel:
        h = hash_key(str, constant='hashkey')
        r = range_key(str)
    router = {
        MyModel: 'my-table'
    }
    db = Connection(router=router, client=dynamo_client)
    await db.create_table(MyModel, read_cap=5, write_cap=5)
    instance = MyModel(r='test')
    assert instance.h == 'hashkey'
    await db.save(instance)
    db_instance = await db.lookup(MyModel, r='test')
    assert instance == db_instance
    assert db_instance.h == 'hashkey'
