# AsyncIO DynamoDB

[![CircleCI](https://circleci.com/gh/HENNGE/aiodynamo.svg?style=svg)](https://circleci.com/gh/HENNGE/aiodynamo)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Documentation Status](https://readthedocs.org/projects/aiodynamo/badge/?version=latest)](https://aiodynamo.readthedocs.io/en/latest/?badge=latest)

Asynchronous pythonic DynamoDB client; **2x** faster than `aiobotocore/boto3/botocore`.

## Quick start

### With httpx
Install this library

`pip install "aiodynamo[httpx]"` or, for poetry users `poetry add aiodynamo -E httpx`

Connect to DynamoDB

```py
from aiodynamo.client import Client
from aiodynamo.credentials import Credentials
from aiodynamo.http.httpx import HTTPX
from httpx import AsyncClient

    async with AsyncClient() as h:
        client = Client(HTTPX(h), Credentials.auto(), "us-east-1")
```

### With aiohttp
Install this library

`pip install "aiodynamo[aiohttp]"` or, for poetry users `poetry add aiodynamo -E aiohttp`

Connect to DynamoDB

```py
from aiodynamo.client import Client
from aiodynamo.credentials import Credentials
from aiodynamo.http.aiohttp import AIOHTTP
from aiohttp import ClientSession

    async with ClientSession() as session:
        client = Client(AIOHTTP(session), Credentials.auto(), "us-east-1")
```

### API use

```py
        table = client.table("my-table")

        # Create table if it doesn't exist
        if not await table.exists():
            await table.create(
                Throughput(read=10, write=10),
                KeySchema(hash_key=KeySpec("key", KeyType.string)),
            )

        # Create or override an item
        await table.put_item({"key": "my-item", "value": 1})
        # Get an item
        item = await table.get_item({"key": "my-item"})
        print(item)
        # Update an item, if it exists.
        await table.update_item(
            {"key": "my-item"}, F("value").add(1), condition=F("key").exists()
        )
```

## Why aiodynamo

* boto3 and botocore are synchronous. aiodynamo is built for **asynchronous** apps.
* aiodynamo is **fast**. Two times faster than aiobotocore, botocore or boto3 for operations such as query or scan.
* aiobotocore is very low level. aiodynamo provides a **pythonic API**, using modern Python features. For example, paginated APIs are automatically depaginated using asynchronous iterators.
* **Legible source code**. botocore and derived libraries generate their interface at runtime, so it cannot be inspected and isn't typed. aiodynamo is hand written code you can read, inspect and understand.
* **Pluggable HTTP client**. If you're already using an asynchronous HTTP client in your project, you can use it with aiodynamo and don't need to add extra dependencies or run into dependency resolution issues.

[Complete documentation is here](https://aiodynamo.readthedocs.io/)
