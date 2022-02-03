Concepts
========

Aiodynamo does not rely on boto3, botocore or aiobotocore. This is primarily done for performance reasons,
as profiling showed that especially for querying data, boto libraries were very slow.

Main differences to boto3/botocore/aiobotocore
----------------------------------------------

Credentials
~~~~~~~~~~~

Since aiodynamo does not rely on botocore, it uses its own logic to load AWS credentials. There is built in support for
loading them from environment variables, EC2 instance metadata APIs and ECS container metadata APIs, and they're loaded
in that order of priority. You may also provide :ref:`your own logic <credentials_loader>` to load credentials.

Pythonic Argument Names
~~~~~~~~~~~~~~~~~~~~~~~

In aiobotocore, all parameters names are camel cased, in aiodynamo they follow the standard Python practice of lowercased
names with underscores.

Autopagination
~~~~~~~~~~~~~~

DynamoDB APIs that return a paginated result, such as ``scan``, ``query`` or ``count`` are automatically de-paginated in
aiodynamo and return asynchronous iterators over the whole result set instead.

Numeric type handling
~~~~~~~~~~~~~~~~~~~~~

Boto3 uses :py:class:`decimal.Decimal` for numeric values. While this is more accurate and precise, it is also often
an annoyance, as most of Python code deals with floats or ints instead. As a result, aiodynamo defaults to using floats
for numeric values returned from DynamoDB, though you can override that behaviour when creating the client.

Optional Arguments Allowed
~~~~~~~~~~~~~~~~~~~~~~~~~~

aiobotocore does not allow you to specify empty optional keyword arguments, any keyword argument given must contain a
value, not ``None``. This is not how Python usually works so aiodynamo allows you to call APIs with optional keyword
arguments set to ``None`` or other empty values.

Example, ``get_item(key, projection=None)`` is allowed in aiodynamo, but would raise an error in aiobotocore.

Typed Objects instead of Dictionaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Almost every argument to aiobotocore DynamoDB APIs are dictionaries which often require very specific keys and values.
In aiodynamo, you create instances of objects and pass those to the APIs.

Compare the following two calls::

    # aiobotocore
    create_table(
        TableName="name",
        KeySchema=[
            {
                "AttributeName": "key",
                "AttributeType": "HASH"
            }
        ],
        AttributeDefinitions=[
            {
                "AttributeName": "key",
                "AttributeType": "S"
            }
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 2
        }
    )

    # aiodynamo
    create_table(
        name="name",
        keys=KeySchema(
            hash_key=KeySpec(
                name="key",
                type=KeyType.string
            )
        ),
        throughput=Throughput(
            read=1,
            write=2
        )
    )

HTTP Client Library Independence
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While aiodynamo has built in support for some versions of aiohttp and httpx, you can use any async HTTP client
library you want by writing a small :ref:`adaptor <http_adaptor>` for it.
