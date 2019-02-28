Concepts
========

Main differences to aiobotocore
-------------------------------

Pythonic Argument Names
~~~~~~~~~~~~~~~~~~~~~~~

In aiobotocore, all parameters names are camel cased, in aiodynamo they follow the standard Python practice of lowercased
names with underscores.

Autopagination
~~~~~~~~~~~~~~

DynamoDB APIs that return a paginated result, such as ``scan``, ``query`` or ``count`` are automatically paginated in
aiodynamo and return asynchronous iterators over the whole result set instead.

Empty String Safe
~~~~~~~~~~~~~~~~~

DynamoDB APIs will return an error if an item contains an empty string as a value anywhere. aiodynamo removes these
values automatically.

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
