Usage
=====

Client instantiation
--------------------

You should try to re-use clients, and especially HTTP clients, as much as possible. Don't create a new one for each
action.

The :py:class:`aiodynamo.client.Client` class takes three required and three optional arguments:

1. An HTTP client adaptor, conforming to the :py:class:`aiodynamo.http.base.HTTP` interface.
2. An instance of :py:class:`aiodynamo.credentials.Credentials` to authenticate with DynamoDB. You may use
   ``Credentials.auto()`` to use the default loading strategy.
3. The region your DynamoDB is in.
4. An optional endpoint URL of your DynamoDB, as a :py:class:`yarl.URL` instance. Useful when using a local DynamoDB implementation such as dynalite or dynamodb-local.
5. Which numeric type to use. This should be a callable which accepts a string as input and returns your numeric type as output. Defaults to ``float``.
6. The throttling configuration to use. An instance of :py:class:`aiodynamo.models.RetryConfig`. By default, if the DynamoDB rate limit is exceeded, aiodynamo will retry up for up to one minute with increasing delays.

Credentials
-----------

In most cases, ``Credentials.auto()`` will load the credentials as you'd expect. Specifically, it will try multiple
credentials providers in this order: :py:class:`aiodynamo.credentials.EnvironmentCredentials`,
:py:class:`aiodynamo.credentials.FileCredentials`, :py:class:`aiodynamo.credentials.ContainerMetadataCredentials`, :py:class:`aiodynamo.credentials.InstanceMetadataCredentialsV2`
and :py:class:`aiodynamo.credentials.InstanceMetadataCredentialsV1`.

In case you want to explicitly pass the credentials from Python, use :py:class:`aiodynamo.credentials.StaticCredentials`.

.. automethod:: aiodynamo.credentials.Credentials.auto

|

.. autoclass:: aiodynamo.credentials.EnvironmentCredentials

|

.. autoclass:: aiodynamo.credentials.ContainerMetadataCredentials

|

.. autoclass:: aiodynamo.credentials.InstanceMetadataCredentialsV2

|


.. autoclass:: aiodynamo.credentials.InstanceMetadataCredentialsV1

|

.. autoclass:: aiodynamo.credentials.ChainCredentials

|

.. autoclass:: aiodynamo.credentials.StaticCredentials

|

.. autoclass:: aiodynamo.credentials.FileCredentials

|

.. autoclass:: aiodynamo.credentials.Key
    :members:
    :undoc-members:


The ``Client`` class
--------------------

.. py:class:: aiodynamo.client.Client

.. automethod:: aiodynamo.client.Client.table

.. automethod:: aiodynamo.client.Client.table_exists
.. automethod:: aiodynamo.client.Client.create_table

    If ``wait_for_active`` is set to ``True``, it will wait until the table status changed into ``Active``.
    If after the defined wait time the table is not active, an exception will be raised.
    Passing a :py:class:`aiodynamo.models.PayPerRequest` object in place of a :py:class:`aiodynamo.models.Throughput`
    configuration will create a ``PAY_PER_REQUEST`` BillingMode table.

    .. seealso::
        `CreateTable - AWS API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html>`_.

.. automethod:: aiodynamo.client.Client.delete_table

    If ``wait_for_disabled`` is set to ``True``, it will wait until the table status changed into ``Disabled``.
    If after the defined wait time the table is not disabled, an exception will be raised.

    .. seealso::
        `DeleteTable - AWS API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html>`_.

.. automethod:: aiodynamo.client.Client.put_item

    .. seealso::
        `PutItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html>`_.

.. automethod:: aiodynamo.client.Client.update_item

    .. seealso::
        `UpdateItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html>`_.

.. automethod:: aiodynamo.client.Client.delete_item

    .. seealso::
        `DeleteItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html>`_.

.. automethod:: aiodynamo.client.Client.get_item

    .. seealso::
        `GetItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html>`_.

.. automethod:: aiodynamo.client.Client.query

    Aiodynamo handles pagination automatically, so this method returns an asynchronous iterator of items.

    To only retrieve a single page, use :py:meth:`aiodynamo.client.Client.query_single_page`

    .. seealso::
        `Query - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html>`_.

.. automethod:: aiodynamo.client.Client.query_single_page

    Queries a single page from DynamoDB. To automatically handle pagination, use :py:meth:`aiodynamo.client.Client.query`

    .. seealso::
        `Query - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html>`_.

.. automethod:: aiodynamo.client.Client.scan

    Aiodynamo handles pagination automatically, so this method returns an asynchronous iterator of items.

    To only retrieve a single page, use :py:meth:`aiodynamo.client.Client.scan_single_page`

    .. seealso::
        `Scan - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html>`_.

.. automethod:: aiodynamo.client.Client.scan_single_page

    Scans a single page from DynamoDB. To automatically handle pagination, use :py:meth:`aiodynamo.client.Client.scan`

    .. seealso::
        `Scan - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html>`_.

.. automethod:: aiodynamo.client.Client.count

    Aiodynamo handles pagination automatically, so this method returns the number of items.

    Queries DynamoDB and returns number of matching items, optionally bounded by ``limit`` keyword argument.

    .. seealso::
        `Query - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html>`_.

.. automethod:: aiodynamo.client.Client.scan_count

    Aiodynamo handles pagination automatically, so this method returns the number of items.

    Scans DynamoDB and returns number of matching items, optionally bounded by ``limit`` keyword argument.

    .. seealso::
        `Scan - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html>`_.

.. automethod:: aiodynamo.client.Client.batch_get

    .. seealso::
        `BatchGetItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html>`_.

.. automethod:: aiodynamo.client.Client.batch_write

    .. seealso::
        `BatchWriteItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html>`_.

.. automethod:: aiodynamo.client.Client.transact_get_items

    .. seealso::
        `TransactGetItems - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html>`_.

.. automethod:: aiodynamo.client.Client.transact_write_items

    .. seealso::
        `TransactWriteItems - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html>`_.


The ``Table`` class
-------------------

The :py:class:`aiodynamo.client.Table` class wraps all methods on :py:class:`aiodynamo.client.Client`
so you don't have to provide the table name each time.

This class should not be instantiated directly. Instead, create it by calling :py:meth:`aiodynamo.client.Client.table`.


Methods
~~~~~~~

.. py:class:: aiodynamo.client.Table

.. automethod:: aiodynamo.client.Table.exists
.. automethod:: aiodynamo.client.Table.create

    If ``wait_for_active`` is set to ``True``, it will wait until the table status changed into ``Active``.
    If after the defined wait time the table is not active, an exception will be raised.

    .. seealso::
        `CreateTable - AWS API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html>`_.

.. automethod:: aiodynamo.client.Table.delete

    If ``wait_for_disabled`` is set to ``True``, it will wait until the table status changed into ``Disabled``.
    If after the defined wait time the table is not disabled, an exception will be raised.

    .. seealso::
        `DeleteTable - AWS API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html>`_.

.. automethod:: aiodynamo.client.Table.put_item

    .. seealso::
        `PutItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html>`_.

.. automethod:: aiodynamo.client.Table.update_item

    .. seealso::
        `UpdateItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html>`_.

.. automethod:: aiodynamo.client.Table.delete_item

    .. seealso::
        `DeleteItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html>`_.

.. automethod:: aiodynamo.client.Table.get_item

    .. seealso::
        `GetItem - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html>`_.

.. automethod:: aiodynamo.client.Table.query

    Aiodynamo handles pagination automatically, so this method returns an asynchronous iterator of items.

    To only retrieve a single page, use :py:meth:`aiodynamo.client.Table.query_single_page`

    .. seealso::
        `Query - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html>`_.

.. automethod:: aiodynamo.client.Table.query_single_page

    Queries a single page from DynamoDB. To automatically handle pagination, use :py:meth:`aiodynamo.client.Table.query`

    .. seealso::
        `Query - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html>`_.

.. automethod:: aiodynamo.client.Table.scan

    Aiodynamo handles pagination automatically, so this method returns an asynchronous iterator of items.

    To only retrieve a single page, use :py:meth:`aiodynamo.client.Table.scan_single_page`

    .. seealso::
        `Scan - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html>`_.

.. automethod:: aiodynamo.client.Table.scan_single_page

    Scans a single page from DynamoDB. To automatically handle pagination, use :py:meth:`aiodynamo.client.Table.scan`

    .. seealso::
        `Scan - DynamoDB API documentation <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html>`_.

The ``F`` class
---------------

The :py:class:`aiodynamo.expressions.F` class is used when building expressions that refer to fields in your
DynamoDB items. It is used to build four different type of expressions: Projection Expression, Update Expression, Filter Expression and Condition.

To refer to a top-level field, simply pass the name of the field to the class constructor. To refer to a nested field, pass the path as multiple arguments. To refer to indices in a list, pass an integer.

For example, to refer to the ``bar`` field in the second element of the ``foo`` field, use ``F("foo", 1, "bar")``.

Projection Expressions
~~~~~~~~~~~~~~~~~~~~~~


.. py:class:: aiodynamo.expressions.ProjectionExpression

    Abstract base class to represent projection expresssions.

    Projection expressions are built using the ``&`` operator. An instance of :py:class:`aiodynamo.expressions.F`
    is a valid Projection expression too.

    For example, to get the field ``foo`` and ``bar``, you would use ``F("foo") & F("bar")``.


Update Expressions
~~~~~~~~~~~~~~~~~~

.. py:class:: aiodynamo.expressions.UpdateExpression

    Update expressions are created by calling methods on instances of :py:class:`aiodynamo.expressions.F` and
    combining the return values of those method calls with the ``&`` operator.

.. automethod:: aiodynamo.expressions.F.set
.. automethod:: aiodynamo.expressions.F.set_if_not_exists
.. automethod:: aiodynamo.expressions.F.change
.. automethod:: aiodynamo.expressions.F.append
.. automethod:: aiodynamo.expressions.F.remove
.. automethod:: aiodynamo.expressions.F.add
.. automethod:: aiodynamo.expressions.F.delete

Filter Expression and Condition Expressions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


.. py:class:: aiodynamo.expressions.Condition

    Abstract base class of conditions and filters.

    Filter and Condition expressions have the same syntax and they are created by calling methods on instances
    of :py:class:`aiodynamo.expressions.F` and combining the return values of those method calls with the
    ``&`` or ``|`` operators. To negate a condition, use the ``~`` infix operator.


.. automethod:: aiodynamo.expressions.F.does_not_exist
.. automethod:: aiodynamo.expressions.F.exists
.. automethod:: aiodynamo.expressions.F.attribute_type
.. automethod:: aiodynamo.expressions.F.begins_with
.. automethod:: aiodynamo.expressions.F.between
.. automethod:: aiodynamo.expressions.F.contains
.. automethod:: aiodynamo.expressions.F.is_in
.. automethod:: aiodynamo.expressions.F.gt
.. automethod:: aiodynamo.expressions.F.gte
.. automethod:: aiodynamo.expressions.F.lt
.. automethod:: aiodynamo.expressions.F.lte
.. automethod:: aiodynamo.expressions.F.equals
.. automethod:: aiodynamo.expressions.F.not_equals
.. automethod:: aiodynamo.expressions.F.size

.. autoclass:: aiodynamo.expressions.Size
    :members:
    :undoc-members:


Key conditions
--------------

Key conditions are created using the :py:class:`aiodynamo.expressions.HashKey` and optionally the
:py:class:`aiodynamo.expressions.RangeKey` classes.

.. autoclass:: aiodynamo.expressions.HashKey

|

.. autoclass:: aiodynamo.expressions.RangeKey
    :members: begins_with, between, gt, gte, lt, lte, equals
    :undoc-members:


Models
------
.. autoclass:: aiodynamo.models.PayPerRequest
    :undoc-members:

.. autoclass:: aiodynamo.models.Throughput
    :members: read, write
    :undoc-members:

.. autoclass:: aiodynamo.models.KeySchema
    :members: hash_key, range_key
    :undoc-members:

.. autoclass:: aiodynamo.models.LocalSecondaryIndex
    :members: name, projection, schema
    :undoc-members:

.. autoclass:: aiodynamo.models.GlobalSecondaryIndex
    :members: name, projection, schema, throughput
    :undoc-members:

.. autoclass:: aiodynamo.models.StreamSpecification
    :members: enabled, view_type
    :undoc-members:

.. autoclass:: aiodynamo.models..StreamViewType
    :members: keys_only, new_image, old_image, new_and_old_images
    :undoc-members:

.. autoclass:: aiodynamo.models.RetryConfig
    :members: time_limit_secs, default, default_wait_config, delays

.. autoclass:: aiodynamo.models.ReturnValues
    :members: none, all_old, updated_old, all_new, updated_new
    :undoc-members:

.. autoclass:: aiodynamo.models.Projection
    :members: type, attrs
    :undoc-members:

.. autoclass:: aiodynamo.models.ProjectionType
    :members: all, keys_only, include
    :undoc-members:

.. autoclass:: aiodynamo.models.BatchGetRequest
    :members: keys, projection
    :undoc-members:

.. autoclass:: aiodynamo.models.BatchGetResponse
    :members: items, unprocessed_keys
    :undoc-members:

.. autoclass:: aiodynamo.models.BatchWriteRequest
    :members: keys_to_delete, items_to_put
    :undoc-members:

.. autoclass:: aiodynamo.models.BatchWriteResult
    :members: undeleted_keys, unput_items
    :undoc-members:

Operations
----------

.. autoclass:: aiodynamo.operations.Get
    :members: table, key, projection
    :undoc-members:

.. autoclass:: aiodynamo.operations.Put
    :members: table, item, condition
    :undoc-members:

.. autoclass:: aiodynamo.operations.Update
    :members: table, key, expression, condition
    :undoc-members:

.. autoclass:: aiodynamo.operations.Delete
    :members: table, key, condition
    :undoc-members:

.. autoclass:: aiodynamo.operations.ConditionCheck
    :members: table, key, condition
    :undoc-members:
