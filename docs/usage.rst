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
6. The throttling configuration to use. An instance of :py:class:`aiodynamo.models.ThrottleConfig`. By default, if the DynamoDB rate limit is exceeded, aiodynamo will retry up for up to one minute with increasing delays.

Credentials
-----------

In most cases, ``Credentials.auto()`` will load the credentials as you'd expect. Specifically, it will try multiple
credentials providers in this order: :py:class:`aiodynamo.credentials.EnvironmentCredentials`,
:py:class:`aiodynamo.credentials.FileCredentials`, :py:class:`aiodynamo.credentials.ContainerMetadataCredentials`
and :py:class:`aiodynamo.credentials.InstanceMetadataCredentials`.

In case you want to explicitly pass the credentials from Python, use :py:class:`aiodynamo.credentials.StaticCredentials`.

.. automethod:: aiodynamo.credentials.Credentials.auto

|

.. autoclass:: aiodynamo.credentials.EnvironmentCredentials

|

.. autoclass:: aiodynamo.credentials.ContainerMetadataCredentials

|

.. autoclass:: aiodynamo.credentials.InstanceMetadataCredentials

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


The ``F`` class
---------------

The :py:class:`aiodynamo.expressions.F` class is used when building expressions that refer to fields in your
DynamoDB items. It is used to build four different type of expressions: Projection Expression, Update Expression, Filter Expression and Condition.

To refer to a top-level field, simply pass the name of the field to the class constructor. To refer to a nested field, pass the path as multiple arguments. To refer to indices in a list, pass an integer.

For example, to refer to the ``bar`` field in the second element of the ``foo`` field, use ``F("foo", 1, "bar")``.

Projection Expressions
~~~~~~~~~~~~~~~~~~~~~~

Projection expressions are built using the ``&`` operator. An instance of :py:class:`aiodynamo.expressions.F`
is a valid Projection expression too.

For example, to get the field ``foo`` and ``bar``, you would use ``F("foo") & F("bar")``.

Update Expressions
~~~~~~~~~~~~~~~~~~

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

Filter and Condition expressions have the same syntax and they are created by calling methods on instances
of :py:class:`aiodynamo.expressions.F` and combining the return values of those method calls with the
``&`` operator.

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
