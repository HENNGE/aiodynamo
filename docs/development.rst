Development
===========

aiodynamo uses `poetry`_ to manage dependencies, make sure you have it installed.

After a git clone, run ``poetry install --extras aiohttp --extras httpx`` to install the dependencies, including the development dependencies.

Please ensure you have `pre-commit`_ set up so that code formatting is applied automatically.

Tests
-----

To run the tests run ``poetry run pytest``. On most systems ``poetry run pytest --numprocesses auto`` will lead to a much faster execution of the test suite.

Integration Tests
-----------------

Integration tests against DynamoDB implementations are automatically ran as part of ``poetry run pytest`` if certain environment variables are set.

Alternative DynamoDB Implementations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently, aiodynamo is tested with `dynamodb-local`_, `dynalite`_ and `ScyllaDB Alternator`_.

To test with one or more implementations, set the ``DYNAMODB_URLS`` environment variable. The value of that variable should be a space separated list of ``<name>=<config>`` pairs, where ``<config>`` is ``<url>[,<flavor>]`` with ``<flavor>`` being one of ``real``, ``dynalite``, ``scylla`` or ``other``. The flavor must be set for `ScyllaDB Alternator`_ as it has a slightly different behavior in ``DescribeTable`` compared to other implementations and `dynalite`_ as it has some known issues.

For example, to run the tests for all three instances with `dynamodb-local`_ running on port 8001, `dynalite`_ running on port 8002 and `ScyllaDB Alternator`_ running on port 8003, you would set ``DYNAMODB_URLS='dynamodb-local=http://localhost:8001 dynalite=http://localhost:8002,dynalite scylla=http://localhost:8003,scylla'``

Since these alternative implementations still require credentials to be set, set both ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` to some made up value.


AWS DynamoDB
~~~~~~~~~~~~

To run on AWS with a real DynamoDB instance, follow these steps:

* Set ``TEST_ON_AWS=true`` in your environment
* Set the ``DYNAMODB_REGION`` environment variable if you're running in a region other than ``us-east-1``.
* Make sure your environment has credentials with full access to DynamoDB, including creating and deleting tables. Normal :ref:`credentials` loading is followed.
* Optionally set ``DYNAMODB_TABLE_PREFIX`` to a string to have all test tables share a common prefix.


Docs
----

To build the documentation, enter the ``docs/`` directory and run ``poetry run make html``.

Benchmarks
----------

There are some benchmarks included in the repository under the benchmarks directory. Some of them may require extra
dependencies such as ``aiobotocore`` or ``botocore``. To run them, refer to their README file.

Releasing
---------

1. Update the version in ``pyproject.toml``
2. Update ``docs/changelog.rst``
3. Make a commit and push to Github
4. `Create a release on Github`_

.. _poetry: https://poetry.eustace.io/
.. _repository: https://github.com/mhart/dynalite
.. _container: https://hub.docker.com/r/dimaqq/dynalite/
.. _pre-commit: https://pre-commit.com/
.. _Create a release on Github: https://github.com/HENNGE/aiodynamo/releases
.. _dynamodb-local: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
.. _dynalite: https://github.com/mhart/dynalite
.. _ScyllaDB Alternator: https://docs.scylladb.com/stable/using-scylla/alternator/
