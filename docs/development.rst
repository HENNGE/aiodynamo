Development
===========

aiodynamo uses `poetry`_ to manage dependencies, make sure you have it installed.

After a git clone, run ``poetry install`` to install the dependencies, including the development dependencies.

Tests
-----

To run the tests run ``poetry run pytest``. Use ``DYNAMODB_REGION`` to specify the region to use, it defaults to
``us-east-1``.

On AWS
~~~~~~

Tests can be run directly on AWS against a real DynamoDB instance. To do so, set the ``TEST_ON_AWS`` environment
variable to ``true``. You may want to set ``DYNAMODB_TABLE_PREFIX`` to some string in case the test suite fails and
leaves behind orphaned tables.

Locally
~~~~~~~

To also run the integration tests, set ``DYNAMODB_URL`` to the endpoint of a DynamoDB instance. We recommend you use
`dynalite`_ for local testing. You also need to set ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``.

Docs
----

To build the documentation, enter the ``docs/`` directory and run ``poetry run make html``.

Benchmarks
----------

There are some benchmarks included in the repository under the benchmarks directory. Some of them may require extra
dependencies such as ``aiobotocore`` or ``botocore``. To run them, refer to their README file.

Releasing
---------

Run `poetry build -fwheel` to create a wheel, then `twine upload dist/aiodynamo-<version>-py3-none-any.whl`.

.. _poetry: https://poetry.eustace.io/
.. _dynalite: https://github.com/mhart/dynalite
