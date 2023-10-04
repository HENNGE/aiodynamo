Development
===========

aiodynamo uses `poetry`_ to manage dependencies, make sure you have it installed.

After a git clone, run ``poetry install --extras aiohttp --extras httpx`` to install the dependencies,
including the development dependencies.

Please ensure you have `pre-commit`_ set up so that code formatting is applied automatically.

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
``dynalite`` (repository_, container_) for local testing. You also need to set ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``.

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
