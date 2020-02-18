Development
===========

aiodynamo uses `poetry`_ to manage dependencies, make sure you have it installed.

After a git clone, run ``poetry install`` to install the dependencies, including the development dependencies.

To run the tests run ``poetry run pytest``. To also run the integration tests, set ``DYNAMODB_URL`` to the endpoint
of your DynamoDB instance. We recommend you use `dynalite`_ for local testing. You also need to set ``AWS_ACCESS_KEY_ID``
and ``AWS_SECRET_ACCESS_KEY``.

To build the documentation, enter the ``docs/`` directory and run ``poetry run make html``.


Releasing
---------

Run `poetry build -fwheel` to create a wheel, then `twine upload dist/aiodynamo-<version>-py3-none-any.whl`.

.. _poetry: https://poetry.eustace.io/
.. _dynalite: https://github.com/mhart/dynalite
