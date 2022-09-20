Changelog
=========

22.9
----

Release Date: September 20th, 2022

* Updated minimum supported mypy version to 0.971, fixing some typing issues thanks to `@akawasaki  <https://github.com/akawasaki>`_.
* Fixed support for exceptions when using dynamodb-local thanks to `@aclemons <https://github.com/aclemons>`_.

22.8
----

Release Date: August 16th, 2022

* Added support for :py:meth:`aiodynamo.client.Client.transact_write_items` and :py:meth:`aiodynamo.client.Client.transact_get_items`
  thanks to `@nicolaszein <https://github.com/nicolaszein>`_.
* Added continuous integration for `localstack <https://github.com/localstack/localstack>`_ thanks to `@nicolaszein <https://github.com/nicolaszein>`_.

22.6
----

Release Date: June 2nd, 2022

* Removed unsupported APIs from :py:class:`aiodynamo.expressions.RangeKey`.

22.4
----

Release Date: April 20th, 2022

* Fixed handling of timeout errors in httpx.

22.2.2
------

Release Date: February 14th, 2022

* Fixed 503 errors from DynamoDB not being retried.
* Fixed mypy type hint errors introduced in 22.2

22.2.1
------

Release Date: February 7th, 2022

* Fixed a critical bug introduced in 22.2 where errors from DynamoDB were not
  correctly handled when retrying requests.

22.2
----

Release Date: February 3rd, 2022

Breaking Changes
~~~~~~~~~~~~~~~~

HTTP Client Adaptor Interface
*****************************

The :ref:`HTTP Client Adaptor Interface <http_adaptor>` has changed completely.
This change should only affect users who implement their own adaptor or use the interface
directly, for example when implementing a custom :ref:`Credentials Loader <credentials_loader>`.

Previously, the interface required the adaptor to be a class with two methods, one for GET and one for POST,
with different semantics for both. Further, adaptors were required to handle DynamoDB errors themselves.
This was confusing, led to issues with error handling and limited its use in Credential Loaders.

The new interface is a callable which takes a :py:class:`aiodynamo.http.types.Request` and returns an awaitable
:py:class:`aiodynamo.http.types.Response`.

Both built-in adaptors still use the same interface for initialization, so no changes should be required by
most users.

Retry and Throttling Unification
********************************

Previously, aiodynamo had two very similar types to handle retrying and client side throttling:
``aiodynamo.models.ThrottleConfig`` and ``aiodynamo.models.WaitConfig``. These have been combined
into :py:class:`aiodynamo.models.RetryConfig`.

``aiodynamo.models.ThrottleConfig`` was used to configure the :py:class:`aiodynamo.client.Client`
and if you used a custom configuration, you will need to replace it with the equivalent :py:class:`aiodynamo.models.RetryConfig`
implementation.

``aiodynamo.models.WaitConfig`` was used in table-level operations such as :py:meth:`aiodynamo.client.Client.create_table`
along others to wait for the table operation to actually complete. If you used a custom wait configuration,
you will need to replace it with the equivalent :py:class:`aiodynamo.models.RetryConfig` implementation.

Credentials Loader Changes
**************************

The internal, undocumented method ``fetch_with_retry`` in :py:class:`aiodynamo.credentials.Credentials` has
been removed.

Fixes
~~~~~

* :py:class:`aiodynamo.credentials.FileCredentials` now supports session tokens

21.12
-----

Release Date: December 20th, 2021

* **Breaking Change** :py:meth:`aiodynamo.expressions.F.set` no longer treats empty strings or empty bytes are removes.
* :py:class:`aiodynamo.expressions.F` now supports ``__eq__`` and `__repr__``
* Added :py:class:`aiodynamo.errors.ResourceInUse`

21.11
-----

Release Date: November 16th, 2021

* Added support for Python 3.10
* Added :py:meth:`aiodynamo.client.Client.scan_count`
* Added support for consistent reads

21.10
-----

Release Date: October 7th, 2021

* Added support for `PAY_PER_REQUEST` billing mode
* Explicit typing imports to support static type checkers


21.9
----

Release Date: September 1st, 2021

* Added the `limit` parameter to :py:meth:`aiodynamo.client.Client.count`
* Require (somewhat) newer `httpx>=0.15.0`

21.8
----

Release Date: August 24th, 2021

* Allow wider version range for `httpx` optional dependency

21.7
----

Release Date: July 30th, 2021

* Improved performance of DynamoDB Item deserialization, by @stupoid

21.6
----

Release Date: June 16th, 2021

* Added :py:meth:`aiodynamo.client.Client.batch_get`
* Added :py:meth:`aiodynamo.client.Client.batch_write`

21.5
----

Release Date: May 27th, 2021

* Added :py:meth:`aiodynamo.client.Client.query_single_page`
* Added :py:meth:`aiodynamo.client.Client.scan_single_page`
* Added :py:meth:`aiodynamo.client.Table.query_single_page`
* Added :py:meth:`aiodynamo.client.Table.scan_single_page`
* More documented APIs

20.11
-----

Release Date: November 30th, 2020

* Added :py:class:`aiodynamo.credentials.FileCredentials`
* :py:meth:`aiodynamo.credentials.Credentials.auto` will now also try :py:class:`aiodynamo.credentials.StaticCredentials`, after
  environment variables but before instance metadata.

20.10.1
-------

Release Date: October 15th, 2020

* Fixed instance metadata credentials not supporting arn-based roles.
* Added :py:class:`aiodynamo.credentials.StaticCredentials`
* Added full `PEP-484`_ type hints.

.. _PEP-484: https://www.python.org/dev/peps/pep-0484/

20.10
-----

Release Date: October 13th, 2020

* Fixed name encoding of :py:class:`aiodynamo.expressions.HashKey`

20.5
----

Release Date: May 22nd, 2020

* Removed special handling of empty strings, as DynamoDB `now supports`_ empty strings for non-key, non-index fields. Detection of empty strings is handled by the server now and will raise a :py:class:`aiodynamo.errors.ValidationError`.
* Retry API calls on internal DynamoDB errors.

.. _now supports: https://aws.amazon.com/about-aws/whats-new/2020/05/amazon-dynamodb-now-supports-empty-values-for-non-key-string-and-binary-attributes-in-dynamodb-tables/

20.4.3
------

Release Date: April 22nd, 2020

* Fixed handling of missing credentials

20.4.2
------

Release Date: April 15th, 2020

* Fix comparison conditions (``equals``, ``not_equals``, ``gt``, ``gte``, ``lt``, ``lte`` on :py:class:`aiodynamo.expressions.F`
  and :py:class:`aiodynamo.expressions.Size` via :py:meth:`aiodynamo.expressions.F.size` to support referencing other
  fields (using :py:class:`aiodynamo.expressions.F`)
* Fix timeout handling in aiohttp based client.

20.4.1
------

Release Date: April 13th, 2020

* Fixed ``put_item`` and ``delete_item`` with a ``condition`` which does not carry any values.
* Wrap underlying HTTP client errors, such as connection issues, so networking issues during
  requests are retried.

20.4
----

Release Date: April 3rd, 2020

* Fixed ``scan`` with a ``projection`` but no ``filter_expression``.
* Fixed logs leaking session tokens (request sending) and keys (metadata fetch).

20.3
----

Release Date: March 31st, 2020

* Added TTL support
* Added support for pluggable HTTP clients. Built in support for ``httpx`` and ``aiohttp``.
* Added custom client implementation.
* Added custom credentials loaders, with support for custom credential loaders.
* Fixed a typo in ``delete_item``
* Improved item deserialization performance
* Improved overall client performance, especially for query, scan and count, which are now up to twice as fast.
* Changed condition, key condition and filter expression APIs to not rely on boto3.
* Moved :py:class:`aiodynamo.models.F` to :py:class:`aiodynamo.expressions.F`.
* Removed boto3 dependency
* Removed botocore dependency
* Removed aiobotocore dependency

19.9
----

Release Date: September 6th, 2019

* Fixed bug in UpdateExpression encoder incorrectly encoding booleans as integers or vice versa.

19.3
----

Release Date: March 4th, 2019

* Initial public release
