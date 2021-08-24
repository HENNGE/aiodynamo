Changelog
=========

UNRELEASED
----------

Release Date: UNRELEASED

* Added the `limit` parameter to :py:meth:`aiodynamo.client.Client.count`

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
