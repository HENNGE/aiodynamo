Changelog
=========

20.4
----

Release Date: Unreleased

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
