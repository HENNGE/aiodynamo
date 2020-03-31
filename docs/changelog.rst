Changelog
=========

20.3
----

Release Date: Unreleased

* Added TTL support
* Fixed a typo in ``delete_item``
* Improved item deserialization performance
* Improved overall client performance, especially for query, scan and count.
* Changed condition, key condition and filter expression APIs.
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
