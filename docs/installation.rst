Installation
============

Aiodynamo requires Python 3.7 or higher.

Aiodynamo requires an HTTP client to function, but abstracts the client away.
You can either use one of the built-in adaptors for `aiohttp`_ or `httpx`_, or
use your own client by writing an adaptor for it.

If you wish to use the aiohttp adaptor, install ``aiodynamo[aiohttp]``, for httpx
use ``aiodynamo[httpx]``.

Aiodynamo uses `CalVer`_, not semantic versioning. Make sure to check the :doc:`changelog`
before upgrading.

.. _aiohttp: https://docs.aiohttp.org/en/stable/
.. _httpx: https://www.python-httpx.org/
.. _CalVer: https://calver.org/
