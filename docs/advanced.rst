Advanced
========

Custom HTTP client adaptor
--------------------------

If for some reason you want to use neither aiohttp nor httpx, you can adapt your
own HTTP client for usage with aiodynamo. To do so, create a class that conforms
to the :py:class:`aiodynamo.http.base.HTTP` interface. Errors should be wrapped in
a :py:exc:`aiodynamo.http.base.RequestFailed`.

.. autoclass:: aiodynamo.http.base.HTTP
    :members:

.. autoexception:: aiodynamo.http.base.RequestFailed


Custom Credentials loader
-------------------------

If you need a special way to load credentials, you can do so by creating a class
which conforms to thee :py:class:`aiodynamo.credentials.Credentials` interface.

.. autoclass:: aiodynamo.credentials.Credentials
    :members: get_key,invalidate,is_disabled
