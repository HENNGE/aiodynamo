Advanced
========

.. _http_adaptor:

Custom HTTP Client Adaptor
--------------------------

Aiodynamo is not strongly tied to any HTTP client library or version thereof. Default
adaptors for httpx and aiohttp are provided, but it is perfectly valid to use your own
adaptor that uses a different library or versions of httpx or aiohttp not supported by
the default adaptors.

An HTTP client adaptor is a callable which takes a :py:class:`aiodynamo.http.types.Request`
as input and either returns a :py:class:`aiodynamo.http.types.Response`.

If the request fails due to a connection error or some other client error, the adaptor should
raise a :py:class:`aiodynamo.http.types.RequestFailed` with the client exception as the first
argument. If the HTTP client library handles timeouts, those timeouts should raise an
:py:class:`asyncio.TimeoutError` exception.


.. autoclass:: aiodynamo.http.types.Request

    .. py:attribute:: method
        :type: Union[Literal["GET"], Literal["POST"]]

    .. py:attribute:: url
        :type: str

    .. py:attribute:: headers
        :type: Optional[Dict[str, str]]

    .. py:attribute:: body
        :type: Optional[bytes]

.. autoclass:: aiodynamo.http.types.Response

    .. py:attribute:: status
        :type: int

    .. py:attribute:: body
        :type: bytes


.. autoexception:: aiodynamo.http.types.RequestFailed

.. _credentials_loader:

Custom Credentials Loader
-------------------------

If the methods to load credentials provided by aiodynamo are not sufficient for
your use case, you can tell aiodynamo how to load credentials by creating a class
which conforms to the :py:class:`aiodynamo.credentials.Credentials` interface.

.. autoclass:: aiodynamo.credentials.Credentials
    :members: get_key,invalidate,is_disabled
