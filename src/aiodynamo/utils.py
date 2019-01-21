import collections
from functools import wraps
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterable, Tuple, Union

from aiodynamo.types import Deserializer, DynamoItem, EMPTY, Item, Serializer


async def unroll(
    coro_func: Callable[[], Awaitable[Dict[str, Any]]],
    inkey: str,
    outkey: str,
    itemkey: str,
    start: Any = None,
    limit: int = None,
    limitkey: str = None,
    process: Callable[[Any], Iterable[Any]] = lambda x: x,
) -> AsyncIterator[Any]:
    value = start
    got = 0
    while True:
        kwargs = {}
        if value is not None:
            kwargs[inkey] = value
        if limit:
            want = limit - got
            kwargs[limitkey] = want
        resp = await coro_func(**kwargs)
        value = resp.get(outkey, None)
        items = resp.get(itemkey, [])
        for item in process(items):
            yield item

            got += 1
            if limit and got >= limit:
                return

        if value is None:
            break


def ensure_not_empty(value):
    if value is None:
        return value

    elif isinstance(value, (bytes, str)):
        if not value:
            return EMPTY

    elif isinstance(value, collections.abc.Mapping):
        value = dict(remove_empty_strings(value))
    elif isinstance(value, collections.abc.Iterable):
        value = value.__class__(
            item for item in map(ensure_not_empty, value) if item is not EMPTY
        )
    return value


def remove_empty_strings(data: Item) -> Iterable[Tuple[str, Any]]:
    for key, value in data.items():
        value = ensure_not_empty(value)
        if value is not EMPTY:
            yield key, value


def py2dy(data: Union[Item, None]) -> Union[DynamoItem, None]:
    if data is None:
        return data

    return {
        key: Serializer.serialize(value) for key, value in remove_empty_strings(data)
    }


def dy2py(data: DynamoItem) -> Item:
    return {key: Deserializer.deserialize(value) for key, value in data.items()}


def check_empty_value(meth):
    @wraps(meth)
    def wrapper(self, *args, **kwargs):
        if self.value is EMPTY:
            return self.value

        return meth(self, *args, **kwargs)

    return wrapper


def clean(**kwargs):
    return {
        key: value for key, value in kwargs.items() if value or isinstance(value, bool)
    }


def maybe_immutable(thing: Any):
    if isinstance(thing, list):
        return tuple(thing)

    elif isinstance(thing, set):
        return frozenset(thing)

    else:
        return thing
