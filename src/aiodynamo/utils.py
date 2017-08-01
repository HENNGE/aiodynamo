from typing import (
    AsyncIterator, Any, Dict, Awaitable, Callable, Iterable, TypeVar,
)


async def unroll(coro_func: Callable[[], Awaitable[Dict[str, Any]]],
                 inkey: str,
                 outkey: str,
                 itemkey: str,
                 start: Any=None,
                 limit: int=None,
                 limitkey: str=None,
                 process: Callable[[Any], Iterable[Any]]=lambda x: x) -> AsyncIterator[Any]:
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
                break
        if value is None:
            break
