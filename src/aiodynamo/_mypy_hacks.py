from typing import TypeVar

from ._compat import Protocol

T = TypeVar("T", contravariant=True)
U = TypeVar("U", covariant=True)


# Callable is broken when used with classes, see https://github.com/python/mypy/issues/5485
class FixedCallable(Protocol[T, U]):
    def __call__(self, arg: T) -> U:
        ...
