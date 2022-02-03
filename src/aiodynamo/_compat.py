import sys

# cannot use try/except ImportError or mypy fails
if sys.version_info >= (3, 8):
    from typing import Literal, Protocol, TypedDict
else:
    from typing_extensions import Literal, Protocol, TypedDict

__all__ = ("TypedDict", "Literal", "Protocol")
