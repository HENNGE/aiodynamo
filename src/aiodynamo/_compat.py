import sys

# cannot use try/except ImportError or mypy fails
if sys.version_info >= (3, 8):
    from typing import Literal, TypedDict
else:
    from typing_extensions import Literal, TypedDict

__all__ = ("TypedDict", "Literal")
