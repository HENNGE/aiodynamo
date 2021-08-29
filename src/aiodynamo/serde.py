from typing import Any, Callable, Mapping

try:
    dumps: Callable[[Mapping[str, Any]], bytes]
    from orjson import dumps as dumps, loads as loads
except ImportError:
    from json import dumps as json_dumps, loads as loads

    def dumps(payload: Mapping[str, Any]) -> bytes:
        return json_dumps(payload, separators=(",", ":")).encode("utf8")
