from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Literal

from aiodynamo.otel.types import (
    CounterLike,
    HistogramLike,
    MeterLike,
    SpanLike,
    TracerLike,
)


@dataclass
class FakeCounter(CounterLike):
    calls: list[tuple[int | float, Dict[str, Any] | None]]

    def add(
        self, amount: int | float, attributes: Dict[str, Any] | None = None
    ) -> None:
        self.calls.append((amount, attributes))


@dataclass
class FakeHistogram(HistogramLike):
    calls: list[tuple[int | float, Dict[str, Any] | None]]

    def record(
        self, amount: int | float, attributes: Dict[str, Any] | None = None
    ) -> None:
        self.calls.append((amount, attributes))


class FakeMeter(MeterLike):
    def __init__(self) -> None:
        self.created: Dict[str, FakeCounter | FakeHistogram] = {}

    def create_counter(
        self, name: str, unit: str | None = None, description: str | None = None
    ) -> FakeCounter:
        instrument = FakeCounter([])
        self.created[name] = instrument
        return instrument

    def create_histogram(
        self, name: str, unit: str | None = None, description: str | None = None
    ) -> FakeHistogram:
        instrument = FakeHistogram([])
        self.created[name] = instrument
        return instrument


@dataclass
class FakeSpan(SpanLike):
    name: str
    attributes: Dict[str, Any]
    events: List[str]
    exceptions: List[str]

    def set_attribute(self, key: str, value: str | int) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: Dict[str, Any] | None = None) -> None:
        self.events.append(name)

    def record_exception(self, exc: BaseException) -> None:
        self.exceptions.append(type(exc).__name__)

    def __enter__(self) -> "FakeSpan":
        return self

    def __exit__(self) -> Literal[False]:
        return False


class FakeTracer(TracerLike):
    def __init__(self) -> None:
        self.spans: List[FakeSpan] = []

    @contextmanager
    def start_as_current_span(self, name: str) -> Iterator[SpanLike]:
        span = FakeSpan(name=name, attributes={}, events=[], exceptions=[])
        self.spans.append(span)
        yield span
