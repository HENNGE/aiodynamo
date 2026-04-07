from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Protocol, ContextManager, Any


class SpanLike(Protocol):
    def set_attribute(self, name: str, value: Any) -> None: ...
    def record_exception(self, exception: BaseException) -> None: ...

    def add_event(
        self, name:
        str,
        attributes: dict[str, Any] | None = None
    ) -> None: ...


class NullSpan:
    def set_attribute(self, name: str, value: Any) -> None: pass
    def record_exception(self, exception: BaseException) -> None: pass

    def add_event(
        self,
        name: str,
        attributes: dict[str, Any] | None = None
    ) -> None: pass


class TracerLike(Protocol):
    def start_as_current_span(self, *args, **kwargs) -> ContextManager[Any]:
        ...


class NullTracer(TracerLike):
    @contextmanager
    def start_as_current_span(self, *args, **kwargs):
        yield NullSpan()


class CounterLike(Protocol):
    def add(
        self,
        amount: int | float,
        attributes: dict[str, Any] | None = None
    ) -> None: ...


class NoopCounter:
    def add(
        self,
        amount: int | float,
        attributes: dict[str, Any] | None = None
    ) -> None: pass


class HistogramLike(Protocol):
    def record(
        self,
        amount: int | float,
        attributes: dict[str, Any] | None = None
    ) -> None: ...


class NoopHistogram:
    def record(
        self,
        amount: int | float,
        attributes: dict[str, Any] | None = None
    ) -> None:
        pass


class MeterLike(Protocol):
    def create_counter(
        self, name, unit: str | None, description: str | None
    ) -> CounterLike: ...

    def create_histogram(
        self,
        name: str,
        unit: str | None = None,
        description: str | None = None,
    ) -> HistogramLike: ...


class NullMeter(MeterLike):
    def create_counter(
        self, name, unit: str | None, description: str | None
    ) -> CounterLike:
        return NoopCounter()

    def create_histogram(
        self,
        name: str,
        unit: str | None = None,
        description: str | None = None,
    ) -> HistogramLike:
        return NoopHistogram()


@dataclass
class Telemetry:
    tracer: TracerLike = field(default_factory=NullTracer)
    meter: MeterLike = field(default_factory=NullMeter)
