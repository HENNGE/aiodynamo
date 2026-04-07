from dataclasses import dataclass
from typing import Any


@dataclass
class FakeCounter:
    calls: list[tuple[int | float, dict | None]]

    def add(self, amount, attributes=None):
        self.calls.append((amount, attributes))


@dataclass
class FakeHistogram:
    calls: list[tuple[int | float, dict | None]]

    def record(self, amount, attributes=None):
        self.calls.append((amount, attributes))


class FakeMeter:
    def __init__(self):
        self.created = {}

    def create_counter(self, name, unit=None, description=None):
        instrument = FakeCounter([])
        self.created[name] = instrument
        return instrument

    def create_histogram(self, name, unit=None, description=None):
        instrument = FakeHistogram([])
        self.created[name] = instrument
        return instrument

@dataclass
class FakeSpan:
    name: str
    attributes: dict[str, Any]
    events: list[str]
    exceptions: list[str]

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def add_event(self, name):
        self.events.append(name)

    def record_exception(self, exc):
        self.exceptions.append(type(exc).__name__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeTracer:
    def __init__(self):
        self.spans = []

    def start_as_current_span(self, name):
        span = FakeSpan(name=name, attributes={}, events=[], exceptions=[])
        self.spans.append(span)
        return span
