from typing import Dict, Tuple

import pytest

from aiodynamo.otel.types import (
    NoopCounter,
    NoopHistogram,
    NullMeter,
    NullSpan,
    Telemetry,
)


@pytest.mark.parametrize(
    "method_name,args",
    [
        ("set_attribute", ("keyname", "keyvalue")),
        ("record_exception", (ValueError("boom"),)),
        ("add_event", ("some_event",)),
        ("add_event", ("some_event_with_attr", {"my_attr": 1})),
    ],
)
def test_null_span_methods_do_not_crash(
    method_name: str,
    args: Tuple[int, Dict[str, int]],
) -> None:
    span = NullSpan()
    getattr(span, method_name)(*args)


@pytest.mark.parametrize(
    "method_name,args",
    [
        ("add", (1,)),
        ("add", (1, {"k": "v"})),
        ("add", (1.5,)),
        ("add", (1.5, {"k": "v"})),
    ],
)
def test_noop_counter_methods_do_not_crash(
    method_name: str,
    args: Tuple[int, Dict[str, str]],
) -> None:
    counter = NoopCounter()
    getattr(counter, method_name)(*args)


@pytest.mark.parametrize(
    "method_name,args",
    [
        ("record", (1,)),
        ("record", (1, {"k": "v"})),
        ("record", (1.5,)),
        ("record", (1.5, {"k": "v"})),
    ],
)
def test_noop_histogram_methods_do_not_crash(
    method_name: str,
    args: Tuple[int, Dict[str, str]],
) -> None:
    histogram = NoopHistogram()
    getattr(histogram, method_name)(*args)


def test_null_meter_returns_noop_instruments() -> None:
    meter = NullMeter()

    counter = meter.create_counter("x", unit="1", description="desc")
    hist = meter.create_histogram("y", unit="s", description="desc")

    assert isinstance(counter, NoopCounter)
    assert isinstance(hist, NoopHistogram)


def test_telemetry_defaults_to_null_implementations() -> None:
    telemetry = Telemetry()

    assert isinstance(telemetry.meter, NullMeter)
