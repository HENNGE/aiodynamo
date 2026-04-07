import pytest

from aiodynamo.otel.types import (
    NullMeter,
    NullSpan,
    NoopCounter,
    NoopHistogram,
    Telemetry,
)


@pytest.mark.parametrize(
    "method_name,args,kwargs",
    [
        ("set_attribute", ("k", "v"), {}),
        ("record_exception", (ValueError("boom"),), {}),
        ("add_event", ("event",), {}),
        ("add_event", ("event", {"a": 1}), {}),
    ],
)
def test_null_span_methods_do_not_crash(method_name, args, kwargs):
    span = NullSpan()
    getattr(span, method_name)(*args, **kwargs)


@pytest.mark.parametrize(
    "method_name,args,kwargs",
    [
        ("add", (1,), {}),
        ("add", (1, {"k": "v"}), {}),
        ("add", (1.5,), {}),
        ("add", (1.5, {"k": "v"}), {}),
    ],
)
def test_noop_counter_methods_do_not_crash(method_name, args, kwargs):
    counter = NoopCounter()
    getattr(counter, method_name)(*args, **kwargs)


@pytest.mark.parametrize(
    "method_name,args,kwargs",
    [
        ("record", (1,), {}),
        ("record", (1, {"k": "v"}), {}),
        ("record", (1.5,), {}),
        ("record", (1.5, {"k": "v"}), {}),
    ],
)
def test_noop_histogram_methods_do_not_crash(method_name, args, kwargs):
    histogram = NoopHistogram()
    getattr(histogram, method_name)(*args, **kwargs)


def test_null_meter_returns_noop_instruments():
    meter = NullMeter()

    counter = meter.create_counter("x", unit="1", description="desc")
    hist = meter.create_histogram("y", unit="s", description="desc")

    assert isinstance(counter, NoopCounter)
    assert isinstance(hist, NoopHistogram)


def test_telemetry_defaults_to_null_implementations():
    telemetry = Telemetry()

    assert isinstance(telemetry.meter, NullMeter)
