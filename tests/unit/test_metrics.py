from typing import Dict, Tuple

import pytest

from aiodynamo.otel.metrics import ClientMetrics
from aiodynamo.otel.types import Telemetry
from tests.fakes import FakeMeter


@pytest.fixture
def telemetry() -> Telemetry:
    return Telemetry(meter=FakeMeter())


@pytest.mark.parametrize(
    "method,kwargs,instrument_name,expected",
    [
        (
            "record_request",
            {"operation": "GetItem", "region": "us-east-1"},
            "aiodynamo.requests",
            (1, {"db.operation": "GetItem", "aws.region": "us-east-1"}),
        ),
        (
            "record_attempt",
            {"operation": "PutItem", "region": "us-west-2"},
            "aiodynamo.attempts",
            (1, {"db.operation": "PutItem", "aws.region": "us-west-2"}),
        ),
        (
            "record_retry",
            {"operation": "Query", "region": "eu-west-1", "reason": "Throttled"},
            "aiodynamo.retries",
            (
                1,
                {
                    "db.operation": "Query",
                    "aws.region": "eu-west-1",
                    "reason": "Throttled",
                },
            ),
        ),
        (
            "record_error",
            {"operation": "Scan", "region": "us-east-1", "error_type": "TimeoutError"},
            "aiodynamo.errors",
            (
                1,
                {
                    "db.operation": "Scan",
                    "aws.region": "us-east-1",
                    "error.type": "TimeoutError",
                },
            ),
        ),
        (
            "record_request_duration",
            {"operation": "DeleteItem", "region": "us-east-1", "duration": 0.25},
            "aiodynamo.request.duration",
            (0.25, {"db.operation": "DeleteItem", "aws.region": "us-east-1"}),
        ),
        (
            "record_batch_items",
            {"operation": "BatchGetItem", "region": "us-east-1", "count": 32},
            "aiodynamo.batch.items_total",
            (32, {"db.operation": "BatchGetItem", "aws.region": "us-east-1"}),
        ),
        (
            "record_batch_items",
            {"operation": "BatchWriteItem", "region": "us-east-1", "count": 1111},
            "aiodynamo.batch.items_total",
            (1111, {"db.operation": "BatchWriteItem", "aws.region": "us-east-1"}),
        ),
        (
            "record_batch_unprocessed_items",
            {"operation": "BatchWriteItem", "region": "us-east-1", "count": 5},
            "aiodynamo.batch.unprocessed_items_total",
            (5, {"db.operation": "BatchWriteItem", "aws.region": "us-east-1"}),
        ),
        (
            "record_batch_duration",
            {"operation": "BatchWriteItem", "region": "us-east-1", "duration": 0.42},
            "aiodynamo.batch.duration",
            (0.42, {"db.operation": "BatchWriteItem", "aws.region": "us-east-1"}),
        ),
    ],
)
def test_metric_helpers_call_underlying_instrument(
    telemetry: Telemetry,
    method: str,
    kwargs: Dict[str, str | int],
    instrument_name: str,
    expected: Tuple[int, Dict[str, str | int]],
) -> None:
    metrics = ClientMetrics.from_telemetry(telemetry)

    getattr(metrics, method)(**kwargs)

    assert telemetry.meter.created[instrument_name].calls == [expected]  # type: ignore[attr-defined]
