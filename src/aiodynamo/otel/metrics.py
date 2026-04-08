from dataclasses import dataclass
from typing import Any

from aiodynamo.otel.types import CounterLike, HistogramLike, Telemetry

COUNTER_UNIT = "1"
HISTOGRAM_UNIT = "s"


def _attributes(operation: str, region: str, **extra: Any) -> dict[str, Any]:
    return {
        "db.operation": operation,
        "aws.region": region,
        **extra,
    }


@dataclass(frozen=True)
class ClientMetrics:
    requests: CounterLike
    attempts: CounterLike
    retries: CounterLike
    errors: CounterLike
    request_duration: HistogramLike

    batch_items_total: CounterLike
    batch_unprocessed_items_total: CounterLike
    batch_duration: HistogramLike

    @classmethod
    def from_telemetry(cls, telemetry: Telemetry) -> "ClientMetrics":
        return cls(
            requests=telemetry.meter.create_counter(
                "aiodynamo.requests",
                unit=COUNTER_UNIT,
                description="Logical DynamoDB requests",
            ),
            attempts=telemetry.meter.create_counter(
                "aiodynamo.attempts",
                unit=COUNTER_UNIT,
                description="HTTP attempts made for DynamoDB requests",
            ),
            retries=telemetry.meter.create_counter(
                "aiodynamo.retries",
                unit=COUNTER_UNIT,
                description="Retry attempts after failures",
            ),
            errors=telemetry.meter.create_counter(
                "aiodynamo.errors",
                unit=COUNTER_UNIT,
                description="Final failed DynamoDB requests",
            ),
            request_duration=telemetry.meter.create_histogram(
                "aiodynamo.request.duration",
                unit=HISTOGRAM_UNIT,
                description="DynamoDB request duration in seconds",
            ),
            batch_items_total=telemetry.meter.create_counter(
                "aiodynamo.batch.items_total",
                unit=COUNTER_UNIT,
                description="DynamoDB batch items total",
            ),
            batch_unprocessed_items_total=telemetry.meter.create_counter(
                "aiodynamo.batch.unprocessed_items_total",
                unit=COUNTER_UNIT,
                description="DynamoDB batch unprocessed items total",
            ),
            batch_duration=telemetry.meter.create_histogram(
                "aiodynamo.batch.duration",
                unit=HISTOGRAM_UNIT,
                description="Batch request duration in seconds",
            ),
        )

    def record_request(self, *, operation: str, region: str) -> None:
        self.requests.add(1, _attributes(operation, region))

    def record_attempt(self, *, operation: str, region: str) -> None:
        self.attempts.add(1, _attributes(operation, region))

    def record_retry(self, *, operation: str, region: str, reason: str) -> None:
        self.retries.add(1, _attributes(operation, region, reason=reason))

    def record_error(self, *, operation: str, region: str, error_type: str) -> None:
        self.errors.add(1, _attributes(operation, region, **{"error.type": error_type}))

    def record_request_duration(
        self, *, operation: str, region: str, duration: float
    ) -> None:
        self.request_duration.record(duration, _attributes(operation, region))

    def record_batch_items(self, *, operation: str, region: str, count: int) -> None:
        self.batch_items_total.add(count, _attributes(operation, region))

    def record_batch_unprocessed_items(
        self, *, operation: str, region: str, count: int
    ) -> None:
        self.batch_unprocessed_items_total.add(count, _attributes(operation, region))

    def record_batch_duration(
        self, *, operation: str, region: str, duration: float
    ) -> None:
        self.batch_duration.record(duration, _attributes(operation, region))
