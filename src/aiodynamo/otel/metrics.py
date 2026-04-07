from dataclasses import dataclass

from aiodynamo.otel.types import CounterLike, Telemetry, HistogramLike


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
                unit="1",
                description="Logical DynamoDB requests",
            ),
            attempts=telemetry.meter.create_counter(
                "aiodynamo.attempts",
                unit="1",
                description="HTTP attempts made for DynamoDB requests",
            ),
            retries=telemetry.meter.create_counter(
                "aiodynamo.retries",
                unit="1",
                description="Retry attempts after failures",
            ),
            errors=telemetry.meter.create_counter(
                "aiodynamo.errors",
                unit="1",
                description="Final failed DynamoDB requests",
            ),
            request_duration=telemetry.meter.create_histogram(
                "aiodynamo.request.duration",
                unit="s",
                description="DynamoDB request duration in seconds",
            ),
            batch_items_total=telemetry.meter.create_counter(
                "aiodynamo.batch.items_total",
                unit="1",
                description="DynamoDB batch items total",
            ),
            batch_unprocessed_items_total=telemetry.meter.create_counter(
                "aiodynamo.batch.unprocessed_items_total",
                unit="1",
                description="DynamoDB batch unprocessed items total",
            ),
            batch_duration=telemetry.meter.create_histogram(
                "aiodynamo.batch.duration",
                unit="s",
                description="Batch request duration in seconds",
            ),
        )

    def record_request(self, *, operation: str, region: str) -> None:
        self.requests.add(
            1,
            attributes={
                "db.operation": operation,
                "aws.region": region,
            },
        )

    def record_attempt(self, *, operation: str, region: str) -> None:
        self.attempts.add(
            1,
            attributes={
                "db.operation": operation,
                "aws.region": region,
            },
        )

    def record_retry(self, *, operation: str, region: str, reason: str) -> None:
        self.retries.add(
            1,
            attributes={
                "db.operation": operation,
                "aws.region": region,
                "reason": reason,
            },
        )

    def record_error(self, *, operation: str, region: str, error_type: str) -> None:
        self.errors.add(
            1,
            attributes={
                "db.operation": operation,
                "aws.region": region,
                "error.type": error_type,
            },
        )

    def record_request_duration(self, *, operation: str, region: str, duration: float) -> None:
        self.request_duration.record(
            duration,
            attributes={
                "db.operation": operation,
                "aws.region": region,
            },
        )

    def record_batch_items(self, *, operation: str, region: str, count: int) -> None:
        self.batch_items_total.add(
            count,
            attributes={
                "db.operation": operation,
                "aws.region": region,
            },
        )

    def record_batch_unprocessed_items(self, *, operation: str, region: str, count: int) -> None:
        self.batch_unprocessed_items_total.add(
            count,
            attributes={
                "db.operation": operation,
                "aws.region": region,
            },
        )

    def record_batch_duration(self, *, operation: str, region: str, duration: float) -> None:
        self.batch_duration.record(
            duration,
            attributes={
                "db.operation": operation,
                "aws.region": region,
            },
        )
