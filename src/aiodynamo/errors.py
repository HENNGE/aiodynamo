import json
from typing import Any, Dict


class AIODynamoError(Exception):
    pass


class NoCredentialsFound(AIODynamoError):
    pass


class EmptyItem(AIODynamoError):
    pass


class ItemNotFound(AIODynamoError):
    pass


class CannotAddToNestedField(AIODynamoError):
    pass


class UnknownError(AIODynamoError):
    def __init__(self, status: int, body: bytes):
        self.status = status
        self.body = body
        super().__init__(body)


class UnknownOperation(AIODynamoError):
    pass


class TableNotFound(AIODynamoError):
    pass


class ProvisionedThroughputExceeded(AIODynamoError):
    pass


class RequestLimitExceeded(AIODynamoError):
    pass


class InternalDynamoError(AIODynamoError):
    pass


class ItemCollectionSizeLimitExceeded(AIODynamoError):
    pass


class BackupInUse(AIODynamoError):
    pass


class ContinuousBackupsUnavailable(AIODynamoError):
    pass


class TableInUse(AIODynamoError):
    pass


class ResourceInUse(AIODynamoError):
    pass


class GlobalTableAlreadyExists(AIODynamoError):
    pass


class SourceTableDoesNotExist(AIODynamoError):
    pass


class LimitExceeded(AIODynamoError):
    pass


class BackupNotFound(AIODynamoError):
    pass


class ConditionalCheckFailed(AIODynamoError):
    pass


class TransactionConflict(AIODynamoError):
    pass


class GlobalTableNotFound(AIODynamoError):
    pass


class TableAlreadyExists(AIODynamoError):
    pass


class InvalidRestoreTime(AIODynamoError):
    pass


class PointInTimeRecoveryUnavailable(AIODynamoError):
    pass


class TransactionCanceled(AIODynamoError):
    pass


class TransactionEmpty(AIODynamoError):
    pass


class TooManyTransactions(AIODynamoError):
    pass


class ReplicaAlreadyExists(AIODynamoError):
    pass


class ReplicaNotFound(AIODynamoError):
    pass


class TableDidNotBecomeActive(AIODynamoError):
    pass


class TableDidNotBecomeDisabled(AIODynamoError):
    pass


class Throttled(AIODynamoError):
    pass


class BrokenThrottleConfig(Throttled):
    pass


class ValidationException(AIODynamoError):
    pass


class TimeToLiveStatusNotChanged(AIODynamoError):
    pass


class ExpiredToken(AIODynamoError):
    pass


class ServiceUnavailable(AIODynamoError):
    pass


class IdempotentParameterMismatch(AIODynamoError):
    pass


class TransactionInProgress(AIODynamoError):
    pass


ERRORS = {
    "ResourceNotFoundException": TableNotFound,
    "UnknownOperationException": UnknownOperation,
    "ProvisionedThroughputExceededException": ProvisionedThroughputExceeded,
    "RequestLimitExceeded": RequestLimitExceeded,
    "ItemCollectionSizeLimitExceededException": ItemCollectionSizeLimitExceeded,
    "BackupInUseException": BackupInUse,
    "ContinuousBackupsUnavailableException": ContinuousBackupsUnavailable,
    "TableInUseException": TableInUse,
    "GlobalTableAlreadyExistsException": GlobalTableAlreadyExists,
    "TableNotFoundException": SourceTableDoesNotExist,
    "LimitExceededException": LimitExceeded,
    "BackupNotFoundException": BackupNotFound,
    "ConditionalCheckFailedException": ConditionalCheckFailed,
    "TransactionConflictException": TransactionConflict,
    "GlobalTableNotFoundException": GlobalTableNotFound,
    "TableAlreadyExistsException": TableAlreadyExists,
    "InvalidRestoreTimeException": InvalidRestoreTime,
    "PointInTimeRecoveryUnavailableException": PointInTimeRecoveryUnavailable,
    "TransactionCanceledException": TransactionCanceled,
    "ReplicaAlreadyExistsException": ReplicaAlreadyExists,
    "ReplicaNotFoundException": ReplicaNotFound,
    "ThrottlingException": Throttled,
    "ValidationException": ValidationException,
    "ExpiredTokenException": ExpiredToken,
    "ResourceInUseException": ResourceInUse,
    "IdempotentParameterMismatchException": IdempotentParameterMismatch,
    "TransactionInProgressException": TransactionInProgress,
}


def exception_from_response(status: int, body: bytes) -> Exception:
    if status == 500:
        return InternalDynamoError()
    elif status == 503:
        return ServiceUnavailable()
    try:
        data = json.loads(body)
        error = ERRORS[data["__type"].split("#", 1)[-1]](data)
        if isinstance(error, TransactionCanceled):
            error = extract_error_from_transaction_canceled(data)
        return error
    except Exception:
        return UnknownError(status, body)


def extract_error_from_transaction_canceled(data: Dict[str, Any]) -> AIODynamoError:
    try:
        error = data["CancellationReasons"][0]
        return ERRORS[f"{error['Code']}Exception"](error["Message"])
    except Exception:
        return ERRORS[data["__type"].split("#", 1)[-1]](data)
