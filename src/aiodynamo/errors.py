import json


class AIODynamoError(Exception):
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


class ValidationException(AIODynamoError):
    pass


class TimeToLiveStatusNotChanged(AIODynamoError):
    pass


class ExpiredToken(AIODynamoError):
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
}


def exception_from_response(status: int, body: bytes) -> Exception:
    if status == 500:
        return InternalDynamoError()
    try:
        data = json.loads(body)
        return ERRORS[data["__type"].split("#", 1)[1]](data)
    except Exception:
        return UnknownError(status, body)
